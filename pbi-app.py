import streamlit as st
import json
import pandas as pd
import sqlparse
import google.generativeai as genai
import os
import re
from pathlib import Path

# Import the analyzer from main.py
from main import SQLLineageAnalyzer

# Set up Gemini API with internal API key
API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyAslNL0AT5-dnxtUxUNh9scPP5jnbkfuwE")
genai.configure(api_key=API_KEY)

# Fixed mapping file path
MAPPING_FILE_PATH = "column_mappings.json"

# Function to normalize column identifiers for comparison (this should be the existing one)
def normalize_column_identifier(column_id):
    """Normalize column identifiers by removing quotes and extracting the important parts."""
    if not column_id: # Handles None or empty string
        return ""
        
    # Remove all double quotes and trim whitespace
    normalized = column_id.replace('"', '').strip()
    
    # Convert to lowercase for case-insensitive matching
    normalized = normalized.upper() # Changed to lower as per previous discussions for consistency
    
    # Split by dots to get components
    parts = normalized.split('.')
    
    # Return the last 3 components at most (schema.table.column) or fewer if not available
    return '.'.join(parts[-3:]) if len(parts) >= 3 else normalized

# Function to load column mappings (ensure this is correctly loading your JSON)
def load_column_mappings(file_path=MAPPING_FILE_PATH):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)  # Load the entire JSON object
            # Extract the actual mappings dictionary which is nested under the "mappings" key
            mappings_dict = data.get("mappings")
            if mappings_dict and isinstance(mappings_dict, dict):
                # Successfully extracted the nested "mappings" object
                return mappings_dict
            else:
                # "mappings" key is missing or not a dictionary
                print(f"ERROR: 'mappings' key not found or is not a dictionary in {file_path}. File content might be malformed.")
                # Return a default structure to prevent errors downstream, but it will show 0 columns
                return {"db_to_powerbi": {}, "powerbi_to_db": {}}
    except FileNotFoundError:
        print(f"ERROR: Mapping file not found: {file_path}")
        return {"db_to_powerbi": {}, "powerbi_to_db": {}} # Return default empty structure
    except json.JSONDecodeError as e:
        print(f"ERROR: Error decoding JSON from mapping file: {file_path} - {str(e)}")
        return {"db_to_powerbi": {}, "powerbi_to_db": {}} # Return default empty structure
    except Exception as e:
        print(f"ERROR: An unexpected error occurred while loading mapping file: {file_path} - {str(e)}")
        return {"db_to_powerbi": {}, "powerbi_to_db": {}} # Return default empty structure

# Refined function to find matching PowerBI columns
def find_matching_powerbi_columns(db_column_from_sql, mappings_dict):
    """Find matching PowerBI columns in the mappings dictionary."""
    if not db_column_from_sql or not mappings_dict or "db_to_powerbi" not in mappings_dict:
        return []
        
    norm_sql_col = normalize_column_identifier(db_column_from_sql)
    
    if not norm_sql_col:
        return []

    found_matches = []
    
    for db_col_from_mapping, pbi_column_infos in mappings_dict["db_to_powerbi"].items():
        norm_mapping_col = normalize_column_identifier(db_col_from_mapping)

        if not norm_mapping_col: 
            continue
            
        if norm_sql_col == norm_mapping_col:
            for pbi_info in pbi_column_infos:
                found_matches.append({
                    "db_column": db_col_from_mapping,       
                    "matched_input": db_column_from_sql,    
                    "powerbi_column": pbi_info.get("powerbi_column"),
                    "table": pbi_info.get("table"),
                    "column": pbi_info.get("column")
                })
            continue 

        sql_parts = norm_sql_col.split('.')
        map_parts = norm_mapping_col.split('.')

        if sql_parts[-1] == map_parts[-1]:
            if norm_sql_col.endswith(norm_mapping_col) or norm_mapping_col.endswith(norm_sql_col):
                already_added = any(
                    match["db_column"] == db_col_from_mapping and match["matched_input"] == db_column_from_sql
                    for match in found_matches
                )
                if not already_added:
                    for pbi_info in pbi_column_infos:
                        found_matches.append({
                            "db_column": db_col_from_mapping,
                            "matched_input": db_column_from_sql,
                            "powerbi_column": pbi_info.get("powerbi_column"),
                            "table": pbi_info.get("table"),
                            "column": pbi_info.get("column")
                        })
                continue 
    return found_matches

def generate_dax_from_sql(sql_expression):
    try:
        model = genai.GenerativeModel('gemini-2.5-flash-preview-05-20')
        prompt = f"""
        Analyze the following SQL expression and provide:
        1. An equivalent PowerBI DAX expression for a MEASURE (properly formatted with line breaks and indentation for readability)
        2. An equivalent PowerBI DAX expression for a CALCULATED COLUMN (properly formatted with line breaks and indentation for readability)
        3. A recommendation on whether this should be implemented as a measure or calculated column in PowerBI based on its characteristics

        SQL Expression:
        ```sql
        {sql_expression}
        ```
        
        Format your response exactly like this example with no additional text:
        MEASURE: 
        CALCULATE(
            SUM(Sales[Revenue]),
            Sales[Year] = 2023
        )
        CALCULATED_COLUMN: 
        IF(
            [Price] * [Quantity] > 1000,
            "High Value",
            "Standard"
        )
        RECOMMENDATION: measure
        """
        
        response = model.generate_content(prompt)
        
        # Clean up response to remove markdown formatting
        dax_response = response.text.strip()
        
        # Extract the different sections using more sophisticated parsing
        sections = {'measure': '', 'calculated_column': '', 'recommendation': ''}
        
        # Split by sections markers
        parts = dax_response.split('MEASURE:')
        if len(parts) > 1:
            rest = parts[1]
            
            # Get CALCULATED_COLUMN section
            calc_parts = rest.split('CALCULATED_COLUMN:')
            if len(calc_parts) > 1:
                sections['measure'] = calc_parts[0].strip()
                rest = calc_parts[1]
                
                # Get RECOMMENDATION section
                rec_parts = rest.split('RECOMMENDATION:')
                if len(rec_parts) > 1:
                    sections['calculated_column'] = rec_parts[0].strip()
                    sections['recommendation'] = rec_parts[1].strip()
                else:
                    sections['calculated_column'] = rest.strip()

        # Clean up any markdown formatting in the sections
        for key in ['measure', 'calculated_column']:
            # Remove code block markers
            sections[key] = sections[key].replace('```dax', '').replace('```', '')
            
            # Remove language identifier if it appears at the beginning
            if sections[key].lstrip().startswith('dax'):
                sections[key] = sections[key].lstrip()[3:].lstrip()

            if sections[key].lstrip().startswith('DAX'):
                sections[key] = sections[key].lstrip()[3:].lstrip()
                
            # Remove any trailing backticks
            sections[key] = sections[key].rstrip('`').strip()
        
        return {
            "measure": sections['measure'],
            "calculated_column": sections['calculated_column'],
            "recommendation": sections['recommendation']
        }
    except Exception as e:
        return {
            "measure": f"Error: {str(e)}",
            "calculated_column": f"Error: {str(e)}",
            "recommendation": "error"
        }

def main():
    st.set_page_config(
        page_title="SQL to Power BI Mapper",
        page_icon="📊",
        layout="wide"
    )
    
    # Initialize session state variables
    if 'sql_query' not in st.session_state:
        st.session_state['sql_query'] = ""
    if 'lineage_data' not in st.session_state:
        st.session_state['lineage_data'] = None
    if 'all_types' not in st.session_state:
        st.session_state['all_types'] = []
    if 'dax_expressions' not in st.session_state:
        st.session_state['dax_expressions'] = {}
    
    # Load column mappings into session state if not already loaded
    # This ensures it's loaded once per session or when the app starts
    if 'column_mappings' not in st.session_state:
        st.session_state['column_mappings'] = load_column_mappings()
    
    st.title("SQL to Power BI Column Mapper")
    st.markdown("""
    This tool analyzes SQL queries to understand column lineage and maps them to PowerBI columns using the column mapping database.
    """)
    
    with st.sidebar:
        st.header("Settings")
        # Use the session state key 'column_mappings'
        current_mappings_in_session = st.session_state.get('column_mappings')
        
        # Check if current_mappings_in_session is a dictionary and has the 'db_to_powerbi' key
        if isinstance(current_mappings_in_session, dict) and "db_to_powerbi" in current_mappings_in_session:
            st.info(f"✅ Using mapping file: {MAPPING_FILE_PATH}")
            # This line should now report the correct number
            st.info(f"Contains {len(current_mappings_in_session.get('db_to_powerbi', {}))} database columns")
        else:
            st.warning(f"⚠️ Could not load or parse mapping file correctly from {MAPPING_FILE_PATH}. Check console for errors.")
            if st.button("Retry Loading Mappings"):
                st.session_state['column_mappings'] = load_column_mappings()
                st.rerun()
    
    col1, col2 = st.columns([4, 1])
    
    with col1:
        sql_query = st.text_area("Enter your SQL query:", 
                                value=st.session_state.get('sql_query', ""),
                                height=300)
        st.session_state['sql_query'] = sql_query
    
    with col2:
        st.write("### Actions")
        analyze_button = st.button("Analyze Query", use_container_width=True)
        clear_button = st.button("Clear Query", use_container_width=True)
        
        if clear_button:
            st.session_state['sql_query'] = ""
            st.session_state['lineage_data'] = None
            st.session_state['all_types'] = []
            st.session_state['dax_expressions'] = {}
            st.rerun()
    
    if analyze_button and sql_query.strip():
        try:
            with st.spinner("Analyzing query..."):
                analyzer = SQLLineageAnalyzer(sql_query, dialect="snowflake")
                st.session_state['lineage_data'] = analyzer.analyze()
                
                if st.session_state['lineage_data']:
                    df = pd.DataFrame(st.session_state['lineage_data'])
                    st.session_state['all_types'] = sorted(df['type'].unique().tolist())
        except Exception as e:
            st.error(f"Error analyzing query: {str(e)}")
            st.exception(e)
    
    if st.session_state['lineage_data']:
        st.subheader("Analysis Results")
        
        df = pd.DataFrame(st.session_state['lineage_data'])
        
        # Adjusted to 3 tabs
        tab1, tab2, tab3 = st.tabs(["Table View", "Detail View", "Raw JSON"])
        
        with tab1:
            selected_types_tab1 = st.multiselect(
                "Filter by type:",
                options=st.session_state['all_types'],
                default=st.session_state['all_types'],
                key="filter_types_tab1"
            )
            
            filtered_df = df[df['type'].isin(selected_types_tab1)] if selected_types_tab1 else df
            st.dataframe(filtered_df, use_container_width=True)
            
            csv = filtered_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name="lineage_analysis.csv",
                mime="text/csv"
            )
        
        with tab2: # Detail View
            selected_types_tab2 = st.multiselect(
                "Filter by type:",
                options=st.session_state['all_types'],
                default=st.session_state['all_types'],
                key="filter_types_tab2"
            )
            
            filtered_items = [item for item in st.session_state['lineage_data'] if item['type'] in selected_types_tab2] if selected_types_tab2 else st.session_state['lineage_data']
            
            for i, item in enumerate(filtered_items):
                with st.expander(f"Column: {item['column']} ({item['type']})"):
                    st.write("**Type:** ", item['type'])
                    
                    if item['type'] == 'expression' and item['final_expression']:
                        formatted_expr = sqlparse.format(
                            item['final_expression'],
                            reindent=True,
                            keyword_case='upper',
                            indent_width=2
                        )
                        st.write("**SQL Expression:**")
                        st.code(formatted_expr, language="sql")
                                                
                        item_id = f"{item['column']}_{i}"
                        if st.button(f"Generate DAX", key=f"dax_btn_{item_id}"):
                            with st.spinner("Generating DAX..."):
                                dax_results = generate_dax_from_sql(item['final_expression'])
                                st.session_state['dax_expressions'][item_id] = dax_results

                        if item_id in st.session_state['dax_expressions']:
                            dax_results = st.session_state['dax_expressions'][item_id]
                            recommendation = dax_results.get("recommendation", "").lower()
                            if recommendation == "measure":
                                st.info("💡 **Recommendation:** **MEASURE**")
                            elif recommendation == "calculated_column" or recommendation == "calculated column":
                                st.info("💡 **Recommendation:** **CALCULATED COLUMN**")
                            st.write("**DAX Measure:**")
                            st.code(dax_results.get("measure", ""), language="")
                            st.write("**DAX Calculated Column:**")
                            st.code(dax_results.get("calculated_column", ""), language="")
                    
                    elif item['type'] == 'expression':
                        st.code("No expression available", language="sql")
                        
                    st.write("**Base columns:**")
                    if item['base_columns']:
                        for col in item['base_columns']:
                            st.write(f"- `{col}`")
                    else:
                        st.write("N/A (Direct column or no base columns identified)")

                    # --- New PBI Mapping Section ---
                    st.markdown("---") # Visual separator
                    st.write("**PBI Mapping for Base Columns:**")
                    if not item['base_columns']:
                        st.caption("No base columns to map for PBI.")
                    elif not st.session_state.get('column_mappings'):
                        st.warning("Mapping file not loaded. PBI mappings cannot be displayed.")
                    else:
                        for base_col_idx, base_col in enumerate(item['base_columns']):
                            norm_base_col = normalize_column_identifier(base_col)
                            st.markdown(f"  - **Base Column {base_col_idx+1}:** `{base_col}` <br>&nbsp;&nbsp;&nbsp;&nbsp;Normalized: `{norm_base_col}`", unsafe_allow_html=True)
                            
                            pbi_matches_for_base_col = find_matching_powerbi_columns(base_col, st.session_state['column_mappings'])
                            
                            if pbi_matches_for_base_col:
                                for match_info in pbi_matches_for_base_col:
                                    st.markdown(f"""
                                        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Mapped to PBI: `{match_info['powerbi_column']}`
                                        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Table: `{match_info['table']}`
                                        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Column: `{match_info['column']}`
                                        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- (Source DB in Mapping: `{match_info['db_column']}`)
                                    """, unsafe_allow_html=True)
                            else:
                                st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- *No PowerBI mapping found for this base column.*", unsafe_allow_html=True)
                            st.markdown("<br>", unsafe_allow_html=True) # Add a little space between base columns
        
        with tab3: # Raw JSON view
            st.json(st.session_state['lineage_data'])
    
    elif analyze_button and not sql_query.strip():
        st.warning("Please enter a SQL query")

if __name__ == "__main__":
    main()