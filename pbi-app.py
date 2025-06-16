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
    
    if 'column_mappings' not in st.session_state:
        st.session_state['column_mappings'] = load_column_mappings()
    
    # Re-introduce mapping_results for the dedicated PBI Mapping tab
    if 'mapping_results' not in st.session_state:
        st.session_state['mapping_results'] = None # Initialize as None or {}
    
    st.title("SQL to Power BI Column Mapper")
    st.markdown("""
    This tool analyzes SQL queries to understand column lineage and maps them to PowerBI columns using the column mapping database.
    """)
    
    with st.sidebar:
        st.header("Settings")
        current_mappings_in_session = st.session_state.get('column_mappings')
        
        if isinstance(current_mappings_in_session, dict) and "db_to_powerbi" in current_mappings_in_session:
            st.info(f"✅ Using mapping file: {MAPPING_FILE_PATH}")
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
            st.session_state['mapping_results'] = None # Clear mapping results too
            st.rerun()
    
    if analyze_button and sql_query.strip():
        try:
            with st.spinner("Analyzing query..."):
                analyzer = SQLLineageAnalyzer(sql_query, dialect="snowflake")
                st.session_state['lineage_data'] = analyzer.analyze()
                
                if st.session_state['lineage_data']:
                    df = pd.DataFrame(st.session_state['lineage_data'])
                    st.session_state['all_types'] = sorted(df['type'].unique().tolist())

                    # Prepare data for the PBI Mapping tab
                    if st.session_state.get('column_mappings'):
                        mapping_results_for_tab = {}
                        for item in st.session_state['lineage_data']:
                            sql_output_column_name = item['column']
                            base_columns_for_item = item.get('base_columns', [])
                            
                            current_sql_col_mappings = []
                            has_any_pbi_mapping_for_this_sql_col = False

                            for base_col in base_columns_for_item:
                                pbi_matches = find_matching_powerbi_columns(base_col, st.session_state['column_mappings'])
                                if pbi_matches:
                                    has_any_pbi_mapping_for_this_sql_col = True
                                current_sql_col_mappings.append({
                                    "original_base_col": base_col,
                                    "normalized_base_col": normalize_column_identifier(base_col),
                                    "pbi_matches": pbi_matches
                                })
                            
                            mapping_results_for_tab[sql_output_column_name] = {
                                "type": item['type'],
                                "base_column_mappings": current_sql_col_mappings,
                                "is_mapped_overall": has_any_pbi_mapping_for_this_sql_col
                            }
                        st.session_state['mapping_results'] = mapping_results_for_tab
                    else:
                        st.session_state['mapping_results'] = None # No mappings if mapping file not loaded

        except Exception as e:
            st.error(f"Error analyzing query: {str(e)}")
            st.exception(e)
    
    if st.session_state['lineage_data']:
        st.subheader("Analysis Results")
        
        df = pd.DataFrame(st.session_state['lineage_data'])
        
        # Adjusted to 4 tabs
        tab1, tab2, tab3, tab4 = st.tabs(["Table View", "Detail View", "PBI Mapping", "Raw JSON"])
        
        with tab1: # Table View
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

                    st.markdown("---") 
                    st.write("**PBI Mapping for Base Columns (Detail):**")
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
                            st.markdown("<br>", unsafe_allow_html=True)
        
        with tab3: # PBI Mapping Tab
            st.header("Consolidated Power BI Column Mappings")
            if not st.session_state.get('column_mappings'):
                st.warning("Mapping file not loaded. Please check sidebar and console for errors.")
            elif not st.session_state.get('mapping_results'):
                st.info("No SQL query analyzed yet, or the query resulted in no columns to map.")
            else:
                mapping_filter = st.radio(
                    "Show SQL Columns:",
                    ["All", "Mapped Only", "Unmapped Only"],
                    horizontal=True,
                    key="pbi_mapping_tab_filter"
                )

                mapping_data_for_tab = st.session_state['mapping_results']
                
                total_sql_cols = len(mapping_data_for_tab)
                mapped_sql_cols_count = sum(1 for data in mapping_data_for_tab.values() if data.get("is_mapped_overall"))
                unmapped_sql_cols_count = total_sql_cols - mapped_sql_cols_count

                m_col1, m_col2, m_col3 = st.columns(3)
                m_col1.metric("Total SQL Columns", total_sql_cols)
                m_col2.metric("Mapped SQL Columns", mapped_sql_cols_count)
                m_col3.metric("Unmapped SQL Columns", unmapped_sql_cols_count)

                export_rows = []

                for sql_col_name, data in mapping_data_for_tab.items():
                    is_overall_mapped = data.get("is_mapped_overall", False)
                    
                    display_this_sql_col = False
                    if mapping_filter == "All":
                        display_this_sql_col = True
                    elif mapping_filter == "Mapped Only" and is_overall_mapped:
                        display_this_sql_col = True
                    elif mapping_filter == "Unmapped Only" and not is_overall_mapped:
                        display_this_sql_col = True
                    
                    if display_this_sql_col:
                        expander_title = f"SQL Column: {sql_col_name} ({data['type']})"
                        expander_title += " ✅ (Mapped)" if is_overall_mapped else " ❌ (Unmapped)"
                        
                        with st.expander(expander_title):
                            if not data["base_column_mappings"]:
                                st.caption("This SQL column has no identified base columns.")
                            
                            has_at_least_one_pbi_mapping_shown = False
                            for base_map_info in data["base_column_mappings"]:
                                st.markdown(f"  - **Base:** `{base_map_info['original_base_col']}` (Normalized: `{base_map_info['normalized_base_col']}`)")
                                if base_map_info["pbi_matches"]:
                                    has_at_least_one_pbi_mapping_shown = True
                                    for pbi_match in base_map_info["pbi_matches"]:
                                        st.markdown(f"    - PBI: `{pbi_match['powerbi_column']}` (Table: `{pbi_match['table']}`, Column: `{pbi_match['column']}`)")
                                        st.markdown(f"      (Source DB in Mapping: `{pbi_match['db_column']}`)")
                                        export_rows.append({
                                            "SQL Output Column": sql_col_name,
                                            "SQL Column Type": data['type'],
                                            "SQL Base Column": base_map_info['original_base_col'],
                                            "Normalized SQL Base Column": base_map_info['normalized_base_col'],
                                            "Mapped PBI Column": pbi_match['powerbi_column'],
                                            "PBI Table": pbi_match['table'],
                                            "PBI Column Name": pbi_match['column'],
                                            "Source DB in Mapping File": pbi_match['db_column']
                                        })
                                else:
                                    st.markdown("    - *No PowerBI mapping found for this base column.*")
                                    export_rows.append({ # Also include unmapped base columns in export
                                        "SQL Output Column": sql_col_name,
                                        "SQL Column Type": data['type'],
                                        "SQL Base Column": base_map_info['original_base_col'],
                                        "Normalized SQL Base Column": base_map_info['normalized_base_col'],
                                        "Mapped PBI Column": "N/A",
                                        "PBI Table": "N/A",
                                        "PBI Column Name": "N/A",
                                        "Source DB in Mapping File": "N/A"
                                    })
                            if not data["base_column_mappings"]: # If SQL col has no base columns
                                 export_rows.append({
                                    "SQL Output Column": sql_col_name,
                                    "SQL Column Type": data['type'],
                                    "SQL Base Column": "N/A",
                                    "Normalized SQL Base Column": "N/A",
                                    "Mapped PBI Column": "N/A",
                                    "PBI Table": "N/A",
                                    "PBI Column Name": "N/A",
                                    "Source DB in Mapping File": "N/A"
                                })


                            if not has_at_least_one_pbi_mapping_shown and data["base_column_mappings"]:
                                st.info("Although this SQL column has base columns, none of them mapped to any Power BI columns.")
                
                if export_rows:
                    export_df = pd.DataFrame(export_rows)
                    csv_export = export_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download All Mappings (CSV)",
                        data=csv_export,
                        file_name="pbi_column_mapping_details.csv",
                        mime="text/csv",
                        key="export_all_mappings_button"
                    )
                elif mapping_data_for_tab : # if there was data but filter resulted in no rows to show
                    st.caption("No mappings to display based on the current filter. Try 'All'.")


        with tab4: # Raw JSON view
            st.json(st.session_state['lineage_data'])
    
    elif analyze_button and not sql_query.strip():
        st.warning("Please enter a SQL query")

if __name__ == "__main__":
    main()