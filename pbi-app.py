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
# (Assuming your existing find_matching_powerbi_columns correctly returns a list of dicts,
# each with 'table' and 'column' keys for PBI, for a given db_column_from_sql)
# Make sure it handles cases where a DB column might map to multiple PBI columns,
# returning all possibilities. The generate_powerbi_equivalent_formula will pick one.
def find_matching_powerbi_columns(db_column_from_sql, mappings_dict):
    """Find matching PowerBI columns in the mappings dictionary.
    Returns a list of dicts, each like:
    {
        "db_column": "DB_COL_FROM_MAPPING_KEY",
        "matched_input": "DB_COLUMN_FROM_SQL_LINEAGE",
        "powerbi_column": "PBI_TABLE.PBI_COLUMN_NAME", # Full PBI identifier
        "table": "PBI_TABLE_NAME",
        "column": "PBI_COLUMN_NAME"
    }
    """
    if not db_column_from_sql or not mappings_dict or "db_to_powerbi" not in mappings_dict:
        return []
        
    norm_sql_col = normalize_column_identifier(db_column_from_sql)
    
    if not norm_sql_col:
        return []

    found_matches = []
    
    # Iterate through each database column entry in the mapping file
    for db_col_from_mapping, pbi_column_infos_list in mappings_dict["db_to_powerbi"].items():
        norm_mapping_col = normalize_column_identifier(db_col_from_mapping)

        if not norm_mapping_col: 
            continue
        
        # Primary match based on normalized identifiers
        if norm_sql_col == norm_mapping_col:
            for pbi_single_mapping_info in pbi_column_infos_list:
                found_matches.append({
                    "db_column": db_col_from_mapping,       
                    "matched_input": db_column_from_sql,    
                    "powerbi_column": pbi_single_mapping_info.get("powerbi_column"),
                    "table": pbi_single_mapping_info.get("table"),
                    "column": pbi_single_mapping_info.get("column")
                })
            # If a direct normalized match is found for the mapping key,
            # we've processed all its PBI targets for the current SQL input.
            # Continue to the next mapping key.
            continue 

        # Fallback Strategy (if primary normalized match failed for this mapping key)
        # Check if the column name part (last part) matches and one is a suffix of the other
        sql_parts = norm_sql_col.split('.')
        map_parts = norm_mapping_col.split('.')
        if sql_parts[-1] == map_parts[-1]: # Column names (last part) are the same
            # And one normalized string is a suffix of the other
            if norm_sql_col.endswith(norm_mapping_col) or norm_mapping_col.endswith(norm_sql_col):
                # Check if this specific mapping entry's PBI targets were already added from a previous, more direct match
                # This is to avoid adding the same PBI target multiple times if different normalization paths lead to it.
                # A more robust deduplication can be done at the end.
                for pbi_single_mapping_info in pbi_column_infos_list:
                    # Simplified: add and deduplicate later if necessary, or ensure this logic is tight.
                    # For now, let's assume this fallback is less common or distinct enough.
                    found_matches.append({
                        "db_column": db_col_from_mapping,
                        "matched_input": db_column_from_sql,
                        "powerbi_column": pbi_single_mapping_info.get("powerbi_column"),
                        "table": pbi_single_mapping_info.get("table"),
                        "column": pbi_single_mapping_info.get("column")
                    })

    # Deduplicate found_matches to ensure each unique PBI target for the input SQL column is listed once.
    if found_matches:
        unique_matches_tuples = set()
        unique_found_matches = []
        for match_dict in found_matches:
            # Define uniqueness by the input SQL column and the target PBI table.column
            identifying_tuple = (
                match_dict.get("matched_input"), # The original SQL column string we are trying to map
                match_dict.get("table"),         # Target PBI table
                match_dict.get("column")         # Target PBI column
            )
            if identifying_tuple not in unique_matches_tuples:
                unique_matches_tuples.add(identifying_tuple)
                unique_found_matches.append(match_dict)
        return unique_found_matches
            
    return []


def generate_powerbi_equivalent_formula(original_sql_expression, base_columns_from_lineage, column_mappings_dict):
    """
    Replaces database column identifiers in a SQL expression with their 
    Power BI DAX equivalents ('Table Name'[Column Name]) based on mappings.
    Returns the modified expression and a boolean indicating if changes were made.
    """
    # print(f"\n--- Debugging generate_powerbi_equivalent_formula ---")
    # print(f"Original SQL Expression: {original_sql_expression}")
    # print(f"Base Columns from Lineage: {base_columns_from_lineage}")

    if not original_sql_expression or not base_columns_from_lineage or not column_mappings_dict:
        # print("Exiting early: Missing original_sql_expression, base_columns, or column_mappings_dict")
        return original_sql_expression, False

    replacements = {} 

    sorted_unique_base_columns = sorted(list(set(base_columns_from_lineage)), key=len, reverse=True)
    # print(f"Sorted Unique Base Columns for Replacement: {sorted_unique_base_columns}")

    for sql_base_col_str in sorted_unique_base_columns:
        # print(f"  Processing Base SQL Column: {sql_base_col_str}")
        pbi_matches = find_matching_powerbi_columns(sql_base_col_str, column_mappings_dict)
        # print(f"    PBI Matches found: {pbi_matches}")
        
        if pbi_matches:
            first_match = pbi_matches[0] 
            pbi_table = first_match.get("table")
            pbi_column = first_match.get("column")
            # print(f"    First PBI Match - Table: {pbi_table}, Column: {pbi_column}")

            if pbi_table and pbi_column:
                dax_table_ref = f"'{pbi_table}'" 
                dax_column_ref = f"[{pbi_column}]"
                dax_full_ref = f"{dax_table_ref}{dax_column_ref}"
                # print(f"    Constructed DAX Ref: {dax_full_ref}")
                replacements[sql_base_col_str] = dax_full_ref
            # else:
                # print(f"    Skipping DAX ref construction: PBI table or column missing in match.")
    
    # print(f"Replacements dictionary built: {replacements}")
    if not replacements:
        # print("No replacements generated. Returning original expression.")
        return original_sql_expression, False

    modified_expression = original_sql_expression
    made_change = False

    for sql_token_to_replace in sorted_unique_base_columns:
        if sql_token_to_replace in replacements:
            dax_equivalent = replacements[sql_token_to_replace]
            # print(f"  Attempting to replace: '{sql_token_to_replace}' with '{dax_equivalent}'")
            if sql_token_to_replace in modified_expression:
                modified_expression = modified_expression.replace(sql_token_to_replace, dax_equivalent)
                made_change = True
                # print(f"    Replaced. Current expression: {modified_expression}")
            # else:
                # print(f"    Token '{sql_token_to_replace}' not found in current expression for replacement.")
            
    # print(f"Final Modified Expression: {modified_expression}")
    # print(f"Made Change: {made_change}")
    # print(f"--- End Debugging ---")
    return modified_expression, made_change

# ... (generate_dax_from_sql - AI based - remains the same) ...
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
    if 'dax_expressions' not in st.session_state: # For AI DAX
        st.session_state['dax_expressions'] = {}
    
    if 'column_mappings' not in st.session_state:
        st.session_state['column_mappings'] = load_column_mappings()
    
    if 'mapping_results' not in st.session_state: # For PBI Mapping Tab
        st.session_state['mapping_results'] = None
    
    st.title("SQL to Power BI Column Mapper")
    st.markdown("""
    This tool analyzes SQL queries to understand column lineage and maps them to PowerBI columns using the column mapping database.
    It can also attempt to translate SQL expressions to Power BI DAX-like formulas.
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
                                    "pbi_matches": pbi_matches # This will be a list of PBI targets
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
                    
                    pbi_eq_formula = item.get('final_expression', "") # Initialize with original SQL
                    made_change_in_rule_based_translation = False # Flag for rule-based translation

                    if item['type'] == 'expression' and item.get('final_expression'):
                        # Display original SQL Expression
                        formatted_expr = sqlparse.format(
                            item['final_expression'],
                            reindent=True,
                            keyword_case='upper',
                            indent_width=2
                        )
                        st.write("**SQL Expression:**")
                        st.code(formatted_expr, language="sql")
                        
                        # --- Generate and Display Power BI Equivalent Formula (Rule-Based) ---
                        st.markdown("---")
                        st.write("**Power BI Equivalent Formula:**")
                        if st.session_state.get('column_mappings') and item.get('base_columns'):
                            # Use the original, unformatted expression for replacement
                            # Store the result of rule-based translation
                            pbi_eq_formula, made_change_in_rule_based_translation = generate_powerbi_equivalent_formula(
                                item['final_expression'], 
                                item.get('base_columns'), 
                                st.session_state['column_mappings']
                            )
                            
                            if made_change_in_rule_based_translation:
                                st.code(pbi_eq_formula, language="dax") 
                            else:
                                # If no change, pbi_eq_formula still holds the original SQL.
                                # We can indicate that no rule-based translation occurred.
                                st.caption("Could not translate to a distinct Power BI equivalent using rules (or no mappings found for base columns in the expression).")
                                pbi_eq_formula = item['final_expression'] # Ensure it's the original SQL for AI if no rule change
                        elif not item.get('base_columns'):
                            st.caption("SQL expression has no identified base columns to map for rule-based translation.")
                        elif not item.get('final_expression'):
                             st.caption("SQL expression is empty.")
                        else: 
                            st.warning("Mapping file not loaded. Cannot generate Power BI equivalent formula using rules.")
                        st.markdown("---")
                                                
                        # --- AI DAX Generation ---
                        item_id = f"{item['column']}_{i}" 
                        
                        # Determine which expression to send to AI
                        expression_for_ai = pbi_eq_formula if made_change_in_rule_based_translation else item['final_expression']
                        
                        if st.button(f"Generate DAX", key=f"dax_btn_{item_id}"):
                            if expression_for_ai and expression_for_ai.strip():
                                with st.spinner("Generating DAX..."):
                                    # Send the potentially translated expression to the AI
                                    dax_results = generate_dax_from_sql(expression_for_ai)
                                    st.session_state['dax_expressions'][item_id] = dax_results
                            else:
                                st.warning("Expression for AI is empty. Cannot generate DAX.")


                        if item_id in st.session_state['dax_expressions']:
                            dax_results = st.session_state['dax_expressions'][item_id]
                            recommendation = dax_results.get("recommendation", "").lower()
                            if recommendation == "measure":
                                st.info("💡 **Recommendation:** **MEASURE**")
                            elif "calculated column" in recommendation: 
                                st.info("💡 **Recommendation:** **CALCULATED COLUMN**")
                            elif recommendation and recommendation != "error":
                                 st.info(f"💡 **Recommendation:** {recommendation.upper()}")

                            st.write("**Generated DAX Measure:**")
                            st.code(dax_results.get("measure", "Not provided or error."), language="dax")
                            st.write("**Generated DAX Calculated Column:**")
                            st.code(dax_results.get("calculated_column", "Not provided or error."), language="dax")
                    
                    elif item['type'] == 'expression': 
                        st.code("No expression available for this column.", language="text")
                        
                    st.write("**Base columns (from SQL Lineage):**")
                    if item.get('base_columns'):
                        for col in item['base_columns']:
                            st.write(f"- `{col}`")
                    else:
                        st.write("N/A (Direct column or no base columns identified by lineage analyzer)")

                    # --- PBI Mapping for individual base columns (Detail) ---
                    st.markdown("---") 
                    st.write("**PBI Mapping for Individual Base Columns:**")
                    if not item.get('base_columns'):
                        st.caption("No base columns to show individual PBI mappings for.")
                    elif not st.session_state.get('column_mappings'):
                        st.warning("Mapping file not loaded. PBI mappings cannot be displayed.")
                    else:
                        for base_col_idx, base_col_str in enumerate(item['base_columns']):
                            norm_base_col = normalize_column_identifier(base_col_str)
                            st.markdown(f"  - **Base Column {base_col_idx+1}:** `{base_col_str}` <br>&nbsp;&nbsp;&nbsp;&nbsp;Normalized: `{norm_base_col}`", unsafe_allow_html=True)
                            
                            pbi_matches_for_this_base_col = find_matching_powerbi_columns(base_col_str, st.session_state['column_mappings'])
                            
                            if pbi_matches_for_this_base_col:
                                for match_idx, match_info in enumerate(pbi_matches_for_this_base_col):
                                    pbi_table_name = match_info.get('table', 'N/A')
                                    pbi_col_name = match_info.get('column', 'N/A')
                                    dax_ref_display = f"'{pbi_table_name}'[{pbi_col_name}]" if pbi_table_name != 'N/A' else "N/A"

                                    st.markdown(f"""
                                        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- PBI Target {match_idx+1}: `{match_info.get('powerbi_column', 'N/A')}` (DAX: `{dax_ref_display}`)
                                        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Table: `{pbi_table_name}`
                                        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Column: `{pbi_col_name}`
                                        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- (Source DB in Mapping: `{match_info.get('db_column', 'N/A')}`)
                                    """, unsafe_allow_html=True)
                            else:
                                st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- *No PowerBI mapping found for this base column.*", unsafe_allow_html=True)
                            st.markdown("<br>", unsafe_allow_html=True) # Space between base columns
        
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
                            
                            has_at_least_one_pbi_mapping_shown_in_expander = False
                            for base_map_info in data["base_column_mappings"]:
                                st.markdown(f"  - **Base:** `{base_map_info['original_base_col']}` (Normalized: `{base_map_info['normalized_base_col']}`)")
                                if base_map_info["pbi_matches"]:
                                    has_at_least_one_pbi_mapping_shown_in_expander = True
                                    for pbi_match_idx, pbi_match in enumerate(base_map_info["pbi_matches"]):
                                        pbi_table_name_tab3 = pbi_match.get('table', 'N/A')
                                        pbi_col_name_tab3 = pbi_match.get('column', 'N/A')
                                        dax_ref_tab3 = f"'{pbi_table_name_tab3}'[{pbi_col_name_tab3}]" if pbi_table_name_tab3 != 'N/A' else "N/A"
                                        
                                        st.markdown(f"    - PBI Target {pbi_match_idx+1}: `{pbi_match.get('powerbi_column','N/A')}` (DAX: `{dax_ref_tab3}`)")
                                        st.markdown(f"      (Source DB in Mapping: `{pbi_match.get('db_column','N/A')}`)")
                                        export_rows.append({
                                            "SQL Output Column": sql_col_name,
                                            "SQL Column Type": data['type'],
                                            "SQL Base Column": base_map_info['original_base_col'],
                                            "Normalized SQL Base Column": base_map_info['normalized_base_col'],
                                            "Mapped PBI Column Full Path": pbi_match.get('powerbi_column','N/A'),
                                            "PBI Table": pbi_table_name_tab3,
                                            "PBI Column Name": pbi_col_name_tab3,
                                            "PBI DAX Reference": dax_ref_tab3,
                                            "Source DB in Mapping File": pbi_match.get('db_column','N/A')
                                        })
                                else:
                                    st.markdown("    - *No PowerBI mapping found for this base column.*")
                                    export_rows.append({ 
                                        "SQL Output Column": sql_col_name,
                                        "SQL Column Type": data['type'],
                                        "SQL Base Column": base_map_info['original_base_col'],
                                        "Normalized SQL Base Column": base_map_info['normalized_base_col'],
                                        "Mapped PBI Column Full Path": "N/A",
                                        "PBI Table": "N/A",
                                        "PBI Column Name": "N/A",
                                        "PBI DAX Reference": "N/A",
                                        "Source DB in Mapping File": "N/A"
                                    })
                            if not data["base_column_mappings"]: # If SQL col has no base columns for lineage
                                 export_rows.append({
                                    "SQL Output Column": sql_col_name,
                                    "SQL Column Type": data['type'],
                                    "SQL Base Column": "N/A (No base columns from lineage)",
                                    "Normalized SQL Base Column": "N/A",
                                    "Mapped PBI Column Full Path": "N/A",
                                    "PBI Table": "N/A",
                                    "PBI Column Name": "N/A",
                                    "PBI DAX Reference": "N/A",
                                    "Source DB in Mapping File": "N/A"
                                })

                            if not has_at_least_one_pbi_mapping_shown_in_expander and data["base_column_mappings"]:
                                st.info("Although this SQL column has base columns, none of them mapped to any Power BI columns.")
                
                if export_rows:
                    export_df = pd.DataFrame(export_rows)
                    csv_export = export_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download All Mappings (CSV)",
                        data=csv_export,
                        file_name="pbi_column_mapping_details.csv",
                        mime="text/csv",
                        key="export_all_mappings_button_tab3" # Ensure unique key
                    )
                elif mapping_data_for_tab : 
                    st.caption("No mappings to display based on the current filter. Try 'All'.")

        with tab4: # Raw JSON view
            st.json(st.session_state['lineage_data'])
    
    elif analyze_button and not sql_query.strip():
        st.warning("Please enter a SQL query")

if __name__ == "__main__":
    main()