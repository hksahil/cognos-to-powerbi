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
    if 'dax_expressions' not in st.session_state:
        st.session_state['dax_expressions'] = {}
    if 'column_mappings' not in st.session_state:
        st.session_state['column_mappings'] = load_column_mappings()
    if 'mapping_results' not in st.session_state:
        st.session_state['mapping_results'] = None

    # New session state for visual configuration
    if 'visual_type' not in st.session_state:
        st.session_state['visual_type'] = "Matrix"
    if 'visual_config_candidates' not in st.session_state:
        # Structure: {'id': sql_name, 'sql_name': sql_name, 'is_sql_expression': bool, 
        #             'pbi_options': [{'display_label': str, 'pbi_dax_reference': str, ...}], 
        #             'chosen_display_label': str, 'chosen_pbi_dax_reference': str}
        st.session_state['visual_config_candidates'] = []
    if 'visual_ambiguity_choices' not in st.session_state:
        # Stores user's choice for ambiguous items: {sql_name: chosen_display_label}
        st.session_state['visual_ambiguity_choices'] = {}
    if 'visual_selected_rows' not in st.session_state:
        st.session_state['visual_selected_rows'] = []
    if 'visual_selected_columns' not in st.session_state:
        st.session_state['visual_selected_columns'] = []
    if 'visual_selected_values' not in st.session_state:
        st.session_state['visual_selected_values'] = []
    
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
            st.session_state['mapping_results'] = None
            st.session_state['visual_config_candidates'] = []
            st.session_state['visual_ambiguity_choices'] = {}
            st.session_state['visual_selected_rows'] = []
            st.session_state['visual_selected_columns'] = []
            st.session_state['visual_selected_values'] = []
            st.rerun()
    
    if analyze_button and sql_query.strip():
        try:
            with st.spinner("Analyzing query..."):
                analyzer = SQLLineageAnalyzer(sql_query, dialect="snowflake")
                st.session_state['lineage_data'] = analyzer.analyze()
                
                if st.session_state['lineage_data']:
                    # ... (existing logic for df, all_types, mapping_results_for_tab) ...

                    # --- Prepare data for Visual Configuration ---
                    visual_candidates = []
                    for item_vis_conf in st.session_state['lineage_data']:
                        sql_name = item_vis_conf['column']
                        is_analyzer_expression_type = item_vis_conf['type'] == 'expression'
                        original_sql_content_for_expr = item_vis_conf.get('final_expression')
                        base_columns_from_lineage = item_vis_conf.get('base_columns')
                        pbi_options_for_item = []

                        if not is_analyzer_expression_type:
                            # Type is "base" (or not 'expression')
                            # Attempt 1: Map the sql_name (output name/alias) directly
                            pbi_matches = find_matching_powerbi_columns(sql_name, st.session_state['column_mappings'])
                            
                            # Attempt 2: If sql_name didn't map, and it has a single base column, try mapping the base column.
                            if not pbi_matches and base_columns_from_lineage and len(base_columns_from_lineage) == 1:
                                pbi_matches = find_matching_powerbi_columns(base_columns_from_lineage[0], st.session_state['column_mappings'])

                            if pbi_matches:
                                for match in pbi_matches:
                                    tbl = match.get("table")
                                    col = match.get("column")
                                    if tbl and col:
                                        pbi_dax_ref = f"'{tbl}'[{col}]"
                                        pbi_options_for_item.append({
                                            'display_label': pbi_dax_ref,  # Show PBI mapped name
                                            'pbi_dax_reference': pbi_dax_ref,
                                            'table': tbl, 'column': col, 'is_expression_translation': False,
                                            'original_sql_column_alias': sql_name, # The original SQL output name
                                            'original_sql_expression': None
                                        })
                            else: # No PBI mapping found for this "base" type
                                pbi_options_for_item.append({
                                    'display_label': sql_name, # Show SQL alias
                                    'pbi_dax_reference': sql_name, # Use SQL alias as reference
                                    'is_expression_translation': False,
                                    'original_sql_column_alias': sql_name,
                                    'original_sql_expression': None
                                })
                        else:
                            # Type is "expression"
                            display_label_for_dropdown = sql_name # Always show SQL alias for expressions in dropdown

                            # Determine the actual PBI DAX reference for this expression
                            actual_pbi_dax_reference = original_sql_content_for_expr or sql_name # Default
                            if original_sql_content_for_expr:
                                translated_expr, made_change = generate_powerbi_equivalent_formula(
                                    original_sql_content_for_expr,
                                    base_columns_from_lineage,
                                    st.session_state['column_mappings']
                                )
                                if made_change:
                                    actual_pbi_dax_reference = translated_expr
                                # else, actual_pbi_dax_reference remains original_sql_content_for_expr
                            
                            pbi_options_for_item.append({
                                'display_label': display_label_for_dropdown,
                                'pbi_dax_reference': actual_pbi_dax_reference,
                                'is_expression_translation': made_change if original_sql_content_for_expr else False,
                                'original_sql_expression': original_sql_content_for_expr,
                                'original_sql_column_alias': sql_name
                            })

                        # Add to visual_candidates
                        if pbi_options_for_item:
                            default_chosen_display_label = pbi_options_for_item[0]['display_label']
                            default_chosen_pbi_dax_reference = pbi_options_for_item[0]['pbi_dax_reference']

                            pre_chosen_display_label_from_session = st.session_state.get('visual_ambiguity_choices', {}).get(sql_name)
                            if pre_chosen_display_label_from_session:
                                found_option_for_pre_choice = next((opt for opt in pbi_options_for_item if opt['display_label'] == pre_chosen_display_label_from_session), None)
                                if found_option_for_pre_choice:
                                    default_chosen_display_label = found_option_for_pre_choice['display_label']
                                    default_chosen_pbi_dax_reference = found_option_for_pre_choice['pbi_dax_reference']
                            
                            visual_candidates.append({
                                'id': sql_name, 
                                'sql_name': sql_name,
                                'is_sql_expression_type_from_analyzer': is_analyzer_expression_type,
                                'pbi_options': pbi_options_for_item,
                                'chosen_display_label': default_chosen_display_label,
                                'chosen_pbi_dax_reference': default_chosen_pbi_dax_reference
                            })
                    st.session_state['visual_config_candidates'] = visual_candidates
                    st.session_state['visual_selected_rows'] = []
                    st.session_state['visual_selected_columns'] = []
                    st.session_state['visual_selected_values'] = []
        except Exception as e:
            st.error(f"Error analyzing query or preparing visual candidates: {str(e)}")
            st.exception(e)

    # --- Display Analysis Results Tabs (existing logic) ---
    if st.session_state['lineage_data']:
        st.subheader("Analysis Results")
        df = pd.DataFrame(st.session_state['lineage_data'])
        tab1, tab2, tab3, tab4 = st.tabs(["Table View", "Detail View", "PBI Mapping", "Raw JSON"])
        # ... (Tab1, Tab2, Tab3, Tab4 logic - ensure variable names are unique if needed, like item_detail_data, etc.) ...
        with tab1: # Table View
            selected_types_tab1 = st.multiselect(
                "Filter by type:",
                options=st.session_state['all_types'],
                default=st.session_state['all_types'],
                key="filter_types_tab1_vis" 
            )
            filtered_df_tab1 = df[df['type'].isin(selected_types_tab1)] if selected_types_tab1 else df
            st.dataframe(filtered_df_tab1, use_container_width=True)
            csv_tab1 = filtered_df_tab1.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV",
                data=csv_tab1,
                file_name="lineage_analysis.csv",
                mime="text/csv",
                key="download_csv_tab1_vis" 
            )
        
        with tab2: # Detail View
            selected_types_tab2 = st.multiselect(
                "Filter by type:",
                options=st.session_state['all_types'],
                default=st.session_state['all_types'],  
                key="filter_types_tab2_vis" 
            )
            filtered_items_tab2 = [item_detail for item_detail in st.session_state['lineage_data'] if item_detail['type'] in selected_types_tab2] if selected_types_tab2 else st.session_state['lineage_data']
            for i_detail, item_detail_data in enumerate(filtered_items_tab2): 
                with st.expander(f"Column: {item_detail_data['column']} ({item_detail_data['type']})"):
                    st.write("**Type:** ", item_detail_data['type'])
                    pbi_eq_formula_detail = item_detail_data.get('final_expression', "") 
                    made_change_in_rule_based_translation_detail = False 
                    if item_detail_data['type'] == 'expression' and item_detail_data.get('final_expression'):
                        formatted_expr_detail = sqlparse.format(
                            item_detail_data['final_expression'],
                            reindent=True,
                            keyword_case='upper',
                            indent_width=2
                        )
                        st.write("**SQL Expression:**")
                        st.code(formatted_expr_detail, language="sql")
                        st.markdown("---")
                        st.write("**Power BI Equivalent Formula (Rule-Based Translation):**")
                        if st.session_state.get('column_mappings') and item_detail_data.get('base_columns'):
                            pbi_eq_formula_detail, made_change_in_rule_based_translation_detail = generate_powerbi_equivalent_formula(
                                item_detail_data['final_expression'], 
                                item_detail_data.get('base_columns'), 
                                st.session_state['column_mappings']
                            )
                            if made_change_in_rule_based_translation_detail:
                                st.code(pbi_eq_formula_detail, language="dax") 
                            else:
                                st.caption("Could not translate to a distinct Power BI equivalent using rules (or no mappings found for base columns in the expression).")
                                pbi_eq_formula_detail = item_detail_data['final_expression'] 
                        elif not item_detail_data.get('base_columns'):
                            st.caption("SQL expression has no identified base columns to map for rule-based translation.")
                        elif not item_detail_data.get('final_expression'):
                             st.caption("SQL expression is empty.")
                        else: 
                            st.warning("Mapping file not loaded. Cannot generate Power BI equivalent formula using rules.")
                        st.markdown("---")
                        item_id_detail = f"{item_detail_data['column']}_{i_detail}" 
                        expression_for_ai_detail = pbi_eq_formula_detail if made_change_in_rule_based_translation_detail else item_detail_data['final_expression']
                        if st.button(f"Generate DAX with AI", key=f"dax_btn_{item_id_detail}_vis"): 
                            if expression_for_ai_detail and expression_for_ai_detail.strip():
                                with st.spinner("Generating DAX with AI..."):
                                    dax_results_detail = generate_dax_from_sql(expression_for_ai_detail)
                                    st.session_state['dax_expressions'][item_id_detail] = dax_results_detail
                            else:
                                st.warning("Expression for AI is empty. Cannot generate DAX.")
                        if item_id_detail in st.session_state['dax_expressions']:
                            dax_results_render = st.session_state['dax_expressions'][item_id_detail] 
                            recommendation_render = dax_results_render.get("recommendation", "").lower()
                            if recommendation_render == "measure":
                                st.info("💡 **AI Recommendation:** **MEASURE**")
                            elif "calculated column" in recommendation_render: 
                                st.info("💡 **AI Recommendation:** **CALCULATED COLUMN**")
                            elif recommendation_render and recommendation_render != "error":
                                 st.info(f"💡 **AI Recommendation:** {recommendation_render.upper()}")
                            st.write("**AI Generated DAX Measure:**")
                            st.code(dax_results_render.get("measure", "Not provided or error."), language="dax")
                            st.write("**AI Generated DAX Calculated Column:**")
                            st.code(dax_results_render.get("calculated_column", "Not provided or error."), language="dax")
                    elif item_detail_data['type'] == 'expression': 
                        st.code("No expression available for this column.", language="text")
                    st.write("**Base columns (from SQL Lineage):**")
                    if item_detail_data.get('base_columns'):
                        for col_detail in item_detail_data['base_columns']:
                            st.write(f"- `{col_detail}`")
                    else:
                        st.write("N/A (Direct column or no base columns identified by lineage analyzer)")
                    st.markdown("---") 
                    st.write("**PBI Mapping for Individual Base Columns:**")
                    if not item_detail_data.get('base_columns'):
                        st.caption("No base columns to show individual PBI mappings for.")
                    elif not st.session_state.get('column_mappings'):
                        st.warning("Mapping file not loaded. PBI mappings cannot be displayed.")
                    else:
                        for base_col_idx_detail, base_col_str_detail in enumerate(item_detail_data['base_columns']):
                            norm_base_col_detail = normalize_column_identifier(base_col_str_detail)
                            st.markdown(f"  - **Base Column {base_col_idx_detail+1}:** `{base_col_str_detail}` <br>&nbsp;&nbsp;&nbsp;&nbsp;Normalized: `{norm_base_col_detail}`", unsafe_allow_html=True)
                            pbi_matches_for_this_base_col_detail = find_matching_powerbi_columns(base_col_str_detail, st.session_state['column_mappings'])
                            if pbi_matches_for_this_base_col_detail:
                                for match_idx_detail, match_info_detail in enumerate(pbi_matches_for_this_base_col_detail):
                                    pbi_table_name_detail = match_info_detail.get('table', 'N/A')
                                    pbi_col_name_detail = match_info_detail.get('column', 'N/A')
                                    dax_ref_display_detail = f"'{pbi_table_name_detail}'[{pbi_col_name_detail}]" if pbi_table_name_detail != 'N/A' else "N/A"
                                    st.markdown(f"""
                                        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- PBI Target {match_idx_detail+1}: `{match_info_detail.get('powerbi_column', 'N/A')}` (DAX: `{dax_ref_display_detail}`)
                                        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Table: `{pbi_table_name_detail}`
                                        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Column: `{pbi_col_name_detail}`
                                        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- (Source DB in Mapping: `{match_info_detail.get('db_column', 'N/A')}`)
                                    """, unsafe_allow_html=True)
                            else:
                                st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- *No PowerBI mapping found for this base column.*", unsafe_allow_html=True)
                            st.markdown("<br>", unsafe_allow_html=True) 
        with tab3: 
            st.header("Consolidated Power BI Column Mappings")
            if not st.session_state.get('column_mappings'):
                st.warning("Mapping file not loaded. Please check sidebar and console for errors.")
            elif not st.session_state.get('mapping_results'):
                st.info("No SQL query analyzed yet, or the query resulted in no columns to map.")
            else:
                mapping_filter_tab3 = st.radio( 
                    "Show SQL Columns:",
                    ["All", "Mapped Only", "Unmapped Only"],
                    horizontal=True,
                    key="pbi_mapping_tab_filter_tab3"
                )
                mapping_data_for_tab3 = st.session_state['mapping_results']
                total_sql_cols_tab3 = len(mapping_data_for_tab3)
                mapped_sql_cols_count_tab3 = sum(1 for data_tab3 in mapping_data_for_tab3.values() if data_tab3.get("is_mapped_overall"))
                unmapped_sql_cols_count_tab3 = total_sql_cols_tab3 - mapped_sql_cols_count_tab3
                m_col1_tab3, m_col2_tab3, m_col3_tab3 = st.columns(3)
                m_col1_tab3.metric("Total SQL Columns", total_sql_cols_tab3)
                m_col2_tab3.metric("Mapped SQL Columns", mapped_sql_cols_count_tab3)
                m_col3_tab3.metric("Unmapped SQL Columns", unmapped_sql_cols_count_tab3)
                export_rows_tab3 = []
                for sql_col_name_tab3, data_val_tab3 in mapping_data_for_tab3.items(): 
                    is_overall_mapped_tab3 = data_val_tab3.get("is_mapped_overall", False)
                    display_this_sql_col_tab3 = False
                    if mapping_filter_tab3 == "All":
                        display_this_sql_col_tab3 = True
                    elif mapping_filter_tab3 == "Mapped Only" and is_overall_mapped_tab3:
                        display_this_sql_col_tab3 = True
                    elif mapping_filter_tab3 == "Unmapped Only" and not is_overall_mapped_tab3:
                        display_this_sql_col_tab3 = True
                    if display_this_sql_col_tab3:
                        expander_title_tab3 = f"SQL Column: {sql_col_name_tab3} ({data_val_tab3['type']})"
                        expander_title_tab3 += " ✅ (Mapped)" if is_overall_mapped_tab3 else " ❌ (Unmapped)"
                        with st.expander(expander_title_tab3):
                            if not data_val_tab3["base_column_mappings"]:
                                st.caption("This SQL column has no identified base columns.")
                            has_at_least_one_pbi_mapping_shown_in_expander_tab3 = False
                            for base_map_info_tab3 in data_val_tab3["base_column_mappings"]:
                                st.markdown(f"  - **Base:** `{base_map_info_tab3['original_base_col']}` (Normalized: `{base_map_info_tab3['normalized_base_col']}`)")
                                if base_map_info_tab3["pbi_matches"]:
                                    has_at_least_one_pbi_mapping_shown_in_expander_tab3 = True
                                    for pbi_match_idx_tab3, pbi_match_data_tab3 in enumerate(base_map_info_tab3["pbi_matches"]): 
                                        pbi_table_name_render_tab3 = pbi_match_data_tab3.get('table', 'N/A') 
                                        pbi_col_name_render_tab3 = pbi_match_data_tab3.get('column', 'N/A') 
                                        dax_ref_render_tab3 = f"'{pbi_table_name_render_tab3}'[{pbi_col_name_render_tab3}]" if pbi_table_name_render_tab3 != 'N/A' else "N/A"
                                        st.markdown(f"    - PBI Target {pbi_match_idx_tab3+1}: `{pbi_match_data_tab3.get('powerbi_column','N/A')}` (DAX: `{dax_ref_render_tab3}`)")
                                        st.markdown(f"      (Source DB in Mapping: `{pbi_match_data_tab3.get('db_column','N/A')}`)")
                                        export_rows_tab3.append({
                                            "SQL Output Column": sql_col_name_tab3, "SQL Column Type": data_val_tab3['type'],
                                            "SQL Base Column": base_map_info_tab3['original_base_col'], "Normalized SQL Base Column": base_map_info_tab3['normalized_base_col'],
                                            "Mapped PBI Column Full Path": pbi_match_data_tab3.get('powerbi_column','N/A'), "PBI Table": pbi_table_name_render_tab3,
                                            "PBI Column Name": pbi_col_name_render_tab3, "PBI DAX Reference": dax_ref_render_tab3,
                                            "Source DB in Mapping File": pbi_match_data_tab3.get('db_column','N/A')
                                        })
                                else:
                                    st.markdown("    - *No PowerBI mapping found for this base column.*")
                                    export_rows_tab3.append({ 
                                        "SQL Output Column": sql_col_name_tab3, "SQL Column Type": data_val_tab3['type'],
                                        "SQL Base Column": base_map_info_tab3['original_base_col'], "Normalized SQL Base Column": base_map_info_tab3['normalized_base_col'],
                                        "Mapped PBI Column Full Path": "N/A", "PBI Table": "N/A", "PBI Column Name": "N/A", "PBI DAX Reference": "N/A",
                                        "Source DB in Mapping File": "N/A"
                                    })
                            if not data_val_tab3["base_column_mappings"]: 
                                 export_rows_tab3.append({
                                    "SQL Output Column": sql_col_name_tab3, "SQL Column Type": data_val_tab3['type'],
                                    "SQL Base Column": "N/A (No base columns from lineage)", "Normalized SQL Base Column": "N/A",
                                    "Mapped PBI Column Full Path": "N/A", "PBI Table": "N/A", "PBI Column Name": "N/A", "PBI DAX Reference": "N/A",
                                    "Source DB in Mapping File": "N/A"
                                })
                            if not has_at_least_one_pbi_mapping_shown_in_expander_tab3 and data_val_tab3["base_column_mappings"]:
                                st.info("Although this SQL column has base columns, none of them mapped to any Power BI columns.")
                if export_rows_tab3:
                    export_df_tab3 = pd.DataFrame(export_rows_tab3)
                    csv_export_tab3 = export_df_tab3.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download All Mappings (CSV)", data=csv_export_tab3,
                        file_name="pbi_column_mapping_details.csv", mime="text/csv",
                        key="export_all_mappings_button_tab3_vis" 
                    )
                elif mapping_data_for_tab3 : 
                    st.caption("No mappings to display based on the current filter. Try 'All'.")
        with tab4: 
            st.json(st.session_state['lineage_data'])

    # --- New Section: Visual Configuration ---
    if st.session_state['lineage_data'] and st.session_state['visual_config_candidates']:
        st.markdown("---")
        st.subheader("Visual Configuration")

        st.session_state['visual_type'] = st.radio(
            "Select Visual Type:",
            ["Matrix", "Table"],
            index=["Matrix", "Table"].index(st.session_state['visual_type']),
            key="visual_type_selector"
        )

        current_visual_config_candidates = [dict(candidate) for candidate in st.session_state['visual_config_candidates']]
        rerun_for_ambiguity_update = False
        
        for idx, candidate in enumerate(current_visual_config_candidates):
            if len(candidate['pbi_options']) > 1:
                current_choice_for_sql_name = st.session_state['visual_ambiguity_choices'].get(candidate['sql_name'])
                # Check if current choice is still valid among the options
                valid_current_choice = False
                if current_choice_for_sql_name:
                    if any(opt['display_label'] == current_choice_for_sql_name for opt in candidate['pbi_options']):
                        valid_current_choice = True
                
                if not valid_current_choice: # Prompt or re-prompt if choice is not set or invalid
                    st.markdown(f"**Choose PBI Representation for SQL Column/Expression:** `{candidate['sql_name']}`")
                    options_display_labels = [opt['display_label'] for opt in candidate['pbi_options']]
                    
                    default_index = 0
                    if current_choice_for_sql_name and current_choice_for_sql_name in options_display_labels: # Should not happen if !valid_current_choice
                        default_index = options_display_labels.index(current_choice_for_sql_name)
                    
                    chosen_display_label = st.radio(
                        f"Select PBI item for '{candidate['sql_name']}':",
                        options_display_labels,
                        index=default_index,
                        key=f"ambiguity_choice_{candidate['id']}"
                    )
                    
                    # Find the corresponding pbi_dax_reference for the chosen_display_label
                    chosen_option_dict = next((opt for opt in candidate['pbi_options'] if opt['display_label'] == chosen_display_label), None)
                    if chosen_option_dict:
                        st.session_state['visual_ambiguity_choices'][candidate['sql_name']] = chosen_display_label
                        current_visual_config_candidates[idx]['chosen_display_label'] = chosen_display_label
                        current_visual_config_candidates[idx]['chosen_pbi_dax_reference'] = chosen_option_dict['pbi_dax_reference']
                        rerun_for_ambiguity_update = True 
                else: # Choice already exists and is valid, ensure candidate reflects it
                    chosen_option_dict = next((opt for opt in candidate['pbi_options'] if opt['display_label'] == current_choice_for_sql_name), None)
                    if chosen_option_dict:
                        current_visual_config_candidates[idx]['chosen_display_label'] = chosen_option_dict['display_label']
                        current_visual_config_candidates[idx]['chosen_pbi_dax_reference'] = chosen_option_dict['pbi_dax_reference']

            elif candidate['pbi_options']: # Only one option, no ambiguity
                current_visual_config_candidates[idx]['chosen_display_label'] = candidate['pbi_options'][0]['display_label']
                current_visual_config_candidates[idx]['chosen_pbi_dax_reference'] = candidate['pbi_options'][0]['pbi_dax_reference']
            else: # No PBI options
                 current_visual_config_candidates[idx]['chosen_display_label'] = None
                 current_visual_config_candidates[idx]['chosen_pbi_dax_reference'] = None
        
        st.session_state['visual_config_candidates'] = current_visual_config_candidates
        
        if rerun_for_ambiguity_update:
            st.rerun()

        all_available_display_labels_for_visual = sorted(list(set(
            candidate['chosen_display_label'] 
            for candidate in st.session_state['visual_config_candidates'] 
            if candidate.get('chosen_display_label')
        )))

        if st.session_state['visual_type'] == "Matrix":
            st.markdown("#### Configure Matrix Visual")
            options_for_rows = [
                item for item in all_available_display_labels_for_visual 
                if item not in st.session_state.get('visual_selected_columns', []) and \
                   item not in st.session_state.get('visual_selected_values', [])
            ]
            current_selected_rows = [r for r in st.session_state.get('visual_selected_rows', []) if r in options_for_rows]
            new_selected_rows = st.multiselect(
                "Rows:", options_for_rows, default=current_selected_rows, key="matrix_rows"
            )
            if new_selected_rows != st.session_state.get('visual_selected_rows', []):
                st.session_state['visual_selected_rows'] = new_selected_rows
                st.rerun()

            options_for_columns = [
                item for item in all_available_display_labels_for_visual 
                if item not in st.session_state.get('visual_selected_rows', []) and \
                   item not in st.session_state.get('visual_selected_values', [])
            ]
            current_selected_columns = [c for c in st.session_state.get('visual_selected_columns', []) if c in options_for_columns]
            new_selected_columns = st.multiselect(
                "Columns:", options_for_columns, default=current_selected_columns, key="matrix_columns"
            )
            if new_selected_columns != st.session_state.get('visual_selected_columns', []):
                st.session_state['visual_selected_columns'] = new_selected_columns
                st.rerun()

            options_for_values = [
                item for item in all_available_display_labels_for_visual 
                if item not in st.session_state.get('visual_selected_rows', []) and \
                   item not in st.session_state.get('visual_selected_columns', [])
            ]
            current_selected_values = [v for v in st.session_state.get('visual_selected_values', []) if v in options_for_values]
            new_selected_values = st.multiselect(
                "Values:", options_for_values, default=current_selected_values, key="matrix_values"
            )
            if new_selected_values != st.session_state.get('visual_selected_values', []):
                st.session_state['visual_selected_values'] = new_selected_values
                st.rerun()

            st.write("Current Matrix Configuration (Display Labels):")
            st.write("Rows:", st.session_state.get('visual_selected_rows', []))
            st.write("Columns:", st.session_state.get('visual_selected_columns', []))
            st.write("Values:", st.session_state.get('visual_selected_values', []))

            # For debugging: show the actual DAX references for selected items
            # selected_rows_dax = [next((c['chosen_pbi_dax_reference'] for c in st.session_state['visual_config_candidates'] if c['chosen_display_label'] == row_label), None) for row_label in st.session_state.get('visual_selected_rows', [])]
            # selected_cols_dax = [next((c['chosen_pbi_dax_reference'] for c in st.session_state['visual_config_candidates'] if c['chosen_display_label'] == col_label), None) for col_label in st.session_state.get('visual_selected_columns', [])]
            # selected_vals_dax = [next((c['chosen_pbi_dax_reference'] for c in st.session_state['visual_config_candidates'] if c['chosen_display_label'] == val_label), None) for val_label in st.session_state.get('visual_selected_values', [])]
            # st.write("Actual DAX for Rows:", selected_rows_dax)
            # st.write("Actual DAX for Columns:", selected_cols_dax)
            # st.write("Actual DAX for Values:", selected_vals_dax)


        elif st.session_state['visual_type'] == "Table":
            st.markdown("#### Configure Table Visual")
            st.info("Table visual configuration will be implemented later.")

    elif analyze_button and not sql_query.strip():
        st.warning("Please enter a SQL query")

if __name__ == "__main__":
    main()