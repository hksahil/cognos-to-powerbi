import streamlit as st
import json
import pandas as pd
import sqlparse
import google.generativeai as genai
import os
import re
from pathlib import Path
import yaml # For YAML processing
from io import StringIO
import subprocess # For running external commands




class FlowDict(dict):
    pass

def flow_dict_representer(dumper, data):
    return dumper.represent_mapping(dumper.DEFAULT_MAPPING_TAG, data, flow_style=True)

class CustomDumper(yaml.SafeDumper):
    pass

CustomDumper.add_representer(FlowDict, flow_dict_representer)

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


def generate_powerbi_equivalent_formula(original_sql_expression, base_columns_from_lineage, column_mappings_dict, resolved_base_col_to_pbi=None):
    if not original_sql_expression or not base_columns_from_lineage or not column_mappings_dict:
        return original_sql_expression, False

    replacements = {}
    sorted_unique_base_columns = sorted(list(set(base_columns_from_lineage)), key=len, reverse=True)

    for sql_base_col_str in sorted_unique_base_columns:
        dax_full_ref = None
        # Use resolved mapping if provided
        if resolved_base_col_to_pbi and sql_base_col_str in resolved_base_col_to_pbi:
            dax_full_ref = resolved_base_col_to_pbi[sql_base_col_str]
        else:
            resolved_label = st.session_state.get('base_col_ambiguity_choices', {}).get(sql_base_col_str)
            pbi_matches = find_matching_powerbi_columns(sql_base_col_str, column_mappings_dict)
            if resolved_label and pbi_matches:
                resolved = next((m for m in pbi_matches if f"'{m['table']}'[{m['column']}]" == resolved_label), None)
                if resolved:
                    dax_full_ref = resolved_label
            elif pbi_matches:
                first_match = pbi_matches[0]
                pbi_table = first_match.get("table")
                pbi_column = first_match.get("column")
                if pbi_table and pbi_column:
                    dax_full_ref = f"'{pbi_table}'[{pbi_column}]"
        if dax_full_ref:
            replacements[sql_base_col_str] = dax_full_ref

    if not replacements:
        return original_sql_expression, False

    modified_expression = original_sql_expression
    made_change = False

    for sql_token_to_replace in sorted_unique_base_columns:
        if sql_token_to_replace in replacements:
            dax_equivalent = replacements[sql_token_to_replace]
            if sql_token_to_replace in modified_expression:
                modified_expression = modified_expression.replace(sql_token_to_replace, dax_equivalent)
                made_change = True

    return modified_expression, made_change



# ... (generate_dax_from_sql - AI based - remains the same) ...
def generate_dax_from_sql(sql_expression):
    try:
        model = genai.GenerativeModel('gemini-2.5-flash-preview-05-20')
        # Define the allowed data types for the prompt
        data_type_options = [
            "text", "whole number", "decimal number", "date/time", 
            "date", "time", "true/false", "fixed decimal number", "binary"
        ]
        prompt = f"""
        Analyze the following SQL expression and provide:
        1. An equivalent PowerBI DAX expression for a MEASURE (properly formatted with line breaks and indentation for readability, don't give name to the measure, only show expression)
        2. An equivalent PowerBI DAX expression for a CALCULATED COLUMN (properly formatted with line breaks and indentation for readability, don't give name to the calculated column only show expression)
        3. A recommendation on whether this should be implemented as a measure or calculated column in PowerBI based on its characteristics
        4. A suitable Power BI DATA TYPE for the MEASURE. Choose one from the following list: {', '.join(data_type_options)}.

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
        DATA_TYPE: decimal number
        """

        response = model.generate_content(prompt)
        dax_response = response.text.strip()

        sections = {
            'measure': '',
            'calculated_column': '',
            'recommendation': '',
            'dataType': 'text'  # Default dataType
        }

        measure_marker = "MEASURE:"
        calc_col_marker = "CALCULATED_COLUMN:"
        rec_marker = "RECOMMENDATION:"
        datatype_marker = "DATA_TYPE:" # Changed from FORMAT_STRING

        idx_measure = dax_response.find(measure_marker)
        idx_calc_col = dax_response.find(calc_col_marker)
        idx_rec = dax_response.find(rec_marker)
        idx_datatype = dax_response.find(datatype_marker) # Changed

        if idx_measure != -1:
            start_measure = idx_measure + len(measure_marker)
            end_measure = idx_calc_col if idx_calc_col != -1 else (idx_rec if idx_rec != -1 else (idx_datatype if idx_datatype != -1 else len(dax_response)))
            sections['measure'] = dax_response[start_measure:end_measure].strip()

        if idx_calc_col != -1:
            start_calc_col = idx_calc_col + len(calc_col_marker)
            end_calc_col = idx_rec if idx_rec != -1 else (idx_datatype if idx_datatype != -1 else len(dax_response))
            sections['calculated_column'] = dax_response[start_calc_col:end_calc_col].strip()

        if idx_rec != -1:
            start_rec = idx_rec + len(rec_marker)
            end_rec = idx_datatype if idx_datatype != -1 else len(dax_response)
            sections['recommendation'] = dax_response[start_rec:end_rec].strip()
        
        if idx_datatype != -1: # Changed
            start_datatype = idx_datatype + len(datatype_marker) # Changed
            sections['dataType'] = dax_response[start_datatype:].strip() # Changed

        # Clean up measure and calculated_column DAX
        for key in ['measure', 'calculated_column']:
            sections[key] = sections[key].replace('```dax', '').replace('```', '')
            if sections[key].lstrip().startswith('dax'):
                sections[key] = sections[key].lstrip()[3:].lstrip()
            if sections[key].lstrip().startswith('DAX'):
                sections[key] = sections[key].lstrip()[3:].lstrip()
            sections[key] = sections[key].rstrip('`').strip()
        
        # Clean up dataType (remove potential quotes and validate against allowed list)
        dt = sections['dataType']
        if dt.startswith('"') and dt.endswith('"'):
            dt = dt[1:-1]
        if dt.startswith("'") and dt.endswith("'"):
            dt = dt[1:-1]
        
        if dt.lower() not in data_type_options: # Validate
            sections['dataType'] = 'text' # Fallback to default if AI gives invalid type
        else:
            sections['dataType'] = dt.lower() # Store in lowercase for consistency

        return sections
    except Exception as e:
        return {
            "measure": f"Error: {str(e)}",
            "calculated_column": f"Error: {str(e)}",
            "recommendation": "error",
            "dataType": "text" # Default dataType on error
        }
    


# --- Rebuild visual_config_candidates if any ambiguity choice changed ---
def build_visual_candidates():
    visual_candidates = []
    for item_vis_conf in st.session_state['lineage_data']:
        sql_name = item_vis_conf['column']
        is_analyzer_expression_type = item_vis_conf['type'] == 'expression'
        original_sql_content_for_expr = item_vis_conf.get('final_expression')
        base_columns_from_lineage = item_vis_conf.get('base_columns')
        pbi_options_for_item = []

        if not is_analyzer_expression_type:
            # Try mapping the SQL output column directly
            pbi_matches = find_matching_powerbi_columns(sql_name, st.session_state['column_mappings'])

            # If not mapped, and has a single base column, try mapping the base column.
            if not pbi_matches and base_columns_from_lineage and len(base_columns_from_lineage) == 1:
                base_col = base_columns_from_lineage[0]
                pbi_matches = find_matching_powerbi_columns(base_col, st.session_state['column_mappings'])
                # Use resolved mapping if ambiguity was resolved for this base column
                resolved_label = st.session_state['base_col_ambiguity_choices'].get(base_col)
                if resolved_label and pbi_matches:
                    resolved = next((m for m in pbi_matches if f"'{m['table']}'[{m['column']}]" == resolved_label), None)
                    if resolved:
                        pbi_options_for_item = [{
                            'display_label': resolved_label,
                            'pbi_dax_reference': resolved_label,
                            'table': resolved['table'],
                            'column': resolved['column'],
                            'is_expression_translation': False,
                            'original_sql_column_alias': sql_name,
                            'original_sql_expression': None
                        }]
            # Use resolved mapping if ambiguity was resolved for this output column
            resolved_label = st.session_state['base_col_ambiguity_choices'].get(sql_name)
            if resolved_label and pbi_matches:
                resolved = next((m for m in pbi_matches if f"'{m['table']}'[{m['column']}]" == resolved_label), None)
                if resolved:
                    pbi_options_for_item = [{
                        'display_label': resolved_label,
                        'pbi_dax_reference': resolved_label,
                        'table': resolved['table'],
                        'column': resolved['column'],
                        'is_expression_translation': False,
                        'original_sql_column_alias': sql_name,
                        'original_sql_expression': None
                    }]
            elif pbi_matches and not pbi_options_for_item:
                for match in pbi_matches:
                    tbl = match.get("table")
                    col = match.get("column")
                    if tbl and col:
                        pbi_dax_ref = f"'{tbl}'[{col}]"
                        pbi_options_for_item.append({
                            'display_label': pbi_dax_ref,
                            'pbi_dax_reference': pbi_dax_ref,
                            'table': tbl, 'column': col, 'is_expression_translation': False,
                            'original_sql_column_alias': sql_name,
                            'original_sql_expression': None
                        })
            if not pbi_options_for_item:
                pbi_options_for_item.append({
                    'display_label': sql_name,
                    'pbi_dax_reference': sql_name,
                    'is_expression_translation': False,
                    'original_sql_column_alias': sql_name,
                    'original_sql_expression': None
                })
        else:
            # Type is "expression"
            display_label_for_dropdown = sql_name # Always show SQL alias for expressions in dropdown

            # Determine the actual PBI DAX reference for this expression
            actual_pbi_dax_reference = original_sql_content_for_expr or sql_name # Default
            made_change = False
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
    return visual_candidates


def enrich_selected_items(selected_labels):
    import re
    enriched = []
    for label in selected_labels:
        candidate = next((c for c in st.session_state['visual_config_candidates'] if c['chosen_display_label'] == label), None)
        if candidate:
            entry = {
                "label": label,
                "type": "expression" if candidate.get("is_sql_expression_type_from_analyzer") else "base",
                "sql_name": candidate.get("sql_name"),
                "pbi_expression": None,
                "pbi_table": None, # Initialize for both types
                "pbi_column": None # Initialize for base type
            }
            if entry["type"] == "expression":
                lineage_item = next((item for item in st.session_state['lineage_data'] if item['column'] == candidate.get("sql_name")), None)
                if lineage_item:
                    orig_sql_expr = lineage_item.get('final_expression')
                    base_columns = lineage_item.get('base_columns', [])
                    if orig_sql_expr and base_columns:
                        pbi_expr, _ = generate_powerbi_equivalent_formula(
                            orig_sql_expr,
                            base_columns,
                            st.session_state['column_mappings'],
                            st.session_state.get('resolved_base_col_to_pbi', {})
                        )
                        entry["pbi_expression"] = pbi_expr
                        # Try to extract the first table name from the translated PBI expression
                        if pbi_expr:
                            # Regex to find the first instance of 'TableName'[ColumnName]
                            # It will capture 'TableName'
                            match_table = re.search(r"'([^']+?)'\[[^\]]+?\]", pbi_expr)
                            if match_table:
                                entry["pbi_table"] = match_table.group(1)
            else: # type is "base"
                # Extract table and column from label like: 'Table'[Column]
                m = re.match(r"'(.+?)'\[(.+?)\]", label)
                if m:
                    entry["pbi_table"] = m.group(1)
                    entry["pbi_column"] = m.group(2)
                # If no match, pbi_table and pbi_column remain None (as initialized)
            enriched.append(entry)
    return enriched



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

    if 'base_col_ambiguity_choices' not in st.session_state:
        st.session_state['base_col_ambiguity_choices'] = {}

    if 'visual_selected_values' not in st.session_state:
        st.session_state['visual_selected_values'] = []
    if 'visual_ai_dax_results' not in st.session_state: # New session state
        st.session_state['visual_ai_dax_results'] = {}

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
                    # --- New Code Block: Extract and store all unique types from lineage data ---
                    types_in_data = set(item['type'] if item['type'] != 'column' else 'base' for item in st.session_state['lineage_data'])
                    st.session_state['all_types'] = sorted(types_in_data)
                    
                    # --- Prepare data for Visual Configuration ---
                    visual_candidates = []
                    for item_vis_conf in st.session_state['lineage_data']:
                        sql_name = item_vis_conf['column']
                        is_analyzer_expression_type = item_vis_conf['type'] == 'expression'
                        original_sql_content_for_expr = item_vis_conf.get('final_expression')
                        base_columns_from_lineage = item_vis_conf.get('base_columns')
                        pbi_options_for_item = []

                        if not is_analyzer_expression_type:
                            # Try mapping the SQL output column directly
                            pbi_matches = find_matching_powerbi_columns(sql_name, st.session_state['column_mappings'])

                            # If not mapped, and has a single base column, try mapping the base column.
                            if not pbi_matches and base_columns_from_lineage and len(base_columns_from_lineage) == 1:
                                base_col = base_columns_from_lineage[0]
                                pbi_matches = find_matching_powerbi_columns(base_col, st.session_state['column_mappings'])
                                # Use resolved mapping if ambiguity was resolved for this base column
                                resolved_label = st.session_state['base_col_ambiguity_choices'].get(base_col)
                                if resolved_label and pbi_matches:
                                    resolved = next((m for m in pbi_matches if f"'{m['table']}'[{m['column']}]" == resolved_label), None)
                                    if resolved:
                                        pbi_options_for_item = [{
                                            'display_label': resolved_label,
                                            'pbi_dax_reference': resolved_label,
                                            'table': resolved['table'],
                                            'column': resolved['column'],
                                            'is_expression_translation': False,
                                            'original_sql_column_alias': sql_name,
                                            'original_sql_expression': None
                                        }]
                            # Use resolved mapping if ambiguity was resolved for this output column
                            resolved_label = st.session_state['base_col_ambiguity_choices'].get(sql_name)
                            if resolved_label and pbi_matches:
                                resolved = next((m for m in pbi_matches if f"'{m['table']}'[{m['column']}]" == resolved_label), None)
                                if resolved:
                                    pbi_options_for_item = [{
                                        'display_label': resolved_label,
                                        'pbi_dax_reference': resolved_label,
                                        'table': resolved['table'],
                                        'column': resolved['column'],
                                        'is_expression_translation': False,
                                        'original_sql_column_alias': sql_name,
                                        'original_sql_expression': None
                                    }]
                            elif pbi_matches and not pbi_options_for_item:
                                for match in pbi_matches:
                                    tbl = match.get("table")
                                    col = match.get("column")
                                    if tbl and col:
                                        pbi_dax_ref = f"'{tbl}'[{col}]"
                                        pbi_options_for_item.append({
                                            'display_label': pbi_dax_ref,
                                            'pbi_dax_reference': pbi_dax_ref,
                                            'table': tbl, 'column': col, 'is_expression_translation': False,
                                            'original_sql_column_alias': sql_name,
                                            'original_sql_expression': None
                                        })
                            if not pbi_options_for_item:
                                pbi_options_for_item.append({
                                    'display_label': sql_name,
                                    'pbi_dax_reference': sql_name,
                                    'is_expression_translation': False,
                                    'original_sql_column_alias': sql_name,
                                    'original_sql_expression': None
                                })
                        else:
                            # Type is "expression"
                            display_label_for_dropdown = sql_name # Always show SQL alias for expressions in dropdown

                            # Determine the actual PBI DAX reference for this expression
                            actual_pbi_dax_reference = original_sql_content_for_expr or sql_name # Default
                            made_change = False
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
            filtered_df_tab1 = df[df['type'].replace('column', 'base').isin(selected_types_tab1)] if selected_types_tab1 else df
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

            # --- Advanced: Resolve Base Database Column Ambiguities ---
        st.markdown("### Advanced: Resolve Base Database Column Ambiguities")
        if 'base_col_ambiguity_choices' not in st.session_state:
            st.session_state['base_col_ambiguity_choices'] = {}
    
        # 1. Gather all unique base columns from all lineage items
        all_base_columns = set()
        for item in st.session_state['lineage_data']:
            for base_col in item.get('base_columns', []):
                all_base_columns.add(base_col)
    
        # 2. For each base column, check for multiple PBI mappings
        base_col_to_matches = {}
        for base_col in all_base_columns:
            matches = find_matching_powerbi_columns(base_col, st.session_state['column_mappings'])
            if matches and len(matches) > 1:
                base_col_to_matches[base_col] = matches
    
        # 3. For each ambiguous base column, also collect where it is used
        for base_col, matches in base_col_to_matches.items():
            # Find all output columns (base and expression) where this base_col is used
            used_in_base = []
            used_in_expr = []
            for item in st.session_state['lineage_data']:
                if base_col in item.get('base_columns', []):
                    if item['type'] == 'expression':
                        used_in_expr.append(item['column'])
                    else:
                        used_in_base.append(item['column'])
    
            options = [f"'{m['table']}'[{m['column']}]" for m in matches]
            current_choice = st.session_state['base_col_ambiguity_choices'].get(base_col, options[0])
            chosen = st.radio(
                f"Choose PBI mapping for base database column `{base_col}`:",
                options,
                index=options.index(current_choice) if current_choice in options else 0,
                key=f"base_col_ambiguity_{base_col}"
            )
            st.session_state['base_col_ambiguity_choices'][base_col] = chosen
    
            # Show where this base column is used
            st.caption("**Used in output columns:**")
            if used_in_base:
                st.markdown(f"- As base column in: {', '.join(f'`{col}`' for col in used_in_base)}")
            if used_in_expr:
                st.markdown(f"- As part of expression in: {', '.join(f'`{col}`' for col in used_in_expr)}")
            if not used_in_base and not used_in_expr:
                st.markdown("- Not used in any output columns (unexpected)")

        


        # Build resolved_base_col_to_pbi mapping
        resolved_base_col_to_pbi = {}
        for item in st.session_state['lineage_data']:
            for base_col in item.get('base_columns', []):
                matches = find_matching_powerbi_columns(base_col, st.session_state['column_mappings'])
                resolved_label = st.session_state['base_col_ambiguity_choices'].get(base_col)
                pbi_ref = None
                if resolved_label and matches:
                    # Use resolved mapping
                    resolved = next((m for m in matches if f"'{m['table']}'[{m['column']}]" == resolved_label), None)
                    if resolved:
                        pbi_ref = resolved_label
                elif matches:
                    # Use first mapping if not ambiguous
                    m = matches[0]
                    pbi_ref = f"'{m['table']}'[{m['column']}]"
                if pbi_ref:
                    resolved_base_col_to_pbi[base_col] = pbi_ref
        st.session_state['resolved_base_col_to_pbi'] = resolved_base_col_to_pbi



        st.session_state['visual_config_candidates'] = build_visual_candidates()

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
                if item not in [x["label"] for x in st.session_state.get('visual_selected_columns', [])] and \
                   item not in [x["label"] for x in st.session_state.get('visual_selected_values', [])]
            ]
            current_selected_rows = [x["label"] for x in st.session_state.get('visual_selected_rows', []) if x["label"] in options_for_rows]
            temp_selected_rows = st.multiselect(
                "Rows:", options_for_rows, default=current_selected_rows, key="matrix_rows"
            )

            options_for_columns = [
                item for item in all_available_display_labels_for_visual 
                if item not in temp_selected_rows and \
                   item not in [x["label"] for x in st.session_state.get('visual_selected_values', [])]
            ]
            current_selected_columns = [x["label"] for x in st.session_state.get('visual_selected_columns', []) if x["label"] in options_for_columns]
            temp_selected_columns = st.multiselect(
                "Columns:", options_for_columns, default=current_selected_columns, key="matrix_columns"
            )

            options_for_values = [
                item for item in all_available_display_labels_for_visual 
                if item not in temp_selected_rows and \
                   item not in temp_selected_columns
            ]
            current_selected_values = [x["label"] for x in st.session_state.get('visual_selected_values', []) if x["label"] in options_for_values]
            temp_selected_values = st.multiselect(
                "Values:", options_for_values, default=current_selected_values, key="matrix_values"
            )

            # Save button
            if st.button("Save Matrix Selection"):
                st.session_state['visual_selected_rows'] = enrich_selected_items(temp_selected_rows)
                st.session_state['visual_selected_columns'] = enrich_selected_items(temp_selected_columns)
                st.session_state['visual_selected_values'] = enrich_selected_items(temp_selected_values)
                st.success("Matrix selection saved!")

            st.write("Current Matrix Configuration (Display Labels):")
            st.write("Rows:", st.session_state.get('visual_selected_rows', []))
            st.write("Columns:", st.session_state.get('visual_selected_columns', []))
            st.write("Values:", st.session_state.get('visual_selected_values', []))


            st.markdown("---")
            st.subheader("AI DAX Generation for Selected Expressions")

            # Button to trigger DAX generation
            if st.button("Generate DAX with AI for Selected Matrix Items"):
                st.session_state['visual_ai_dax_results'] = {} # Clear previous results
                items_to_process_for_ai = []
                for item_list_name, item_list in [
                    ("Row", st.session_state.get('visual_selected_rows', [])),
                    ("Column", st.session_state.get('visual_selected_columns', [])),
                    ("Value", st.session_state.get('visual_selected_values', []))
                ]:
                    for item_detail in item_list:
                        if item_detail.get("type") == "expression" and item_detail.get("pbi_expression"):
                            items_to_process_for_ai.append({
                                "label": item_detail["label"],
                                "pbi_expression": item_detail["pbi_expression"],
                                "category": item_list_name
                            })
                
                if not items_to_process_for_ai:
                    st.info("No expressions found in the current matrix selection to generate DAX for.")
                else:
                    with st.spinner("Generating DAX with AI for selected items..."):
                        for item_to_process in items_to_process_for_ai:
                            label = item_to_process["label"]
                            pbi_expr = item_to_process["pbi_expression"]
                            category = item_to_process["category"]
                            unique_key = f"{category}_{label}"

                            dax_results = generate_dax_from_sql(pbi_expr) # dax_results now includes 'dataType'
                            st.session_state['visual_ai_dax_results'][unique_key] = {
                                "label": label,
                                "input_pbi_expression": pbi_expr,
                                "ai_output": dax_results, # This dictionary contains the dataType
                                "category": category
                            }
                    overall_config_updated = False
                    for list_key_str, category_name_str in [
                        ('visual_selected_rows', "Row"),
                        ('visual_selected_columns', "Column"),
                        ('visual_selected_values', "Value")
                    ]:
                        if list_key_str in st.session_state:
                            current_list_in_state = st.session_state[list_key_str]
                            for item_dict_idx in range(len(current_list_in_state)):
                                item_dict = current_list_in_state[item_dict_idx]

                                if item_dict.get("type") == "expression":
                                    ai_result_lookup_key = f"{category_name_str}_{item_dict['label']}"

                                    had_previous_ai_dax = "ai_generated_dax" in item_dict
                                    current_item_modified = False

                                    if ai_result_lookup_key in st.session_state['visual_ai_dax_results']:
                                        ai_result_data = st.session_state['visual_ai_dax_results'][ai_result_lookup_key]
                                        ai_output_data = ai_result_data['ai_output'] 
                                        recommendation_data = ai_output_data.get("recommendation", "").lower()

                                        if "measure" in recommendation_data:
                                            measure_dax_from_ai = ai_output_data.get("measure")
                                            data_type_from_ai = ai_output_data.get("dataType", "text") # Get dataType

                                            if measure_dax_from_ai and not measure_dax_from_ai.startswith("Error:") and measure_dax_from_ai != "Not provided or error.":
                                                item_dict["ai_generated_dax"] = measure_dax_from_ai
                                                item_dict["ai_dataType"] = data_type_from_ai # Store dataType
                                                current_item_modified = True

                                    if not current_item_modified and had_previous_ai_dax:
                                        if "ai_generated_dax" in item_dict:
                                            del item_dict["ai_generated_dax"]
                                        if "ai_dataType" in item_dict: # Also remove dataType if DAX is removed
                                            del item_dict["ai_dataType"]
                                        current_item_modified = True

                                    if current_item_modified:
                                        overall_config_updated = True

                    st.success(f"AI DAX generation complete for {len(st.session_state['visual_ai_dax_results'])} items.")
                    if overall_config_updated:
                        st.rerun()
            
            # Display generated DAX results
            if st.session_state['visual_ai_dax_results']:
                st.markdown("---")
                for key, result_info in st.session_state['visual_ai_dax_results'].items():
                    st.markdown(f"#### AI DAX for {result_info['category']}: `{result_info['label']}`")
                    st.markdown("**Input PBI Expression (Rule-based):**")
                    st.code(result_info['input_pbi_expression'], language="dax")
                    
                    ai_output = result_info['ai_output']
                    recommendation = ai_output.get("recommendation", "").lower()

                    if "measure" in recommendation:
                        st.write("**AI Generated DAX Measure:**")
                        st.code(ai_output.get("measure", "Not provided or error."), language="dax")
                    elif "calculated column" in recommendation:
                        st.info("Calculated Column DAX generation is a Work In Progress.")
                    else:
                        # Fallback for other recommendations or errors - you might want to show both or a generic message
                        st.error("AI could not generate a specific DAX measure or an error occurred.")
                        
                    st.markdown("---")


        elif st.session_state['visual_type'] == "Table":
            st.markdown("#### Configure Table Visual")
            st.info("Table visual configuration will be implemented later.")

        st.markdown("---")
        st.header("PBI Automation `config.yaml` Generation")

        if st.button("Generate PBI Automation Config File"):
            try:
                new_config = {}

                # --- Hardcoded Static Fields ---
                new_config['projectName'] = "1.1.10.117. Daily Report Renault"
                new_config['dataset'] = {
                    "connection": {
                        "connectionString": "Data Source=powerbi://api.powerbi.com/v1.0/myorg/EMEA Development;Initial Catalog=\"EU Order to Cash (Ad-hoc)\";Access Mode=readonly;Integrated Security=ClaimsToken",
                        "database": "7f97f9b2-2c89-4359-966b-4612b960fbb1"
                    },
                    "modelName": "EU Order to Cash (Ad-Hoc)"
                }
                new_config['report'] = {
                    'title': FlowDict({ # title can be FlowDict if it's simple
                        "text": "1.1.10.117. Daily Report Renault"
                    }),
                    'data_refresh': FlowDict({ # data_refresh can be FlowDict
                        "table": "Date Refresh Table",
                        "column": "UPDATED_DATE"
                    })
                }

                # --- Generate Measures (Dynamic) ---
                generated_measures = []
                all_selected_items_for_measures = (
                    st.session_state.get('visual_selected_rows', []) +
                    st.session_state.get('visual_selected_columns', []) +
                    st.session_state.get('visual_selected_values', [])
                )
                processed_measure_labels = set()

                for item in all_selected_items_for_measures:
                    if item.get("type") == "expression" and item.get("ai_generated_dax") and item.get("label") not in processed_measure_labels:
                        if item.get("pbi_table"):
                            generated_measures.append(FlowDict({
                                "name": item["label"] + " Measure", 
                                "table": item["pbi_table"],
                                "expression": item["ai_generated_dax"],
                                "dataType": item.get("ai_dataType", "text") # Use stored AI dataType
                            }))
                            processed_measure_labels.add(item["label"])
                        # ... (else warning)
    
                base_measures_from_example = [
                    FlowDict({
                        "name": "Current Year", "table": "Order To Cash (OTC)",
                        "expression": "SELECTEDVALUE('Order To Cash (OTC)'[Year]) = YEAR(TODAY())",
                        "dataType": "true/false" # Updated from formatString
                    }),
                    FlowDict({
                        "name": "Current Month", "table": "Order To Cash (OTC)",
                        "expression": "SELECTEDVALUE('Order To Cash (OTC)'[Month]) = Month(TODAY())",
                        "dataType": "true/false" # Updated from formatString
                    })
                ]
                new_config['report']['measures'] = base_measures_from_example + generated_measures

                # --- Generate Visuals (Dynamic Rows/Cols/Values for the first Matrix) ---
                matrix_visual_definition = { # This main visual dict remains standard (block)
                    "type": "matrix",
                    "position": FlowDict({ "x": 28.8, "y": 100, "width": 1220, "height": 400 }), # position is FlowDict
                    "rows": [],
                    "columns": [],
                    "values": [],
                    "filters": [ # Hardcoded filters, each item wrapped in FlowDict
                        FlowDict({
                            "field": FlowDict({ "name": "Company ID", "table": "Country", "type": "column" }),
                            "filterType": "Categorical", "values": [ "E211" ]
                        }),
                        FlowDict({
                            "field": FlowDict({ "name": "Distribution Channel Code", "table": "Distribution Channel", "type": "column" }),
                            "filterType": "Categorical", "values": [ "01" ]
                        }),
                        FlowDict({
                            "field": FlowDict({ "name": "PAK Hierarchy Level 1 Code", "table": "Product Aggregation Key (PAK)", "type": "column" }),
                            "filterType": "Categorical", "values": [ "10" ]
                        }),
                        FlowDict({
                            "field": FlowDict({ "name": "Year", "table": "Order To Cash (OTC)", "type": "Column" }),
                            "filterType": "Categorical", "values": [ 2025 ] }),
                        FlowDict({
                            "field": FlowDict({ "name": "Month", "table": "Order To Cash (OTC)", "type": "Column" }),
                            "filterType": "Categorical", "values": [ 6 ] }),
                        FlowDict({
                            "field": FlowDict({ "name": "Payer Hierarchy Level 4 ID", "table": "Payer Customer (Hierarchy)", "type": "column" }),
                            "filterType": "Categorical", "values": [ "DE4RENZENT" ]
                        })
                    ]
                }

                for r_item in st.session_state.get('visual_selected_rows', []):
                    item_data = {}
                    if r_item.get("type") == "base":
                        item_data = {
                            "name": r_item.get("pbi_column", r_item["label"]),
                            "table": r_item.get("pbi_table", "UnknownTable"),
                            "type": "Column"
                        }
                    elif r_item.get("type") == "expression" and r_item.get("ai_generated_dax"):
                        item_data = {
                            "name": r_item["label"] + " Measure", # Ensure this matches the measure name in new_config['report']['measures']
                            "table": r_item.get("pbi_table", "UnknownTable"),
                            "type": "Measure"
                        }
                    if item_data:
                        matrix_visual_definition['rows'].append(FlowDict(item_data))

                # Populate Columns
                for c_item in st.session_state.get('visual_selected_columns', []):
                    item_data = {}
                    if c_item.get("type") == "base":
                        item_data = {
                            "name": c_item.get("pbi_column", c_item["label"]),
                            "table": c_item.get("pbi_table", "UnknownTable"),
                            "type": "Column"
                        }
                    elif c_item.get("type") == "expression" and c_item.get("ai_generated_dax"):
                        item_data = {
                            "name": c_item["label"] + " Measure",
                            "table": c_item.get("pbi_table", "UnknownTable"),
                            "type": "Measure"
                        }
                    if item_data:
                        matrix_visual_definition['columns'].append(FlowDict(item_data))

                # Populate Values
                for v_item in st.session_state.get('visual_selected_values', []):
                    item_data = {}
                    if v_item.get("type") == "base":
                        item_data = {
                            "name": v_item.get("pbi_column", v_item["label"]),
                            "table": v_item.get("pbi_table", "UnknownTable"),
                            "type": "Column"
                        }
                    elif v_item.get("type") == "expression" and v_item.get("ai_generated_dax"):
                        item_data = {
                            "name": v_item["label"] + " Measure",
                            "table": v_item.get("pbi_table", "UnknownTable"),
                            "type": "Measure"
                        }
                    if item_data:
                        matrix_visual_definition['values'].append(FlowDict(item_data))

                new_config['report']['visuals'] = [matrix_visual_definition]

                # Convert Python dict to YAML string for display and download
                yaml_string_io = StringIO()
                yaml.dump(new_config, yaml_string_io, Dumper=CustomDumper, sort_keys=False, indent=2, allow_unicode=True)
                generated_yaml_str = yaml_string_io.getvalue()

                st.session_state['generated_pbi_config'] = generated_yaml_str.strip()
                st.success("PBI Automation config.yaml generated successfully!")

                local_config_filename = "config.yaml" # Save in the same directory as pbi-app.py
                app_dir = Path(__file__).parent # Get the directory of the current script (pbi-app.py)
                local_config_path = app_dir / local_config_filename

                with open(local_config_path, 'w', encoding='utf-8') as f:
                    f.write(st.session_state['generated_pbi_config'])
                st.info(f"Generated `config.yaml` saved locally to: {local_config_path}")

                # Define the command to run PBI Automation
                # Ensure paths are correct for your environment
                pbi_automation_python_exe = r"C:\Users\NileshPhapale\Desktop\PBI Automation\.venv\Scripts\python.exe"
                pbi_automation_main_script = r"C:\Users\NileshPhapale\Desktop\PBI Automation\main.py"
                
                # Command to execute. The --config argument will use the config.yaml in the PBI Automation script's CWD.
                # We need to run the PBI Automation script from its own directory so it finds the config.yaml we place there.
                
                # Path to the PBI Automation directory
                pbi_automation_dir = Path(pbi_automation_main_script).parent
                # Path where the config file should be for the PBI automation script
                pbi_automation_config_path = pbi_automation_dir / "config.yaml"

                # Save the config file in the PBI Automation directory
                with open(pbi_automation_config_path, 'w', encoding='utf-8') as f:
                    f.write(st.session_state['generated_pbi_config'])
                st.info(f"Copied `config.yaml` to PBI Automation directory: {pbi_automation_config_path}")

                command_to_run = [
                    pbi_automation_python_exe,
                    pbi_automation_main_script,
                    "--config", "config.yaml" # This will be relative to the PBI Automation script's CWD
                ]

                st.markdown("---")
                st.subheader("Running PBI Automation Script...")
                st.code(" ".join(command_to_run)) # Display the command being run

                with st.spinner(f"Executing PBI Automation script from {pbi_automation_dir}..."):
                    # Run the command from the PBI Automation script's directory
                    process = subprocess.run(
                        command_to_run,
                        capture_output=True,
                        text=True,
                        cwd=pbi_automation_dir # Set the current working directory for the subprocess
                    )
                
                st.markdown("#### PBI Automation Output:")
                if process.stdout:
                    st.text_area("Standard Output:", value=process.stdout, height=200, key="pbi_stdout")
                if process.stderr:
                    st.text_area("Standard Error (if any):", value=process.stderr, height=150, key="pbi_stderr")
                
                if process.returncode == 0:
                    st.success("PBI Automation script executed successfully.")
                else:
                    st.error(f"PBI Automation script execution failed with return code: {process.returncode}")

            except Exception as e:
                st.error(f"An unexpected error occurred: {e}")
                st.exception(e) 
                # st.session_state['generated_pbi_config'] = None # Already handled if generation fails
    
        if st.session_state.get('generated_pbi_config'):
            st.subheader("Generated `config.yaml` Content (for review)") # Changed subheader slightly
            st.code(st.session_state['generated_pbi_config'], language="yaml")
            st.download_button(
                label="Download Generated config.yaml",
                data=st.session_state['generated_pbi_config'],
                file_name="generated_config.yaml", # Keep this distinct from the one used by the script
                mime="text/yaml"
            )



    elif analyze_button and not sql_query.strip():
        st.warning("Please enter a SQL query")





if __name__ == "__main__":
    main()