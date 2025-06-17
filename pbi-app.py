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
import shutil



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




def parse_dax_filter_for_display(dax_string):
    """
    Parses a DAX filter string (potentially SQL-like) for display purposes.
    Returns a dictionary with extracted details.
    """
    # Pattern 1: 'Table'[Column] IN (val1, val2, ...)
    # Handles values like ('E211'), ('01'), (2025), or ('Val1', 'Val2')
    match_in = re.fullmatch(r"^\s*'([^']+)'\[([^']+)\]\s+IN\s+\(\s*([^)]+)\s*\)\s*$", dax_string, re.IGNORECASE)
    if match_in:
        table, column, values_str = match_in.groups()
        raw_values = [v.strip() for v in values_str.split(',')]
        display_values = []
        for rv in raw_values:
            if (rv.startswith("'") and rv.endswith("'")) or \
               (rv.startswith('"') and rv.endswith('"')):
                display_values.append(rv[1:-1]) # Remove outer quotes
            else:
                display_values.append(rv) # Keep as is (e.g., numbers)
        
        return {
            "pbi_column_name": column,
            "pbi_table_name": table,
            "type": "Column", # Hardcoded as per request
            "filter_type": "Categorical",
            "values": display_values
        }

    # Pattern 2: 'Table'[Column] = Value
    # Handles "Value", 'Value', NumericValue, TRUE, FALSE
    match_equals = re.fullmatch(r"^\s*'([^']+)'\[([^']+)\]\s*=\s*(.+)$", dax_string, re.IGNORECASE)
    if match_equals:
        table, column, value_str = match_equals.groups()
        value_str = value_str.strip()
        display_value = value_str
        # Remove outer quotes for display if present
        if (value_str.startswith("'") and value_str.endswith("'")) or \
           (value_str.startswith('"') and value_str.endswith('"')):
            display_value = value_str[1:-1]
        
        return {
            "pbi_column_name": column,
            "pbi_table_name": table,
            "type": "Column", # Hardcoded as per request
            "filter_type": "Categorical",
            "values": [display_value] # Value as a list
        }

    # Fallback for unparsed DAX string
    return {
        "pbi_column_name": "N/A (Unparsed)",
        "pbi_table_name": "N/A",
        "type": "Column",
        "filter_type": "Unknown",
        "values": [dax_string] # Show the raw DAX as value
    }



# --- Rebuild visual_config_candidates if any ambiguity choice changed ---
def build_visual_candidates():
    visual_candidates = []
    if not st.session_state.get('lineage_data'):
        return []

    for item_vis_conf in st.session_state['lineage_data']:
        if item_vis_conf.get('type') == 'filter_condition': # Skip filter conditions
            continue

        # Use 'item' key for the SQL name/alias, which is the output column name from SELECT
        sql_name = item_vis_conf.get('item') 
        if not sql_name: # Should not happen for non-filter_condition types from SELECT
            st.warning(f"Skipping lineage item due to missing 'item' key: {item_vis_conf}")
            continue

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
        candidate = next((c for c in st.session_state.get('visual_config_candidates', []) if c.get('chosen_display_label') == label), None)
        if candidate:
            entry = {
                "label": label, # This is the chosen_display_label
                "type": "expression" if candidate.get("is_sql_expression_type_from_analyzer") else "base",
                "sql_name": candidate.get("sql_name"), # Original SQL alias/name from 'item'
                "pbi_expression": None,
                "pbi_table": None, 
                "pbi_column": None 
            }
            if entry["type"] == "expression":
                # Find the original lineage item using the 'sql_name' (which was item_vis_conf['item'])
                # Use .get('item') for safety and consistency
                lineage_item = next((item for item in st.session_state.get('lineage_data', []) if item.get('item') == candidate.get("sql_name")), None)
                if lineage_item:
                    orig_sql_expr = lineage_item.get('final_expression')
# ...existing code...
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
                            match_table = re.search(r"'([^']+?)'\[[^\]]+?\]", pbi_expr)
                            if match_table:
                                entry["pbi_table"] = match_table.group(1)
            else: # type is "base"
                m = re.match(r"'(.+?)'\[(.+?)\]", label)
                if m:
                    entry["pbi_table"] = m.group(1)
                    entry["pbi_column"] = m.group(2)
            enriched.append(entry)
    return enriched


def initialize_session_state():
    """Initializes all necessary session state variables."""
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
    if 'visual_ai_dax_results' not in st.session_state:
        st.session_state['visual_ai_dax_results'] = {}
    if 'visual_type' not in st.session_state:
        st.session_state['visual_type'] = "Matrix"
    if 'visual_config_candidates' not in st.session_state:
        st.session_state['visual_config_candidates'] = []
    if 'visual_ambiguity_choices' not in st.session_state:
        st.session_state['visual_ambiguity_choices'] = {}
    if 'visual_selected_rows' not in st.session_state:
        st.session_state['visual_selected_rows'] = []
    if 'visual_selected_columns' not in st.session_state:
        st.session_state['visual_selected_columns'] = []
    if 'generated_pbi_config' not in st.session_state:
        st.session_state['generated_pbi_config'] = None
    if 'resolved_base_col_to_pbi' not in st.session_state:
        st.session_state['resolved_base_col_to_pbi'] = {}
    if 'translated_filter_conditions' not in st.session_state:
        st.session_state['translated_filter_conditions'] = [] # For storing {sql, pbi_dax, id}
    if 'visual_selected_filters_dax' not in st.session_state:
        st.session_state['visual_selected_filters_dax'] = [] 


def display_sidebar():
    """Displays the sidebar content."""
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


def display_query_input_area():
    """Displays the SQL query input and action buttons."""
    col1, col2 = st.columns([4, 1])
    sql_query_input = ""
    analyze_button_pressed = False
    clear_button_pressed = False

    with col1:
        sql_query_input = st.text_area("Enter your SQL query:", 
                                value=st.session_state.get('sql_query', ""),
                                height=300)
        st.session_state['sql_query'] = sql_query_input # Keep session state updated
    
    with col2:
        st.write("### Actions")
        analyze_button_pressed = st.button("Analyze Query", use_container_width=True)
        clear_button_pressed = st.button("Clear Query", use_container_width=True)
        
        if clear_button_pressed:
            st.session_state['sql_query'] = ""
            st.session_state['lineage_data'] = None
            st.session_state['all_types'] = []
            st.session_state['dax_expressions'] = {}
            st.session_state['mapping_results'] = None
            st.session_state['visual_config_candidates'] = []
            st.session_state['visual_ambiguity_choices'] = {}
            st.session_state['base_col_ambiguity_choices'] = {}
            st.session_state['visual_selected_rows'] = []
            st.session_state['visual_selected_columns'] = []
            st.session_state['visual_selected_values'] = []
            st.session_state['visual_ai_dax_results'] = {}
            st.session_state['generated_pbi_config'] = None
            st.session_state['resolved_base_col_to_pbi'] = {}
            st.rerun()
            
    return sql_query_input, analyze_button_pressed


def perform_sql_analysis(sql_query):
    """Performs SQL analysis and updates session state."""
    try:
        with st.spinner("Analyzing query..."):
            analyzer = SQLLineageAnalyzer(sql_query, dialect="snowflake")
            st.session_state['lineage_data'] = analyzer.analyze()
            
            if st.session_state['lineage_data']:
                types_in_data = set()
                for item in st.session_state['lineage_data']:
                    item_type = item.get('type')
                    if item_type == 'column': 
                        types_in_data.add('base')
                    elif item_type:
                        types_in_data.add(item_type)
                st.session_state['all_types'] = sorted(list(types_in_data))
                
                # Initial pass for visual candidates (will be refined by build_visual_candidates)
                # This part is simplified as build_visual_candidates does the heavy lifting
                st.session_state['visual_config_candidates'] = build_visual_candidates() # Call the more detailed builder

                # Reset selections and AI results when query is re-analyzed
                st.session_state['visual_selected_rows'] = []
                st.session_state['visual_selected_columns'] = []
                st.session_state['visual_selected_values'] = []
                st.session_state['visual_selected_rows_labels'] = []
                st.session_state['visual_selected_columns_labels'] = []
                st.session_state['visual_selected_values_labels'] = []
                st.session_state['visual_ambiguity_choices'] = {} 
                st.session_state['base_col_ambiguity_choices'] = {} 
                st.session_state['visual_ai_dax_results'] = {} 
                st.session_state['resolved_base_col_to_pbi'] = {}
                st.session_state['translated_filter_conditions'] = []
                st.session_state['visual_selected_filters_dax'] = []

                # Prepare mapping_results (simplified, ensure your original logic is preserved or integrated here)
                # This is a placeholder for your mapping_results generation logic
                temp_mapping_results = {}
                for item_map in st.session_state['lineage_data']:
                    if item_map.get('type') != 'filter_condition':
                        sql_col_name = item_map.get('item', item_map.get('column')) # Use 'item' first
                        if sql_col_name:
                            base_cols_for_map = item_map.get('base_columns', [])
                            pbi_matches_for_map = []
                            is_mapped_overall_map = False
                            for bc_map in base_cols_for_map:
                                matches_bc = find_matching_powerbi_columns(bc_map, st.session_state['column_mappings'])
                                if matches_bc: is_mapped_overall_map = True
                                pbi_matches_for_map.append({
                                    'original_base_col': bc_map,
                                    'normalized_base_col': normalize_column_identifier(bc_map),
                                    'pbi_matches': matches_bc
                                })
                            # If not an expression and no base_columns, try to map the item itself
                            if item_map.get('type') != 'expression' and not base_cols_for_map:
                                direct_matches = find_matching_powerbi_columns(sql_col_name, st.session_state['column_mappings'])
                                if direct_matches: is_mapped_overall_map = True
                                # Add a structure for direct mapping if needed by tab3
                                pbi_matches_for_map.append({
                                     'original_base_col': sql_col_name, # Treat the item itself as a "base" for mapping display
                                     'normalized_base_col': normalize_column_identifier(sql_col_name),
                                     'pbi_matches': direct_matches
                                 })


                            temp_mapping_results[sql_col_name] = {
                                'type': item_map.get('type'),
                                'base_column_mappings': pbi_matches_for_map,
                                'is_mapped_overall': is_mapped_overall_map
                            }
                st.session_state['mapping_results'] = temp_mapping_results

    except Exception as e:
        st.error(f"Error analyzing query or preparing visual candidates: {str(e)}")
        st.exception(e)
        st.session_state['lineage_data'] = None # Clear data on error
        st.session_state['visual_config_candidates'] = []
        st.session_state['mapping_results'] = None



def display_analysis_results_tabs():
    """Displays the tabs for SQL analysis results."""
    # ... (The entire content of the 'if st.session_state['lineage_data']:' block for tabs)
    # ... (This includes tab1, tab2, tab3, tab_filters, tab4 definitions and their 'with' blocks)
    # ... (This function will be quite large, consider breaking each tab into its own function too)
    # --- Display Analysis Results Tabs (existing logic) ---
    if st.session_state['lineage_data']:
        st.subheader("Analysis Results")
        df = pd.DataFrame(st.session_state['lineage_data'])
        
        options_for_general_tabs = [t for t in st.session_state.get('all_types', []) if t != 'filter_condition']

        tab1, tab2, tab3, tab_filters, tab4 = st.tabs([
            "Table View", "Detail View", "PBI Mapping", "Filter Conditions", "Raw JSON"
        ])

        with tab1: 
            # ... (Content of Table View tab) ...
            st.header("SQL Query Analysis - Table View")
            df_display_tab1 = df[df['type'] != 'filter_condition'].copy()
            selected_types_tab1 = st.multiselect(
                "Filter by type (excluding filter conditions):",
                options=options_for_general_tabs, 
                default=options_for_general_tabs, 
                key="filter_types_tab1_vis_revised"
            )
            df_display_tab1['display_type_for_filter'] = df_display_tab1['type'].replace('column', 'base')
            if selected_types_tab1:
                filtered_df_tab1 = df_display_tab1[df_display_tab1['display_type_for_filter'].isin(selected_types_tab1)]
            else:
                filtered_df_tab1 = df_display_tab1
            st.dataframe(filtered_df_tab1.drop(columns=['display_type_for_filter']), use_container_width=True)
            if not filtered_df_tab1.empty:
                csv_tab1 = filtered_df_tab1.drop(columns=['display_type_for_filter']).to_csv(index=False).encode('utf-8')
                st.download_button(label="Download Filtered Table View (CSV)", data=csv_tab1, file_name="table_view_analysis.csv", mime="text/csv", key="download_csv_tab1_vis_revised")

        with tab2: 
            # ... (Content of Detail View tab - this is extensive) ...
            st.header("SQL Query Analysis - Detail View")
            selected_types_tab2 = st.multiselect(
                "Filter by type (excluding filter conditions):",
                options=options_for_general_tabs, 
                default=options_for_general_tabs, 
                key="filter_types_tab2_vis_revised"
            )
            items_for_detail_view = [
                item_detail for item_detail in st.session_state['lineage_data'] 
                if item_detail['type'] != 'filter_condition' and \
                   (item_detail['type'].replace('column', 'base') in selected_types_tab2 if selected_types_tab2 else True)
            ]
            if not items_for_detail_view:
                st.info("No items to display based on the current filter (excluding filter conditions).")
            else:
                for i_detail, item_detail_data in enumerate(items_for_detail_view): 
                    expander_label_key = item_detail_data.get('item', item_detail_data.get('column', f"Item {i_detail+1}")) # Use 'item' first
                    with st.expander(f"Details for: {expander_label_key} (Type: {item_detail_data['type']})"):
                        # ... (rest of the detailed view logic from your original code)
                        st.write("**Type:** ", item_detail_data['type'])
                        pbi_eq_formula_detail = item_detail_data.get('final_expression', "") 
                        made_change_in_rule_based_translation_detail = False 
                        if item_detail_data['type'] == 'expression' and item_detail_data.get('final_expression'):
                            # ... (SQL expression display, PBI equivalent, AI DAX button and display) ...
                            formatted_expr_detail = sqlparse.format(item_detail_data['final_expression'], reindent=True, keyword_case='upper', indent_width=2)
                            st.write("**SQL Expression:**"); st.code(formatted_expr_detail, language="sql")
                            st.markdown("---"); st.write("**Power BI Equivalent Formula (Rule-Based Translation):**")
                            if st.session_state.get('column_mappings') and item_detail_data.get('base_columns'):
                                pbi_eq_formula_detail, made_change_in_rule_based_translation_detail = generate_powerbi_equivalent_formula(
                                    item_detail_data['final_expression'], item_detail_data.get('base_columns'), 
                                    st.session_state['column_mappings'], st.session_state.get('resolved_base_col_to_pbi', {}))
                                if made_change_in_rule_based_translation_detail: st.code(pbi_eq_formula_detail, language="dax") 
                                else: st.caption("Could not translate..."); pbi_eq_formula_detail = item_detail_data['final_expression'] 
                            # ... (other conditions for translation) ...
                            st.markdown("---"); item_id_detail = f"{expander_label_key}_{i_detail}" 
                            expression_for_ai_detail = pbi_eq_formula_detail if made_change_in_rule_based_translation_detail else item_detail_data.get('final_expression', '')
                            if st.button(f"Generate DAX with AI", key=f"dax_btn_{item_id_detail}_vis_revised"): 
                                if expression_for_ai_detail and expression_for_ai_detail.strip():
                                    with st.spinner("Generating DAX with AI..."):
                                        dax_results_detail = generate_dax_from_sql(expression_for_ai_detail)
                                        st.session_state['dax_expressions'][item_id_detail] = dax_results_detail
                                else: st.warning("Expression for AI is empty.")
                            if item_id_detail in st.session_state['dax_expressions']:
                                # ... (display AI DAX results) ...
                                dax_results_render = st.session_state['dax_expressions'][item_id_detail] 
                                recommendation_render = dax_results_render.get("recommendation", "").lower()
                                if recommendation_render == "measure": st.info("💡 **AI Recommendation:** **MEASURE**")
                                elif "calculated column" in recommendation_render: st.info("💡 **AI Recommendation:** **CALCULATED COLUMN**")
                                elif recommendation_render and recommendation_render != "error": st.info(f"💡 **AI Recommendation:** {recommendation_render.upper()}")
                                st.write("**AI Generated DAX Measure:**"); st.code(dax_results_render.get("measure", "Not provided or error."), language="dax")
                                st.write("**AI Generated DAX Calculated Column:**"); st.code(dax_results_render.get("calculated_column", "Not provided or error."), language="dax")
                                st.write("**AI Suggested Data Type (for Measure):**"); st.code(dax_results_render.get("dataType", "text"), language="text")
                        elif item_detail_data['type'] == 'expression': st.code("No expression available for this item.", language="text")
                        st.write("**Base columns (from SQL Lineage):**")
                        if item_detail_data.get('base_columns'):
                            for col_detail in item_detail_data['base_columns']: st.write(f"- `{col_detail}`")
                        else: st.write("N/A")
                        st.markdown("---"); st.write("**PBI Mapping for Individual Base Columns:**")
                        if not item_detail_data.get('base_columns'): st.caption("No base columns to show.")
                        elif not st.session_state.get('column_mappings'): st.warning("Mapping file not loaded.")
                        else:
                            for base_col_idx_detail, base_col_str_detail in enumerate(item_detail_data['base_columns']):
                                # ... (display PBI mapping for each base column) ...
                                norm_base_col_detail = normalize_column_identifier(base_col_str_detail)
                                st.markdown(f"  - **Base Column {base_col_idx_detail+1}:** `{base_col_str_detail}` <br>&nbsp;&nbsp;&nbsp;&nbsp;Normalized: `{norm_base_col_detail}`", unsafe_allow_html=True)
                                pbi_matches_for_this_base_col_detail = find_matching_powerbi_columns(base_col_str_detail, st.session_state['column_mappings'])
                                if pbi_matches_for_this_base_col_detail:
                                    for match_idx_detail, match_info_detail in enumerate(pbi_matches_for_this_base_col_detail):
                                        # ... (display match details) ...
                                        pbi_table_name_detail = match_info_detail.get('table', 'N/A'); pbi_col_name_detail = match_info_detail.get('column', 'N/A') 
                                        dax_ref_display_detail = f"'{pbi_table_name_detail}'[{pbi_col_name_detail}]" if pbi_table_name_detail != 'N/A' else "N/A"
                                        st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- PBI Target {match_idx_detail+1}: `{match_info_detail.get('powerbi_column', 'N/A')}` (DAX: `{dax_ref_display_detail}`)<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Table: `{pbi_table_name_detail}`<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Column: `{pbi_col_name_detail}`<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- (Source DB in Mapping: `{match_info_detail.get('db_column', 'N/A')}`)", unsafe_allow_html=True)
                                else: st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- *No PowerBI mapping found for this base column.*", unsafe_allow_html=True)
                                st.markdown("<br>", unsafe_allow_html=True) 

        with tab3:
            # ... (Content of PBI Mapping tab - this is extensive) ...
            st.header("Consolidated Power BI Column Mappings")
            if not st.session_state.get('column_mappings'): st.warning("Mapping file not loaded.")
            elif not st.session_state.get('mapping_results'): st.info("No SQL query analyzed yet.")
            else:
                # ... (rest of tab3 logic from your original code) ...
                mapping_filter_tab3 = st.radio("Show SQL Columns:", ["All", "Mapped Only", "Unmapped Only"], horizontal=True, key="pbi_mapping_tab_filter_tab3_revised")
                mapping_data_for_tab3 = {k: v for k, v in st.session_state['mapping_results'].items()}
                if not mapping_data_for_tab3: st.info("No mappable items found.")
                else:
                    # ... (metrics and expander logic for mappings) ...
                    total_sql_cols_tab3 = len(mapping_data_for_tab3); mapped_sql_cols_count_tab3 = sum(1 for data_tab3 in mapping_data_for_tab3.values() if data_tab3.get("is_mapped_overall")); unmapped_sql_cols_count_tab3 = total_sql_cols_tab3 - mapped_sql_cols_count_tab3
                    m_col1_tab3, m_col2_tab3, m_col3_tab3 = st.columns(3); m_col1_tab3.metric("Total SQL Items", total_sql_cols_tab3); m_col2_tab3.metric("Mapped", mapped_sql_cols_count_tab3); m_col3_tab3.metric("Unmapped", unmapped_sql_cols_count_tab3)
                    export_rows_tab3 = []
                    for sql_col_name_tab3, data_val_tab3 in mapping_data_for_tab3.items(): 
                        # ... (expander logic for each SQL item and its base column mappings) ...
                        is_overall_mapped_tab3 = data_val_tab3.get("is_mapped_overall", False); display_this_sql_col_tab3 = False
                        if mapping_filter_tab3 == "All": display_this_sql_col_tab3 = True
                        elif mapping_filter_tab3 == "Mapped Only" and is_overall_mapped_tab3: display_this_sql_col_tab3 = True
                        elif mapping_filter_tab3 == "Unmapped Only" and not is_overall_mapped_tab3: display_this_sql_col_tab3 = True
                        if display_this_sql_col_tab3:
                            expander_title_tab3 = f"SQL Item: {sql_col_name_tab3} (Type: {data_val_tab3.get('type', 'N/A')})"
                            expander_title_tab3 += " ✅ (Mapped)" if is_overall_mapped_tab3 else " ❌ (Unmapped)"
                            with st.expander(expander_title_tab3):
                                # ... (display base column mappings within expander) ...
                                pass # Placeholder for detailed mapping display
                    if export_rows_tab3: # Simplified, ensure export_rows_tab3 is populated correctly
                        export_df_tab3 = pd.DataFrame(export_rows_tab3)
                        csv_export_tab3 = export_df_tab3.to_csv(index=False).encode('utf-8')
                        st.download_button(label="Download All Mappings (CSV)", data=csv_export_tab3, file_name="pbi_column_mapping_details.csv", mime="text/csv", key="export_all_mappings_button_tab3_vis_revised" )

        with tab_filters:
            # ... (Content of Filter Conditions tab) ...
            st.header("WHERE Clause Filter Conditions Analysis")
            filter_conditions = [item for item in st.session_state['lineage_data'] if item.get('type') == 'filter_condition']
            if not filter_conditions: st.info("No WHERE clause conditions found.")
            else:
                for i, condition_data in enumerate(filter_conditions):
                    with st.expander(f"Condition {i+1}: {condition_data.get('item', 'Unknown Condition Context')}"):
                        st.write("**Source Clause:**", condition_data.get('source_clause', 'N/A'))
                        st.write("**Filter Condition SQL:**")
                        st.code(condition_data.get('filter_condition', 'N/A'), language="sql")

                        base_columns_in_filter = condition_data.get('base_columns', [])
                        st.write("**Base Columns Involved:**")
                        if not base_columns_in_filter: 
                            st.caption("No base columns identified for this filter.")
                        else:
                            for col_filter in base_columns_in_filter: 
                                st.write(f"- `{col_filter}`")
                        
                        st.markdown("---")
                        st.write("**Power BI Equivalent Filter DAX (Rule-Based Translation):**")
                        if st.session_state.get('column_mappings') and base_columns_in_filter and condition_data.get('filter_condition'):
                            pbi_eq_filter_dax, made_change_filter_dax = generate_powerbi_equivalent_formula(
                                condition_data['filter_condition'], 
                                base_columns_in_filter, 
                                st.session_state['column_mappings'],
                                st.session_state.get('resolved_base_col_to_pbi', {})
                            )
                            if made_change_filter_dax:
                                st.code(pbi_eq_filter_dax, language="dax")
                            else:
                                st.caption("Could not translate filter condition to DAX based on current mappings.")
                        elif not base_columns_in_filter:
                            st.caption("No base columns identified to attempt DAX translation.")
                        else:
                            st.caption("Translation prerequisites not met (e.g., mappings not loaded).")

                        st.markdown("---")
                        st.write("**PBI Mapping for Individual Base Columns in Filter:**")
                        if not base_columns_in_filter: 
                            st.caption("No base columns to show PBI mappings for.")
                        elif not st.session_state.get('column_mappings'): 
                            st.warning("Mapping file not loaded.")
                        else:
                            for base_col_idx_filter, base_col_str_filter in enumerate(base_columns_in_filter):
                                norm_base_col_filter = normalize_column_identifier(base_col_str_filter)
                                st.markdown(f"  - **Base Column {base_col_idx_filter+1}:** `{base_col_str_filter}` <br>&nbsp;&nbsp;&nbsp;&nbsp;Normalized: `{norm_base_col_filter}`", unsafe_allow_html=True)
                                pbi_matches_for_this_base_col_filter = find_matching_powerbi_columns(base_col_str_filter, st.session_state['column_mappings'])
                                if pbi_matches_for_this_base_col_filter:
                                    for match_idx_filter, match_info_filter in enumerate(pbi_matches_for_this_base_col_filter):
                                        pbi_table_name_filter = match_info_filter.get('table', 'N/A')
                                        pbi_col_name_filter = match_info_filter.get('column', 'N/A')
                                        dax_ref_display_filter = f"'{pbi_table_name_filter}'[{pbi_col_name_filter}]" if pbi_table_name_filter != 'N/A' else "N/A"
                                        st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- PBI Target {match_idx_filter+1}: `{match_info_filter.get('powerbi_column', 'N/A')}` (DAX: `{dax_ref_display_filter}`)<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Table: `{pbi_table_name_filter}`<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Column: `{pbi_col_name_filter}`<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- (Source DB in Mapping: `{match_info_filter.get('db_column', 'N/A')}`)", unsafe_allow_html=True)
                                else:
                                    st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- *No PowerBI mapping found for this base column.*", unsafe_allow_html=True)
                                st.markdown("<br>", unsafe_allow_html=True)
                        st.markdown("---")
        with tab4:
            st.header("Raw Lineage Data (JSON)")
            st.json(st.session_state['lineage_data'])




def display_filter_selection_ui():
    """Display filter selection UI and update session state."""
    st.markdown("##### Select Filters for Visual:")
    if not st.session_state.get('translated_filter_conditions'):
        raw_filters = [item for item in st.session_state.get('lineage_data', []) if item.get('type') == 'filter_condition']
        temp_translated_filters = []
        for i, f_item in enumerate(raw_filters):
            sql_expr = f_item.get('filter_condition')
            base_cols = f_item.get('base_columns', [])
            if sql_expr:
                pbi_dax, _ = generate_powerbi_equivalent_formula(
                    sql_expr, base_cols, 
                    st.session_state['column_mappings'], 
                    st.session_state['resolved_base_col_to_pbi']
                )
                temp_translated_filters.append({'id': f"filter_{i}_{hash(sql_expr)}", 'sql': sql_expr, 'pbi_dax': pbi_dax})
        st.session_state['translated_filter_conditions'] = temp_translated_filters
        st.session_state['visual_selected_filters_dax'] = [tf['pbi_dax'] for tf in temp_translated_filters if tf['pbi_dax']]

    if not st.session_state['translated_filter_conditions']:
        st.caption("No filter conditions found in the SQL query or they could not be translated.")
    else:
        current_selected_filters = list(st.session_state['visual_selected_filters_dax'])
        for filter_item in st.session_state['translated_filter_conditions']:
            pbi_dax = filter_item['pbi_dax']
            filter_id = filter_item['id']
            if not pbi_dax: continue
            is_checked = st.checkbox(
                f"{pbi_dax}", 
                value=(pbi_dax in current_selected_filters), 
                key=f"filter_cb_{filter_id}"
            )
            if is_checked and pbi_dax not in current_selected_filters:
                current_selected_filters.append(pbi_dax)
            elif not is_checked and pbi_dax in current_selected_filters:
                current_selected_filters.remove(pbi_dax)
        if st.session_state['visual_selected_filters_dax'] != current_selected_filters:
            st.session_state['visual_selected_filters_dax'] = current_selected_filters



def display_visual_configuration_section():
    """Handles the entire visual configuration UI and logic."""
    if st.session_state.get('lineage_data') and st.session_state.get('visual_config_candidates'):
        st.markdown("### Advanced: Resolve Base Database Column Ambiguities")
        if 'base_col_ambiguity_choices' not in st.session_state: st.session_state['base_col_ambiguity_choices'] = {}
        
        all_base_columns_for_ambiguity = set()
        for item in st.session_state['lineage_data']:
            # Include base columns from SELECT items AND filter_conditions
            for base_col in item.get('base_columns', []): 
                all_base_columns_for_ambiguity.add(base_col)
        
        base_col_to_matches = {}
        for base_col in all_base_columns_for_ambiguity:
            matches = find_matching_powerbi_columns(base_col, st.session_state['column_mappings'])
            if matches and len(matches) > 1: base_col_to_matches[base_col] = matches
        
        ambiguity_resolved_this_run = False
        if base_col_to_matches:
            st.caption("Some base database columns have multiple Power BI mapping candidates. Please select the correct one to use for DAX generation.")
            for base_col, matches in base_col_to_matches.items():
                options = [f"'{m['table']}'[{m['column']}]" for m in matches]
                current_choice_for_base_col = st.session_state['base_col_ambiguity_choices'].get(base_col)
                
                # Ensure current_choice is valid, default to first option if not
                if current_choice_for_base_col not in options:
                    current_choice_for_base_col = options[0] if options else None

                if options: # Only show radio if there are options
                    chosen = st.radio(
                        f"Choose PBI mapping for base DB column `{base_col}`:", 
                        options, 
                        index=options.index(current_choice_for_base_col) if current_choice_for_base_col in options else 0, 
                        key=f"base_col_ambiguity_{base_col.replace('.', '_').replace(' ', '_')}" # Make key more robust
                    )
                    if st.session_state['base_col_ambiguity_choices'].get(base_col) != chosen:
                        st.session_state['base_col_ambiguity_choices'][base_col] = chosen
                        ambiguity_resolved_this_run = True
        else:
            st.caption("No base column ambiguities found or all have single PBI mappings.")

        if ambiguity_resolved_this_run:
            st.session_state['visual_config_candidates'] = build_visual_candidates() # Rebuild with new resolutions
             # Re-translate filters as well
            st.session_state['translated_filter_conditions'] = [] # Clear to force re-translation
            st.rerun()

        resolved_base_col_to_pbi = {}
        for item_lineage in st.session_state['lineage_data']:
            for base_col_lineage in item_lineage.get('base_columns', []):
                if base_col_lineage not in resolved_base_col_to_pbi: # Process each base column once
                    matches_res = find_matching_powerbi_columns(base_col_lineage, st.session_state['column_mappings'])
                    resolved_label_res = st.session_state['base_col_ambiguity_choices'].get(base_col_lineage)
                    pbi_ref_res = None
                    if resolved_label_res and matches_res:
                        resolved_match = next((m_res for m_res in matches_res if f"'{m_res['table']}'[{m_res['column']}]" == resolved_label_res), None)
                        if resolved_match: pbi_ref_res = resolved_label_res
                    elif matches_res and len(matches_res) == 1: # Auto-select if only one match and no explicit choice needed/made
                        m_first = matches_res[0]; pbi_ref_res = f"'{m_first['table']}'[{m_first['column']}]"
                    elif matches_res: # Multiple matches but no choice made yet (e.g. first run), pick first as temp default
                         m_first = matches_res[0]; pbi_ref_res = f"'{m_first['table']}'[{m_first['column']}]"
                    
                    if pbi_ref_res: resolved_base_col_to_pbi[base_col_lineage] = pbi_ref_res
        st.session_state['resolved_base_col_to_pbi'] = resolved_base_col_to_pbi
        
        # Rebuild candidates if resolved_base_col_to_pbi changed significantly (e.g. first population)
        # This check might be too simple, but aims to refresh candidates once resolution is stable.
        if not st.session_state.get('visual_config_candidates_built_after_resolution', False) and resolved_base_col_to_pbi:
            st.session_state['visual_config_candidates'] = build_visual_candidates()
            st.session_state['visual_config_candidates_built_after_resolution'] = True # Mark as built
            st.session_state['translated_filter_conditions'] = [] # Clear to force re-translation with new candidates
            st.rerun()


        st.markdown("---")
        st.subheader("Visual Configuration")
        st.session_state['visual_type'] = st.radio(
            "Select Visual Type:", 
            ["Matrix", "Table"], 
            index=["Matrix", "Table"].index(st.session_state.get('visual_type', "Matrix")), 
            key="visual_type_selector"
        )
        
        all_available_display_labels_for_visual = sorted(list(set(
            c['chosen_display_label'] for c in st.session_state.get('visual_config_candidates', []) if c.get('chosen_display_label')
        )))

        if st.session_state['visual_type'] == "Matrix":
            st.markdown("#### Configure Matrix Visual")

            selected_rows = st.multiselect(
                "Select Rows for Matrix:",
                options=all_available_display_labels_for_visual,
                default=st.session_state.get('visual_selected_rows_labels', []),
                key="matrix_rows_multiselect"
            )
            selected_columns = st.multiselect(
                "Select Columns for Matrix:",
                options=all_available_display_labels_for_visual,
                default=st.session_state.get('visual_selected_columns_labels', []),
                key="matrix_cols_multiselect"
            )
            selected_values = st.multiselect(
                "Select Values for Matrix:",
                options=all_available_display_labels_for_visual,
                default=st.session_state.get('visual_selected_values_labels', []),
                key="matrix_values_multiselect"
            )

            display_filter_selection_ui()

            if st.button("Save Matrix Selection (including filters)"):
                st.session_state['visual_selected_rows'] = enrich_selected_items(selected_rows)
                st.session_state['visual_selected_columns'] = enrich_selected_items(selected_columns)
                st.session_state['visual_selected_values'] = enrich_selected_items(selected_values)
                st.session_state['visual_selected_rows_labels'] = selected_rows
                st.session_state['visual_selected_columns_labels'] = selected_columns
                st.session_state['visual_selected_values_labels'] = selected_values
                # Filter selections are already updated in session state by the checkboxes directly
                st.success("Matrix selection and filters saved!")
                st.rerun() # Rerun to reflect saved state or update dependent UI

            # Display current matrix configuration (your existing logic)
            st.markdown("##### Current Matrix Configuration:")
            if st.session_state.get('visual_selected_rows') or \
               st.session_state.get('visual_selected_columns') or \
               st.session_state.get('visual_selected_values')or \
               st.session_state.get('visual_selected_filters_dax'):
                
                st.write("**Rows:**")
                if st.session_state.get('visual_selected_rows'):
                    st.json(st.session_state['visual_selected_rows'])
                else: 
                    st.caption("  (None selected)")

                st.write("**Columns:**")
                if st.session_state.get('visual_selected_columns'):
                    st.json(st.session_state['visual_selected_columns'])
                else: 
                    st.caption("  (None selected)")

                st.write("**Values:**")
                if st.session_state.get('visual_selected_values'):
                    st.json(st.session_state['visual_selected_values'])
                else: 
                    st.caption("  (None selected)")

                st.write("**Filters:**")
                selected_filters_dax_strings = st.session_state.get('visual_selected_filters_dax', [])
                if selected_filters_dax_strings:
                    parsed_filters_for_display = []
                    for f_dax_str in selected_filters_dax_strings:
                        parsed_filter = parse_dax_filter_for_display(f_dax_str)
                        parsed_filters_for_display.append(parsed_filter)
                    st.json(parsed_filters_for_display)
                else:
                    st.caption("  (None selected)")
            else:
                st.caption("No items selected for the matrix.")
            
            st.markdown("---"); st.subheader("AI DAX Generation for Selected Matrix Items")
            items_to_process_for_ai = []
            for category, selected_list_key in [
                ("Row", 'visual_selected_rows'), 
                ("Column", 'visual_selected_columns'), 
                ("Value", 'visual_selected_values')]:
                if selected_list_key in st.session_state:
                    for item_dict_ai in st.session_state[selected_list_key]:
                        if item_dict_ai.get("type") == "expression" and item_dict_ai.get("pbi_expression"):
                            items_to_process_for_ai.append({
                                "label": item_dict_ai["label"],
                                "pbi_expression": item_dict_ai["pbi_expression"],
                                "category": category
                            })
            
            if not items_to_process_for_ai:
                st.caption("No expressions selected in Rows, Columns, or Values to generate DAX for.")
            
            if items_to_process_for_ai and st.button("Generate DAX with AI for Selected Matrix Items", key="ai_dax_matrix_btn_main"):
                with st.spinner(f"Generating DAX for {len(items_to_process_for_ai)} expression(s)..."):
                    for item_to_gen in items_to_process_for_ai:
                        label = item_to_gen["label"]
                        pbi_expr = item_to_gen["pbi_expression"]
                        category = item_to_gen["category"]
                        unique_key = f"{category}_{label}" # Consistent key

                        dax_results = generate_dax_from_sql(pbi_expr)
                        st.session_state['visual_ai_dax_results'][unique_key] = {
                            "label": label,
                            "input_pbi_expression": pbi_expr,
                            "ai_output": dax_results,
                            "category": category
                        }
                
                # Logic to update the main selected items with AI DAX (measure recommendation)
                overall_config_updated_by_ai = False
                for list_key_str_ai, category_name_str_ai in [
                    ('visual_selected_rows', "Row"),
                    ('visual_selected_columns', "Column"),
                    ('visual_selected_values', "Value")
                ]:
                    if list_key_str_ai in st.session_state:
                        current_list_in_state_ai = st.session_state[list_key_str_ai]
                        for item_dict_idx_ai in range(len(current_list_in_state_ai)):
                            item_dict_ai_update = current_list_in_state_ai[item_dict_idx_ai]
                            if item_dict_ai_update.get("type") == "expression":
                                ai_result_lookup_key = f"{category_name_str_ai}_{item_dict_ai_update['label']}"
                                had_previous_ai_dax = "ai_generated_dax" in item_dict_ai_update
                                current_item_modified_by_ai = False

                                if ai_result_lookup_key in st.session_state['visual_ai_dax_results']:
                                    ai_result_data = st.session_state['visual_ai_dax_results'][ai_result_lookup_key]
                                    ai_output_data = ai_result_data['ai_output']
                                    recommendation_data = ai_output_data.get("recommendation", "").lower()
                                    
                                    if "measure" in recommendation_data:
                                        measure_dax_from_ai = ai_output_data.get("measure")
                                        data_type_from_ai = ai_output_data.get("dataType", "text")
                                        if measure_dax_from_ai and not measure_dax_from_ai.startswith("Error:") and measure_dax_from_ai != "Not provided or error.":
                                            item_dict_ai_update["ai_generated_dax"] = measure_dax_from_ai
                                            item_dict_ai_update["ai_dataType"] = data_type_from_ai
                                            current_item_modified_by_ai = True
                                
                                if not current_item_modified_by_ai and had_previous_ai_dax:
                                    if "ai_generated_dax" in item_dict_ai_update: del item_dict_ai_update["ai_generated_dax"]
                                    if "ai_dataType" in item_dict_ai_update: del item_dict_ai_update["ai_dataType"]
                                    current_item_modified_by_ai = True
                                
                                if current_item_modified_by_ai:
                                    overall_config_updated_by_ai = True
                
                st.success(f"AI DAX generation complete for {len(st.session_state['visual_ai_dax_results'])} items.")
                if overall_config_updated_by_ai:
                    st.rerun() # Rerun to reflect the ai_generated_dax in the config file generation

            if st.session_state.get('visual_ai_dax_results'):
                st.markdown("##### DAX Generation Results (for selected expressions):") # Changed
                for unique_key_disp, result_data_disp in st.session_state['visual_ai_dax_results'].items():
                    with st.expander(f"{result_data_disp['label']}"): # Changed
                        st.write(f"**Input PBI Expression (Rule-Based):**")
                        st.code(result_data_disp.get('input_pbi_expression', 'N/A'), language="dax")
                        
                        ai_output_disp = result_data_disp.get('ai_output', {}) # Internal variable name can remain
                        recommendation_disp = ai_output_disp.get("recommendation", "").lower()
                        
                        if "measure" in recommendation_disp: # Prioritize if "measure" is in the recommendation string
                            st.info("💡 **Recommendation:** **MEASURE**") # Changed
                            st.write("**Generated DAX Measure:**") # Changed
                            st.code(ai_output_disp.get("measure", "Not provided or error."), language="dax")
                            st.write("**Suggested Data Type (for Measure):**") # Changed
                            st.code(ai_output_disp.get("dataType", "text"), language="text")
                        elif "calculated column" in recommendation_disp:
                            st.info("💡 **Recommendation:** **CALCULATED COLUMN**") # Changed
                            st.warning("Calculated Column is not directly used for visual measures. The generated DAX is for reference.") # Changed
                            st.write("**Generated DAX Calculated Column (for reference):**") # Changed
                            st.code(ai_output_disp.get("calculated_column", "Not provided or error."), language="dax")
                        elif recommendation_disp and recommendation_disp != "error": # Other valid recommendations
                            st.info(f"💡 **Recommendation:** {recommendation_disp.upper()}") # Changed
                            st.write("**Generated DAX Measure:**") # Changed
                            st.code(ai_output_disp.get("measure", "Not provided or error."), language="dax")
                            st.write("**Generated DAX Calculated Column:**") # Changed
                            st.code(ai_output_disp.get("calculated_column", "Not provided or error."), language="dax")
                            st.write("**Suggested Data Type (for Measure):**") # Changed
                            st.code(ai_output_disp.get("dataType", "text"), language="text")
                        else: # Error or unknown recommendation
                            st.error(f"Recommendation: {recommendation_disp if recommendation_disp else 'Not available'}") # Changed
                            st.write("**Generated DAX Measure (Attempt):**") # Changed
                            st.code(ai_output_disp.get("measure", "Not provided or error."), language="dax")
                            st.write("**Generated DAX Calculated Column (Attempt):**") # Changed
                            st.code(ai_output_disp.get("calculated_column", "Not provided or error."), language="dax")
                            st.write("**Suggested Data Type (Attempt):**") # Changed
                            st.code(ai_output_disp.get("dataType", "text"), language="text")

        elif st.session_state['visual_type'] == "Table":
            st.markdown("#### Configure Table Visual")

            # Show all available columns (base and expression) for selection
            table_column_labels = [
                c['chosen_display_label']
                for c in st.session_state.get('visual_config_candidates', [])
                if c.get('chosen_display_label')
            ]
            selected_table_fields = st.multiselect(
                "Select Columns/Expressions for Table:",
                options=table_column_labels,
                default=st.session_state.get('visual_selected_table_fields_labels', []),
                key="table_fields_multiselect"
            )


            display_filter_selection_ui()
            # Save selection
            if st.button("Save Table Selection (including filters)"):
                st.session_state['visual_selected_table_fields'] = enrich_selected_items(selected_table_fields)
                st.session_state['visual_selected_table_fields_labels'] = selected_table_fields
                st.success("Table selection and filters saved!")
                st.rerun()

            # Show current config
            st.markdown("##### Current Table Configuration:")
            if st.session_state.get('visual_selected_table_fields') or st.session_state.get('visual_selected_filters_dax'):
                st.write("**Fields:**")
                if st.session_state.get('visual_selected_table_fields'):
                    st.json(st.session_state['visual_selected_table_fields'])
                else:
                    st.caption("  (None selected)")
                st.write("**Filters:**")
                selected_filters_dax_strings = st.session_state.get('visual_selected_filters_dax', [])
                if selected_filters_dax_strings:
                    parsed_filters_for_display = []
                    for f_dax_str in selected_filters_dax_strings:
                        parsed_filter = parse_dax_filter_for_display(f_dax_str)
                        parsed_filters_for_display.append(parsed_filter)
                    st.json(parsed_filters_for_display)
                else:
                    st.caption("  (None selected)")
            else:
                st.caption("No items selected for the table.")

            # --- AI DAX generation for selected expressions (replicate Matrix logic) ---
            st.markdown("---"); st.subheader("AI DAX Generation for Selected Table Items")
            items_to_process_for_ai = []
            for item_dict_ai in st.session_state.get('visual_selected_table_fields', []):
                if item_dict_ai.get("type") == "expression" and item_dict_ai.get("pbi_expression"):
                    items_to_process_for_ai.append({
                        "label": item_dict_ai["label"],
                        "pbi_expression": item_dict_ai["pbi_expression"],
                        "category": "TableField"
                    })
            if not items_to_process_for_ai:
                st.caption("No expressions selected in Table fields to generate DAX for.")
            if items_to_process_for_ai and st.button("Generate DAX with AI for Selected Table Items", key="ai_dax_table_btn_main"):
                with st.spinner(f"Generating DAX for {len(items_to_process_for_ai)} expression(s)..."):
                    for item_to_gen in items_to_process_for_ai:
                        label = item_to_gen["label"]
                        pbi_expr = item_to_gen["pbi_expression"]
                        category = item_to_gen["category"]
                        unique_key = f"{category}_{label}"
                        dax_results = generate_dax_from_sql(pbi_expr)
                        st.session_state['visual_ai_dax_results'][unique_key] = {
                            "label": label,
                            "input_pbi_expression": pbi_expr,
                            "ai_output": dax_results,
                            "category": category
                        }

                # Update the selected table fields with AI DAX (measure recommendation)
                overall_config_updated_by_ai = False
                current_list_in_state_ai = st.session_state['visual_selected_table_fields']
                for item_dict_idx_ai in range(len(current_list_in_state_ai)):
                    item_dict_ai_update = current_list_in_state_ai[item_dict_idx_ai]
                    if item_dict_ai_update.get("type") == "expression":
                        ai_result_lookup_key = f"TableField_{item_dict_ai_update['label']}"
                        had_previous_ai_dax = "ai_generated_dax" in item_dict_ai_update
                        current_item_modified_by_ai = False

                        if ai_result_lookup_key in st.session_state['visual_ai_dax_results']:
                            ai_result_data = st.session_state['visual_ai_dax_results'][ai_result_lookup_key]
                            ai_output_data = ai_result_data['ai_output']
                            recommendation_data = ai_output_data.get("recommendation", "").lower()
                            if "measure" in recommendation_data:
                                measure_dax_from_ai = ai_output_data.get("measure")
                                data_type_from_ai = ai_output_data.get("dataType", "text")
                                if measure_dax_from_ai and not measure_dax_from_ai.startswith("Error:") and measure_dax_from_ai != "Not provided or error.":
                                    item_dict_ai_update["ai_generated_dax"] = measure_dax_from_ai
                                    item_dict_ai_update["ai_dataType"] = data_type_from_ai
                                    current_item_modified_by_ai = True

                        if not current_item_modified_by_ai and had_previous_ai_dax:
                            if "ai_generated_dax" in item_dict_ai_update: del item_dict_ai_update["ai_generated_dax"]
                            if "ai_dataType" in item_dict_ai_update: del item_dict_ai_update["ai_dataType"]
                            current_item_modified_by_ai = True

                        if current_item_modified_by_ai:
                            overall_config_updated_by_ai = True

                st.success(f"AI DAX generation complete for {len(st.session_state['visual_ai_dax_results'])} items.")
                if overall_config_updated_by_ai:
                    st.rerun() # Rerun to reflect the ai_generated_dax in the config file generation

            # Show DAX Generation Results for Table
            if st.session_state.get('visual_ai_dax_results'):
                st.markdown("##### DAX Generation Results (for selected expressions):")
                for unique_key_disp, result_data_disp in st.session_state['visual_ai_dax_results'].items():
                    if result_data_disp.get("category") != "TableField":
                        continue
                    with st.expander(f"{result_data_disp['label']}"):
                        st.write(f"**Input PBI Expression (Rule-Based):**")
                        st.code(result_data_disp.get('input_pbi_expression', 'N/A'), language="dax")
                        ai_output_disp = result_data_disp.get('ai_output', {})
                        recommendation_disp = ai_output_disp.get("recommendation", "").lower()
                        if "measure" in recommendation_disp:
                            st.info("💡 **Recommendation:** **MEASURE**")
                            st.write("**Generated DAX Measure:**")
                            st.code(ai_output_disp.get("measure", "Not provided or error."), language="dax")
                            st.write("**Suggested Data Type (for Measure):**")
                            st.code(ai_output_disp.get("dataType", "text"), language="text")
                        elif "calculated column" in recommendation_disp:
                            st.info("💡 **Recommendation:** **CALCULATED COLUMN**")
                            st.warning("Calculated Column is not directly used for visual measures. The generated DAX is for reference.")
                            st.write("**Generated DAX Calculated Column (for reference):**")
                            st.code(ai_output_disp.get("calculated_column", "Not provided or error."), language="dax")
                        elif recommendation_disp and recommendation_disp != "error":
                            st.info(f"💡 **Recommendation:** {recommendation_disp.upper()}")
                            st.write("**Generated DAX Measure:**")
                            st.code(ai_output_disp.get("measure", "Not provided or error."), language="dax")
                            st.write("**Generated DAX Calculated Column:**")
                            st.code(ai_output_disp.get("calculated_column", "Not provided or error."), language="dax")
                            st.write("**Suggested Data Type (for Measure):**")
                            st.code(ai_output_disp.get("dataType", "text"), language="text")
                        else:
                            st.error(f"Recommendation: {recommendation_disp if recommendation_disp else 'Not available'}")
                            st.write("**Generated DAX Measure (Attempt):**")
                            st.code(ai_output_disp.get("measure", "Not provided or error."), language="dax")
                            st.write("**Generated DAX Calculated Column (Attempt):**")
                            st.code(ai_output_disp.get("calculated_column", "Not provided or error."), language="dax")
                            st.write("**Suggested Data Type (Attempt):**")
                            st.code(ai_output_disp.get("dataType", "text"), language="text")



def parse_simple_dax_filter(dax_expression_str, generated_measures):
    """
    Attempts to parse simple DAX filter expressions into a structure
    compatible with the PBI Automation config.
    Returns a dict for the filter config, or None if parsing fails.
    """
    # Pattern 1: 'Table'[Column] = "StringValue" or 'StringValue'
    match_eq_str = re.fullmatch(r"^\s*'([^']+)'\[([^']+)\]\s*=\s*(?:\"([^\"]*)\"|'([^']*)')\s*$", dax_expression_str)
    if match_eq_str:
        table, column, value_double_quoted, value_single_quoted = match_eq_str.groups()
        value = value_double_quoted if value_double_quoted is not None else value_single_quoted
        return {"field": FlowDict({"name": column, "table": table, "type": "column"}), "filterType": "Categorical", "values": [value]}

    # Pattern 2: 'Table'[Column] = NumericValue
    match_eq_num = re.fullmatch(r"^\s*'([^']+)'\[([^']+)\]\s*=\s*([0-9\.]+)\s*$", dax_expression_str)
    if match_eq_num:
        table, column, value_str = match_eq_num.groups()
        try:
            value = float(value_str) if '.' in value_str else int(value_str)
            return {"field": FlowDict({"name": column, "table": table, "type": "column"}), "filterType": "Categorical", "values": [value]}
        except ValueError:
            return None

    # Pattern 3: 'Table'[Column] = TRUE/FALSE (case insensitive for TRUE/FALSE)
    match_eq_bool = re.fullmatch(r"^\s*'([^']+)'\[([^']+)\]\s*=\s*(TRUE|FALSE)\s*$", dax_expression_str, re.IGNORECASE)
    if match_eq_bool:
        table, column, bool_val_str = match_eq_bool.groups()
        return {"field": FlowDict({"name": column, "table": table, "type": "column"}), "filterType": "Categorical", "values": [bool_val_str.lower() == 'true']}

    # Pattern 4: 'Table'[Column] IN ('Val1', 'Val2', ...) or (1, 2, ...) or ("Val1", "Val2", ...)
    # This pattern now handles parentheses and mixed quoting for values.
    match_in = re.fullmatch(r"^\s*'([^']+)'\[([^']+)\]\s+IN\s+\(\s*([^)]+)\s*\)\s*$", dax_expression_str, re.IGNORECASE)
    if match_in:
        table, column, values_str_group = match_in.groups()
        # Split values, then strip quotes/convert type for each
        # This regex splits by comma, but respects quotes around values
        values_list_items = re.findall(r"(?:\"[^\"]*\"|'[^']*'|[^,]+)+", values_str_group)
        
        parsed_values_in = []
        for v_item_str in values_list_items:
            v_item_str = v_item_str.strip()
            if (v_item_str.startswith("'") and v_item_str.endswith("'")) or \
               (v_item_str.startswith('"') and v_item_str.endswith('"')):
                parsed_values_in.append(v_item_str[1:-1])  # Remove outer quotes
            else:
                try:
                    # Attempt to convert to number if no quotes
                    parsed_values_in.append(float(v_item_str) if '.' in v_item_str else int(v_item_str))
                except ValueError:
                    # If it's not a quoted string and not a number, it might be an unquoted string literal
                    # or a more complex scenario. For simplicity, if it's not a number,
                    # and wasn't quoted, we might decide to treat it as a string or flag an error.
                    # Given the examples, unquoted items are numbers.
                    # If it's truly an unquoted string that DAX allows in some contexts,
                    # this might need adjustment or the DAX generator should quote them.
                    # For now, if it's not a number after failing quote checks, it's problematic for this simple parser.
                    # However, the regex re.findall should capture quoted strings correctly.
                    # This path (erroring here) is less likely if input is like ('val1', 2, 'val3')
                    # Let's assume for now that unquoted = number, quoted = string.
                    # If an unquoted item is not a number, the DAX is likely malformed for simple parsing.
                    return None # Could not parse a value within IN clause cleanly
        
        if parsed_values_in:
            return {"field": FlowDict({"name": column, "table": table, "type": "column"}), "filterType": "Categorical", "values": parsed_values_in}


    # Pattern 5: '[Measure Name]' or 'Table'[Boolean Column] (implies = TRUE)
    # Check if it's a known measure first
    for measure_details in generated_measures:
        measure_name_candidate = dax_expression_str.strip()
        # Handle if measure name in DAX has brackets or not
        if (measure_name_candidate == measure_details["name"]) or \
           (measure_name_candidate == f"[{measure_details['name']}]"):
            if measure_details.get("dataType", "").lower() == "true/false":
                return {"field": FlowDict({"name": measure_details["name"], "table": measure_details["table"], "type": "measure"}), "filterType": "Advanced", "condition": "IsTrue"}
            # If it's a measure but not boolean, it's not a simple TRUE/FALSE filter by name alone.
            # This pattern is specifically for boolean measures/columns used as implicit TRUE filters.

    # Check for simple boolean column form: 'Table'[ColumnName] (implies = TRUE)
    match_bool_col = re.fullmatch(r"^\s*'([^']+)'\[([^']+)\]\s*$", dax_expression_str)
    if match_bool_col:
        table, column = match_bool_col.groups()
        # This assumes it's a boolean column used as a filter (evaluates to TRUE)
        # This might need more context to confirm it's boolean, but it's a common pattern.
        return {"field": FlowDict({"name": column, "table": table, "type": "column"}), "filterType": "Advanced", "condition": "IsTrue"}

    return None # Parsing failed for known simple patterns
def display_pbi_automation_config_section():
    """Handles the PBI Automation config.yaml generation and script execution."""
    st.markdown("---")
    st.header("PBI Automation `config.yaml` Generation")

    if st.button("Generate PBI Automation Config File"):
        try:
            new_config = {}
            # --- Hardcoded Static Fields ---
            new_config['projectName'] = "1.1.10.117. Daily Report Renault"
            new_config['dataset'] = { 
                "connection": { 
                    "connectionString": 'Data Source=powerbi://api.powerbi.com/v1.0/myorg/EMEA Development;Initial Catalog="EU Order to Cash (Ad-hoc)";Access Mode=readonly;Integrated Security=ClaimsToken',
                    "database": "7f97f9b2-2c89-4359-966b-4612b960fbb1" 
                }, 
                "modelName": "EU Order to Cash (Ad-Hoc)"
            }
            new_config['report'] = { 
                'title': FlowDict({"text": "1.1.10.117. Daily Report Renault"}), 
                'data_refresh': FlowDict({"table": "Date Refresh Table", "column": "UPDATED_DATE"})
            }

            # --- Generate Measures (Dynamic) ---
            generated_measures = []
            measure_candidate_lists = [
                st.session_state.get('visual_selected_rows', []),
                st.session_state.get('visual_selected_columns', []),
                st.session_state.get('visual_selected_values', []),
                st.session_state.get('visual_selected_table_fields', []) 
            ]
            processed_measure_labels = set() 
            for item_list in measure_candidate_lists:
                for item in item_list:
                    if item.get("type") == "expression" and item.get("label") not in processed_measure_labels:
                        base_measure_name = item["label"]
                        measure_name_for_definition = base_measure_name
                        if not base_measure_name.endswith(" Measure"):
                            measure_name_for_definition = f"{base_measure_name} Measure"
                        dax_expression = item.get("pbi_expression") 
                        data_type = "text" 
                        if "ai_generated_dax" in item and item.get("ai_generated_dax"):
                            dax_expression = item["ai_generated_dax"]
                            data_type = item.get("ai_dataType", "text")
                        measure_table = item.get("pbi_table", "_Measures")
                        generated_measures.append(FlowDict({
                            "name": measure_name_for_definition,
                            "table": measure_table, 
                            "expression": dax_expression,
                            "dataType": data_type
                        }))
                        processed_measure_labels.add(base_measure_name)
            new_config['report']['measures'] = generated_measures

            visuals = []

            # --- Matrix Visual ---
            if st.session_state.get('visual_type', 'Matrix') == "Matrix":
                # ...existing matrix config code...
                matrix_rows_config = []
                for item in st.session_state.get('visual_selected_rows', []):
                    row_item_config = {"name": item["label"], "table": item.get("pbi_table", "UnknownTable"), "type": "Column"}
                    if item.get("type") == "base" and item.get("pbi_table") and item.get("pbi_column"):
                        row_item_config["name"] = item["pbi_column"]
                        row_item_config["table"] = item["pbi_table"]
                    elif item.get("type") == "expression":
                        row_item_config["name"] = item["label"]
                        row_item_config["table"] = item.get("pbi_table", "_Measures")
                    matrix_rows_config.append(FlowDict(row_item_config))

                matrix_columns_config = []
                for item in st.session_state.get('visual_selected_columns', []):
                    column_item_config = {"name": item["label"], "table": item.get("pbi_table", "UnknownTable"), "type": "Column"}
                    if item.get("type") == "base" and item.get("pbi_table") and item.get("pbi_column"):
                        column_item_config["name"] = item["pbi_column"]
                        column_item_config["table"] = item["pbi_table"]
                    elif item.get("type") == "expression":
                        column_item_config["name"] = item["label"]
                        column_item_config["table"] = item.get("pbi_table", "_Measures")
                    matrix_columns_config.append(FlowDict(column_item_config))

                matrix_values_config = []
                for item in st.session_state.get('visual_selected_values', []):
                    if item.get("type") == "expression":
                        base_value_name = item["label"]
                        value_name_for_visual = base_value_name
                        if not base_value_name.endswith(" Measure"):
                            value_name_for_visual = f"{base_value_name} Measure"
                        measure_table_ref = item.get("pbi_table", "_Measures")
                        defined_measure = next((m for m in generated_measures if m["name"] == value_name_for_visual), None)
                        if defined_measure:
                            measure_table_ref = defined_measure["table"]
                        matrix_values_config.append(FlowDict({
                            "name": value_name_for_visual, 
                            "table": measure_table_ref, 
                            "type": "Measure"
                        }))

                # --- Generate Filters (Dynamic) ---
                matrix_filters_config = []
                selected_filter_dax_expressions = st.session_state.get('visual_selected_filters_dax', [])
                for pbi_dax_filter_str in selected_filter_dax_expressions:
                    parsed_filter_structure = parse_simple_dax_filter(pbi_dax_filter_str, generated_measures)
                    if parsed_filter_structure:
                        matrix_filters_config.append(FlowDict(parsed_filter_structure))
                    else:
                        st.warning(f"Could not parse filter DAX: '{pbi_dax_filter_str}'. This filter will be skipped in config.yaml. Consider simplifying the DAX or extending parsing capabilities if this filter is required.")

                matrix_visual_definition = {
                    "type": "matrix",
                    "position": FlowDict({"x": 28.8, "y": 100, "width": 1220, "height": 800}),
                    "rows": matrix_rows_config,
                    "columns": matrix_columns_config,
                    "values": matrix_values_config,
                    "filters": matrix_filters_config
                }
                visuals.append(matrix_visual_definition)

            # --- Table Visual ---
            elif st.session_state.get('visual_type') == "Table":
                table_fields_config = []
                for item in st.session_state.get('visual_selected_table_fields', []):
                    field_item_config = {"name": item["label"], "table": item.get("pbi_table", "UnknownTable"), "type": "Column"}
                    if item.get("type") == "base" and item.get("pbi_table") and item.get("pbi_column"):
                        field_item_config["name"] = item["pbi_column"]
                        field_item_config["table"] = item["pbi_table"]
                        field_item_config["type"] = "Column"
                    elif item.get("type") == "expression":
                        base_value_name = item["label"]
                        value_name_for_visual = base_value_name
                        if not base_value_name.endswith(" Measure"):
                            value_name_for_visual = f"{base_value_name} Measure"
                        measure_table_ref = item.get("pbi_table", "_Measures")
                        defined_measure = next((m for m in generated_measures if m["name"] == value_name_for_visual), None)
                        if defined_measure:
                            measure_table_ref = defined_measure["table"]
                        field_item_config["name"] = value_name_for_visual
                        field_item_config["table"] = measure_table_ref
                        field_item_config["type"] = "Measure"
                    table_fields_config.append(FlowDict(field_item_config))

                table_filters_config = []
                selected_filter_dax_expressions = st.session_state.get('visual_selected_filters_dax', [])
                for pbi_dax_filter_str in selected_filter_dax_expressions:
                    parsed_filter_structure = parse_simple_dax_filter(pbi_dax_filter_str, generated_measures)
                    if parsed_filter_structure:
                        table_filters_config.append(FlowDict(parsed_filter_structure))
                    else:
                        st.warning(f"Could not parse filter DAX: '{pbi_dax_filter_str}'. This filter will be skipped in config.yaml for the table visual.")

                table_visual_definition = {
                    "type": "table",
                    "position": FlowDict({"x": 28.8, "y": 100, "width": 1220, "height": 800}),
                    "fields": table_fields_config,
                    "filters": table_filters_config
                }
                visuals.append(table_visual_definition)

            new_config['report']['visuals'] = visuals


            yaml_string_io = StringIO()
            yaml.dump(new_config, yaml_string_io, Dumper=CustomDumper, sort_keys=False, indent=2, allow_unicode=True)
            generated_yaml_str = yaml_string_io.getvalue()
            st.session_state['generated_pbi_config'] = generated_yaml_str.strip()
            st.success("PBI Automation config.yaml content generated successfully!")

            # --- Save config locally and run PBI Automation ---
            local_config_filename = "config.yaml"
            app_dir = Path(__file__).parent
            local_config_path = app_dir / local_config_filename # This is in the Streamlit app's directory
            with open(local_config_path, 'w', encoding='utf-8') as f:
                f.write(st.session_state['generated_pbi_config'])
            st.info(f"Generated `config.yaml` saved to: {local_config_path}") # Updated message
            
            # --- PBI Automation script execution logic (Placeholder) ---
            # This assumes your PBI Automation script is in a 'PBI Automation' directory
            # relative to this script's location, and it's called 'main.py'.
            # Adjust the path and command as necessary.
            pbi_automation_script_path = Path(r"C:\Users\NileshPhapale\Desktop\PBI Automation\main.py")
            pbi_automation_project_dir = Path(r"C:\Users\NileshPhapale\Desktop\PBI Automation") # Still needed for cwd
            python_executable = r"C:\Users\NileshPhapale\Desktop\PBI Automation\.venv\Scripts\python.exe" # Specific python executable
            
            if pbi_automation_script_path.exists():
                st.info(f"Attempting to run PBI Automation script: {pbi_automation_script_path}")
                
                try:
                    # Construct the command
                    command = [
                        python_executable, 
                        str(pbi_automation_script_path),
                        "--config", 
                        str(local_config_path.resolve()) # Pass absolute path to the config file
                    ]
                    st.info(f"Executing command: {' '.join(command)}") # Log the command being run

                    process = subprocess.Popen(
                        command, 
                        cwd=str(pbi_automation_project_dir), # Script still runs from its own directory
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        encoding='utf-8' 
                    )
                    stdout, stderr = process.communicate(timeout=300) 

                    if process.returncode == 0:
                        st.success("PBI Automation script executed successfully!")
                        if stdout: st.text_area("Script Output:", value=stdout, height=200)
                        if stderr: st.text_area("Script Error Output (if any):", value=stderr, height=100) # Show stderr even on success
                    else:
                        st.error(f"PBI Automation script execution failed with code {process.returncode}.")
                        if stdout: st.text_area("Script Output:", value=stdout, height=150)
                        if stderr: st.text_area("Script Error Output:", value=stderr, height=150)
                except subprocess.TimeoutExpired:
                    st.error("PBI Automation script timed out.")
                except FileNotFoundError:
                    st.error(f"Python executable not found at '{python_executable}'. Please ensure the path is correct.")
                except Exception as sub_e:
                    st.error(f"Error running PBI Automation script: {sub_e}")
                    st.exception(sub_e)
            else:
                st.warning(f"PBI Automation script not found at: {pbi_automation_script_path}. Skipping execution.")

        except Exception as e:
            st.error(f"An unexpected error occurred during config generation or script execution: {e}")
            st.exception(e) 
    
    if st.session_state.get('generated_pbi_config'):
        st.subheader("Generated `config.yaml` Content (for review)")
        st.code(st.session_state['generated_pbi_config'], language="yaml")
        st.download_button(label="Download Generated config.yaml", data=st.session_state['generated_pbi_config'], file_name="generated_config.yaml", mime="text/yaml")




def main():
    st.set_page_config(page_title="SQL to Power BI Mapper", page_icon="📊", layout="wide")
    
    initialize_session_state()
    
    st.title("SQL to Power BI Column Mapper & Visual Configurator") # Updated title
    st.markdown("""
    This tool analyzes SQL queries, maps columns to Power BI, helps configure visuals, 
    and generates configuration for PBI Automation.
    """)

    display_sidebar()
    sql_query, analyze_button_pressed = display_query_input_area()

    if analyze_button_pressed and sql_query.strip():
        perform_sql_analysis(sql_query)
        # After analysis, mapping_results and visual_config_candidates are populated
        # No explicit call to build_visual_candidates here as perform_sql_analysis handles it.

    display_analysis_results_tabs()
    display_visual_configuration_section() # This will handle its own conditions for display
    display_pbi_automation_config_section() # This will handle its own conditions for display



if __name__ == "__main__":
    main()