import streamlit as st
import google.generativeai as genai
import re
import json

from src.constants import API_KEY
from src.mapping import find_matching_powerbi_columns
from src.utils import FlowDict

genai.configure(api_key=API_KEY)

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

        # print(response)

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
