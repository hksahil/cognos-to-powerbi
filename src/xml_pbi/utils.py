import json
import re
import streamlit as st
import yaml



# --- YAML HELPER CLASSES ---
class FlowDict(dict):
    pass

class CustomDumper(yaml.Dumper):
    def represent_data(self, data):
        if isinstance(data, FlowDict):
            return self.represent_dict(data)
        return super().represent_data(data)

CustomDumper.add_representer(FlowDict, CustomDumper.represent_dict)



def load_all_mappings(filepath="column_mappings.json"):
    """Loads the entire mappings JSON file."""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error(f"Mapping file not found at {filepath}. Please ensure it's in the root directory.")
        return None
    except json.JSONDecodeError:
        st.error(f"Error decoding JSON from {filepath}. Please check the file for syntax errors.")
        return None


def parse_pbi_string(pbi_string):
    """Parses a Power BI string like ''Table'[Column]' into its components."""
    if not pbi_string:
        return None, None
    # Use strip() to handle potential leading/trailing whitespace
    match = re.match(r"'(.*?)'\[(.*?)\]", pbi_string.strip())
    if match:
        # Strip whitespace from captured groups as well
        table = match.group(1).strip()
        column = match.group(2).strip()
        return table, column
    return None, None

def parse_filter_expression(expression):
    """
    Parses a Cognos filter expression to extract values for 'in' or '=' clauses.
    Returns a list of values.
    """
    if not expression:
        return []

    # Try to match 'in ('val1'; 'val2')' - handles single quotes and optional spaces
    in_match = re.search(r'in\s*\((.*?)\)', expression, re.IGNORECASE)
    if in_match:
        values_str = in_match.group(1)
        # Split by comma or semicolon, then strip whitespace and quotes
        values = [val.strip().strip("'\"") for val in re.split(r'[,;]', values_str)]
        return values

    # Try to match '= ('val')'
    equals_in_parens_match = re.search(r'=\s*\(\s*\'(.*?)\'\s*\)', expression)
    if equals_in_parens_match:
        return [equals_in_parens_match.group(1)] # Return the single value in a list

    # Try to match '= 'val''
    equals_match = re.search(r'=\s*\'(.*?)\'', expression)
    if equals_match:
        return [equals_match.group(1)] # Return the single value in a list
    
    return []
