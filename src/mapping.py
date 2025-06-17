from src.constants import MAPPING_FILE_PATH
import json


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
