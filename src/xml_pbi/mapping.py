import streamlit as st
import re

def map_cognos_to_db(report_data, cognos_db_map):
    """
    Enriches the report data with database column mappings by iterating through
    visuals and their columns to find database equivalents.
    """
    if not cognos_db_map:
        st.warning("Cognos to DB mapping data is empty. Cannot map columns.")
        return report_data

    def create_lookup_key(expression):
        """
        Normalizes a Cognos expression to create a consistent lookup key.
        Example: '[Presentation Layer].[Brand].[Brand Label]' -> 'presentation layer.brand.brand label'
        """
        if not isinstance(expression, str):
            return None
        parts = re.findall(r'\[(.*?)\]', expression)
        if len(parts) >= 2:
            cleaned_parts = [part.replace('"', '').strip() for part in parts]
            return ".".join(cleaned_parts).lower()
        return None

    for page in report_data.get('pages', []):
        for visual in page.get('visuals', []):
            for column_type in ['rows', 'columns']:
                for item in visual.get(column_type, []):
                    lookup_key = create_lookup_key(item.get('expression'))
                    item['db_mapping'] = cognos_db_map.get(lookup_key, 'N/A')
            
            for f in visual.get('filters', []):
                lookup_key = create_lookup_key(f.get('column'))
                f['db_mapping'] = cognos_db_map.get(lookup_key, 'N/A')

    return report_data

def find_pbi_mappings(mapped_data, db_to_pbi_map):
    """Finds Power BI mappings for all unique DB columns and their associated Cognos display items."""
    if not db_to_pbi_map:
        return [] # Return an empty list

    db_column_details = {}

    # Collect all unique DB columns and their associated Cognos display items
    for page in mapped_data.get('pages', []):
        for visual in page.get('visuals', []):
            # Process rows and columns: use the full expression as the display item
            for item in visual.get('rows', []) + visual.get('columns', []):
                db_map = item.get('db_mapping')
                expression = item.get('expression')
                if db_map and db_map != 'N/A' and expression:
                    if db_map not in db_column_details:
                        db_column_details[db_map] = {'display_items': set()}
                    db_column_details[db_map]['display_items'].add(expression)
            
            # Process filters: use the column name as the display item
            for f in visual.get('filters', []):
                db_map = f.get('db_mapping')
                column_name = f.get('column')
                if db_map and db_map != 'N/A' and column_name:
                    if db_map not in db_column_details:
                        db_column_details[db_map] = {'display_items': set()}
                    db_column_details[db_map]['display_items'].add(column_name)
    
    # Build the final result structure with the 'display_items' key
    pbi_mappings_result = []
    for db_col, details in sorted(db_column_details.items()):
        pbi_maps = db_to_pbi_map.get(db_col, [])
        pbi_mappings_result.append({
            "db_column": db_col,
            "display_items": sorted(list(details['display_items'])),
            "pbi_mappings": pbi_maps
        })
            
    return pbi_mappings_result
