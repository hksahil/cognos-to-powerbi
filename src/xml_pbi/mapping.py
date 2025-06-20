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
    """Finds Power BI mappings for all unique Cognos expressions."""
    if not db_to_pbi_map:
        return []

    cognos_expression_details = {}

    for page in mapped_data.get('pages', []):
        for visual in page.get('visuals', []):
            all_items = visual.get('rows', []) + visual.get('columns', [])
            all_filters = visual.get('filters', [])

            for item in all_items:
                cognos_expr = item.get('expression')
                db_map = item.get('db_mapping')
                if cognos_expr and db_map and db_map != 'N/A':
                    if cognos_expr not in cognos_expression_details:
                        cognos_expression_details[cognos_expr] = {
                            "db_column": db_map,
                            "pbi_mappings": db_to_pbi_map.get(db_map, [])
                        }

            for f in all_filters:
                cognos_expr = f.get('column')
                db_map = f.get('db_mapping')
                if cognos_expr and db_map and db_map != 'N/A':
                    if cognos_expr not in cognos_expression_details:
                        cognos_expression_details[cognos_expr] = {
                            "db_column": db_map,
                            "pbi_mappings": db_to_pbi_map.get(db_map, [])
                        }

    result = []
    for cognos_expr, details in sorted(cognos_expression_details.items()):
        result.append({
            "cognos_expression": cognos_expr,
            "db_column": details["db_column"],
            "pbi_mappings": details["pbi_mappings"]
        })
    
    return result
