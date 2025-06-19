import streamlit as st
import json
import re
import pandas as pd
from src.cog_rep import extract_cognos_report_info

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
    """Finds Power BI mappings for all unique DB columns and their associated Cognos names."""
    if not db_to_pbi_map:
        return [] # Return an empty list

    db_column_details = {}

    # Collect all unique DB columns and their associated Cognos names
    for page in mapped_data.get('pages', []):
        for visual in page.get('visuals', []):
            # Process rows and columns
            for item in visual.get('rows', []) + visual.get('columns', []):
                db_map = item.get('db_mapping')
                cognos_name = item.get('name')
                if db_map and db_map != 'N/A' and cognos_name:
                    if db_map not in db_column_details:
                        db_column_details[db_map] = {'cognos_names': set()}
                    db_column_details[db_map]['cognos_names'].add(cognos_name)
            
            # Process filters
            for f in visual.get('filters', []):
                db_map = f.get('db_mapping')
                # The 'column' field in a filter holds the Cognos expression/name
                cognos_name = f.get('column')
                if db_map and db_map != 'N/A' and cognos_name:
                    if db_map not in db_column_details:
                        db_column_details[db_map] = {'cognos_names': set()}
                    db_column_details[db_map]['cognos_names'].add(cognos_name)
    
    # Build the final result structure
    pbi_mappings_result = []
    for db_col, details in sorted(db_column_details.items()):
        pbi_maps = db_to_pbi_map.get(db_col, [])
        pbi_mappings_result.append({
            "db_column": db_col,
            "cognos_names": sorted(list(details['cognos_names'])),
            "pbi_mappings": pbi_maps
        })
            
    return pbi_mappings_result

def display_structured_data(data):
    """Displays the extracted report data in a structured, user-friendly format."""
    st.header("Step 1: Cognos Report Analysis")
    st.subheader(f"Report Name: {data.get('report_name', 'N/A')}")

    for page in data.get('pages', []):
        with st.expander(f"Page: {page.get('page_name', 'Unnamed Page')}", expanded=True):
            for i, visual in enumerate(page.get('visuals', [])):
                st.markdown("---")
                st.subheader(f"Visual: {visual.get('visual_name', 'Unnamed Visual')}")
                st.caption(f"Type: `{visual.get('visual_type')}` | Query Reference: `{visual.get('query_ref')}`")

                all_fields = []
                for item in visual.get('rows', []):
                    item['role'] = 'Row'
                    all_fields.append(item)
                for item in visual.get('columns', []):
                    item['role'] = 'Column'
                    all_fields.append(item)
                for f in visual.get('filters', []):
                    filter_field = {
                        'role': 'Filter', 'name': f.get('column', 'N/A'), 'type': None,
                        'aggregation': None, 'db_mapping': f.get('db_mapping', 'N/A'),
                        'expression': f.get('expression')
                    }
                    all_fields.append(filter_field)

                if all_fields:
                    df = pd.DataFrame(all_fields)
                    df.fillna({'type': '-', 'aggregation': '-'}, inplace=True)
                    df_display = df[['role', 'name', 'type', 'aggregation', 'db_mapping', 'expression']]
                    df_display.columns = ['Role', 'Name', 'Type', 'Aggregation', 'DB Mapping', 'Cognos Expression']
                    st.dataframe(df_display)

def display_pbi_mappings(pbi_data):
    """Displays the found Power BI mappings in a structured, collapsible format."""
    st.markdown("---")
    st.header("Step 2: Power BI Mapping")
    if not pbi_data:
        st.warning("No Power BI mappings were found for the database columns in this report.")
        return

    for mapping_group in pbi_data:
        cognos_names_str = ", ".join(f"`{name}`" for name in mapping_group['cognos_names'])
        with st.expander(f"Cognos Names: {cognos_names_str}"):
            st.markdown(f"**Database Column:** `{mapping_group['db_column']}`")
            
            pbi_maps = mapping_group.get('pbi_mappings', [])
            if pbi_maps:
                st.markdown("**Power BI Mappings:**")
                for i, pbi_map in enumerate(pbi_maps):
                    st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp; &bull; **Table:** `{pbi_map.get('table')}`, **Column:** `{pbi_map.get('column')}`")
            else:
                st.markdown("_No corresponding Power BI mapping found._")
def main():
    """Main function to run the Streamlit application."""
    st.set_page_config(layout="wide")
    st.title("Cognos to Power BI Report Generator")
    st.write("Paste your Cognos `report.xml` content below to start the report generation process.")

    if 'mapped_data' not in st.session_state:
        st.session_state.mapped_data = None
    if 'pbi_mappings' not in st.session_state:
        st.session_state.pbi_mappings = None

    xml_input = st.text_area("Paste XML content here", height=300, placeholder="<report>...</report>")

    if st.button("Analyze and Find All Mappings"):
        if xml_input:
            try:
                report_data = extract_cognos_report_info(xml_input)
                if not report_data:
                    st.error("Could not extract information from the XML.")
                    st.session_state.mapped_data = None
                    st.session_state.pbi_mappings = None
                else:
                    all_mappings = load_all_mappings()
                    if all_mappings:
                        cognos_to_db_map = all_mappings.get("mappings", {}).get("cognos_to_db", {})
                        st.session_state.mapped_data = map_cognos_to_db(report_data, cognos_to_db_map)
                        
                        db_to_pbi_map = all_mappings.get("mappings", {}).get("db_to_powerbi", {})
                        st.session_state.pbi_mappings = find_pbi_mappings(st.session_state.mapped_data, db_to_pbi_map)
                        st.success("✅ Analysis and mapping complete.")
                    else:
                        st.session_state.mapped_data = None
                        st.session_state.pbi_mappings = None
            except Exception as e:
                st.error(f"An error occurred: {e}")
                st.session_state.mapped_data = None
                st.session_state.pbi_mappings = None
        else:
            st.warning("Please paste XML content to begin.")

    if st.session_state.mapped_data:
        tab1, tab2 = st.tabs(["Structured View", "Raw JSON"])
        with tab1:
            display_structured_data(st.session_state.mapped_data)
            if st.session_state.pbi_mappings is not None:
                display_pbi_mappings(st.session_state.pbi_mappings)
        with tab2:
            st.json(st.session_state.mapped_data)
        st.info("Next Step: Configure visuals based on the mapped data.")

if __name__ == "__main__":
    main()