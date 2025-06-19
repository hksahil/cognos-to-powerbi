import streamlit as st
import json
import re
import pandas as pd
from src.cog_rep import extract_cognos_report_info
from src.dax import generate_dax_for_measure

from dotenv import load_dotenv
load_dotenv(dotenv_path='.env')


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
    """Displays all found Power BI mappings in a non-interactive, collapsible format."""
    st.markdown("---")
    st.header("Step 2: Power BI Mapping Overview")
    if not pbi_data:
        st.warning("No Power BI mappings were found for the database columns in this report.")
        return

    for mapping_group in pbi_data:
        # Consistently use the 'display_items' key
        display_items_str = ", ".join(f"`{item}`" for item in mapping_group['display_items'])
        with st.expander(f"Cognos Items: {display_items_str}"):
            st.markdown(f"**Database Column:** `{mapping_group['db_column']}`")
            pbi_maps = mapping_group.get('pbi_mappings', [])
            if pbi_maps:
                st.markdown("**Found Power BI Mappings:**")
                for pbi_map in pbi_maps:
                    st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp; &bull; **Table:** `{pbi_map.get('table')}`, **Column:** `{pbi_map.get('column')}`")
            else:
                st.markdown("_No corresponding Power BI mapping found._")

def resolve_ambiguities(pbi_data):
    """Creates a UI section for resolving ambiguous mappings and populates all resolved choices."""
    ambiguous_items = [
        group for group in pbi_data
        if group.get('pbi_mappings') and len(group.get('pbi_mappings')) > 1
    ]
    non_ambiguous_items = [
        group for group in pbi_data
        if group.get('pbi_mappings') and len(group.get('pbi_mappings')) == 1
    ]

    # Automatically set choices for non-ambiguous items first. This is the crucial fix.
    for mapping_group in non_ambiguous_items:
        db_column = mapping_group['db_column']
        pbi_map = mapping_group['pbi_mappings'][0]
        st.session_state.ambiguity_choices[db_column] = f"'{pbi_map.get('table')}'[{pbi_map.get('column')}]"

    st.markdown("---")
    st.header("Step 3: Resolve Ambiguities")

    if not ambiguous_items:
        st.success("✅ No mapping ambiguities to resolve.")
        return

    st.info("The following database columns have multiple Power BI mappings. Please select the correct one for each.")
    
    for mapping_group in ambiguous_items:
        db_column = mapping_group['db_column']
        # Consistently use the 'display_items' key
        display_items = mapping_group.get('display_items', [])
        pbi_maps = mapping_group.get('pbi_mappings', [])
        options = [f"'{pbi_map.get('table')}'[{pbi_map.get('column')}]" for pbi_map in pbi_maps]
        
        display_items_str = ", ".join(f"`{item}`" for item in display_items)
        st.markdown(f"#### Resolve for Cognos Items: {display_items_str}")
        st.caption(f"(Database Column: `{db_column}`)")
        
        current_choice = st.session_state.ambiguity_choices.get(db_column, options[0])
        chosen = st.radio(
            label="Select the correct Power BI column:",
            options=options,
            index=options.index(current_choice) if current_choice in options else 0,
            key=f"radio_resolve_{db_column}"
        )
        st.session_state.ambiguity_choices[db_column] = chosen


def configure_visuals(mapped_data, ambiguity_choices):
    """Creates a UI for configuring Power BI visuals and their filters."""
    st.markdown("---")
    st.header("Step 4: Configure Visuals")

    if 'visual_configs' not in st.session_state:
        st.session_state.visual_configs = {}

    def parse_pbi_string(pbi_string):
        """Helper to parse 'Table'[Column] into a tuple."""
        match = re.match(r"'(.*?)'\[(.*?)\]", pbi_string)
        if match:
            return match.groups()
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

        # Try to match '= 'val''
        equals_match = re.search(r'=\s*\'(.*?)\'', expression)
        if equals_match:
            return [equals_match.group(1)] # Return the single value in a list
        
        return []


    for p_idx, page in enumerate(mapped_data.get('pages', [])):
        st.subheader(f"Page: {page.get('page_name', 'Unnamed Page')}")
        for v_idx, visual in enumerate(page.get('visuals', [])):
            visual_key = f"p{p_idx}_v{v_idx}"
            
            with st.container(border=True):
                st.markdown(f"**Visual:** `{visual.get('visual_name', 'Unnamed Visual')}`")
                
                visual_config_data = {
                    "visual_name": visual.get('visual_name'),
                    "visual_type": visual.get('visual_type')
                }

                if visual.get('visual_type') == 'crosstab':
                    visual_config_data['visual_type'] = 'matrix'
                    # --- Prepare detailed options and string-based lookups for UI ---
                    row_options_lookup = {}
                    col_val_options_lookup = {}

                    for item in visual.get('rows', []):
                        db_map = item.get('db_mapping')
                        if db_map and db_map in ambiguity_choices:
                            pbi_string = ambiguity_choices[db_map]
                            table, column = parse_pbi_string(pbi_string)
                            if table:
                                pbi_type = 'Measure' if item.get('type').lower() == 'measure' else 'Column'
                                detail = {"pbi_expression": f"'{table}'[{column}]","table": table, "column": column, "type": pbi_type}
                                if pbi_type == 'Measure':
                                    detail['aggregation'] = item.get('aggregation')
                                row_options_lookup[pbi_string] = detail
                    
                    for item in visual.get('columns', []):
                        db_map = item.get('db_mapping')
                        if db_map and db_map in ambiguity_choices:
                            pbi_string = ambiguity_choices[db_map]
                            table, column = parse_pbi_string(pbi_string)
                            if table:
                                pbi_type = 'Measure' if item.get('type').lower() == 'measure' else 'Column'
                                detail = {"pbi_expression": f"'{table}'[{column}]","table": table, "column": column, "type": pbi_type}
                                if pbi_type == 'Measure':
                                    detail['aggregation'] = item.get('aggregation')
                                col_val_options_lookup[pbi_string] = detail
                    row_options = list(row_options_lookup.keys())
                    col_val_options = list(col_val_options_lookup.keys())
                    
                    current_config = st.session_state.visual_configs.get(visual_key, {})
                    
                    if current_config:
                        def format_item(item):
                            if isinstance(item, dict):
                                return f"'{item['table']}'[{item['column']}]"
                            return str(item)

                        default_rows = [format_item(item) for item in current_config.get('rows', [])]
                        default_cols = [format_item(item) for item in current_config.get('columns', [])]
                        default_vals = [format_item(item) for item in current_config.get('values', [])]
                    else:
                        default_rows = row_options
                        default_cols = []
                        default_vals = []

                    selected_rows_str = st.multiselect("Matrix Rows", options=row_options, default=default_rows, key=f"{visual_key}_rows")
                    selected_cols_str = st.multiselect("Matrix Columns", options=col_val_options, default=default_cols, key=f"{visual_key}_cols")
                    selected_vals_str = st.multiselect("Matrix Values", options=col_val_options, default=default_vals, key=f"{visual_key}_vals")
                    
                    # --- THIS IS THE CRUCIAL FIX ---
                    # Instead of rebuilding items from a clean source, we first check if the item
                    # already exists in the current config. If it does, we reuse it to preserve its state (like ai_generated_dax).
                    
                    # 1. Create a lookup of all items from the PREVIOUS state. These may have AI DAX.
                    previous_items_lookup = {format_item(item): item for item in current_config.get('rows', [])}
                    previous_items_lookup.update({format_item(item): item for item in current_config.get('columns', [])})
                    previous_items_lookup.update({format_item(item): item for item in current_config.get('values', [])})

                    # 2. Create a lookup of all "clean" items available for selection.
                    all_options_lookup = {**row_options_lookup, **col_val_options_lookup}

                    # 3. Rebuild the selected lists. Prioritize using the item from the previous state.
                    selected_rows_obj = [previous_items_lookup.get(s, all_options_lookup.get(s)) for s in selected_rows_str if s in all_options_lookup or s in previous_items_lookup]
                    selected_cols_obj = [previous_items_lookup.get(s, all_options_lookup.get(s)) for s in selected_cols_str if s in all_options_lookup or s in previous_items_lookup]
                    selected_vals_obj = [previous_items_lookup.get(s, all_options_lookup.get(s)) for s in selected_vals_str if s in all_options_lookup or s in previous_items_lookup]

                    visual_config_data.update({
                        "rows": selected_rows_obj,
                        "columns": selected_cols_obj,
                        "values": selected_vals_obj
                    })
                else:
                    st.info(f"Visual type '{visual.get('visual_type')}' will be implemented later.")

                # --- Handle and Display Filters for ALL visual types ---
                resolved_filters = []
                for f in visual.get('filters', []):
                    db_map = f.get('db_mapping')
                    if db_map and db_map in ambiguity_choices:
                        pbi_string = ambiguity_choices[db_map]
                        table, column = parse_pbi_string(pbi_string)
                        if table:
                            full_cognos_filter_expr = f.get('expression')
                            filter_values = parse_filter_expression(full_cognos_filter_expr)

                            if filter_values:
                                resolved_filters.append({
                                    "pbi_expression": f"'{table}'[{column}]",
                                    "table": table,
                                    "column": column,
                                    "type": "Column",
                                    "filter_type": "Categorical",
                                    "values": filter_values
                                })
                
                
                visual_config_data['filters'] = resolved_filters

                # Store the complete configuration for this visual in session state
                st.session_state.visual_configs[visual_key] = visual_config_data

def main():
    """Main function to run the Streamlit application."""
    st.set_page_config(layout="wide")
    st.title("Cognos to Power BI Report Generator")
    st.write("Paste your Cognos `report.xml` content below to start the report generation process.")

    # Initialize session state variables
    if 'mapped_data' not in st.session_state:
        st.session_state.mapped_data = None
    if 'pbi_mappings' not in st.session_state:
        st.session_state.pbi_mappings = None
    if 'ambiguity_choices' not in st.session_state:
        st.session_state.ambiguity_choices = {}
    if 'final_config' not in st.session_state:
        st.session_state.final_config = None
    if 'measure_ai_dax_results' not in st.session_state:
        st.session_state.measure_ai_dax_results = {}

    xml_input = st.text_area("Paste XML content here", height=300, placeholder="<report>...</report>")

    if st.button("Analyze and Find All Mappings"):
        # Reset choices on new analysis
        st.session_state.ambiguity_choices = {}
        st.session_state.visual_configs = {}
        st.session_state.final_config = None
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
        tab1, tab2 = st.tabs(["Analysis and Configuration", "Raw JSON"])
        with tab1:
            display_structured_data(st.session_state.mapped_data)
            if st.session_state.pbi_mappings is not None:

                old_ambiguity_choices = st.session_state.ambiguity_choices.copy()

                display_pbi_mappings(st.session_state.pbi_mappings)
                resolve_ambiguities(st.session_state.pbi_mappings)

                                # If the user changed an ambiguity choice, the old visual config is invalid.
                if old_ambiguity_choices != st.session_state.ambiguity_choices:
                    st.session_state.visual_configs = {} # Reset the visual configuration
                    st.rerun() # Rerun to rebuild the UI with a clean state


                configure_visuals(st.session_state.mapped_data, st.session_state.ambiguity_choices)

                if st.button("Save Visual Configuration"):
                    st.session_state.final_config = {
                        "report_name": st.session_state.mapped_data.get('report_name', 'Generated Report'),
                        "visuals": list(st.session_state.visual_configs.values())
                    }


                # --- AI DAX Generation (using pbi-app.py pattern) ---
                if st.button("Generate DAX for Measures"):
                    if not st.session_state.get('visual_configs'):
                        st.warning("Please configure and select items for visuals before generating DAX.")
                    else:
                        # 1. Collect all unique measures to process
                        tasks_to_process = {}
                        for visual_key, visual_config in st.session_state.visual_configs.items():
                            for field_type in ['rows', 'columns', 'values']:
                                for item in visual_config.get(field_type, []):
                                    if item.get('type').lower() == 'measure' and item.get('pbi_expression') and item.get('aggregation'):
                                        unique_key = f"{visual_key}_{item['pbi_expression']}"
                                        if unique_key not in tasks_to_process:
                                            tasks_to_process[unique_key] = {
                                                "pbi_expression": item['pbi_expression'],
                                                "aggregation": item['aggregation']
                                            }
                        
                        items_to_process = list(tasks_to_process.items())

                        if not items_to_process:
                            st.info("No measures selected in any visual to generate DAX for.")
                        else:
                            # 2. Call AI and cache results
                            ai_results_cache = {}
                            with st.spinner(f"🤖 Generating DAX for {len(items_to_process)} measure(s)..."):
                                for unique_key, task in items_to_process:
                                    ai_results = generate_dax_for_measure(task['pbi_expression'], task['aggregation'])
                                    ai_results['input_expression'] = task['pbi_expression']
                                    ai_results_cache[unique_key] = ai_results
                            

                            # print(ai_results_cache)  # Debugging output to check AI results
                            # 3. Update the main visual_configs in session state IN-PLACE
                            config_updated = False
                            for visual_key, visual_config in st.session_state.visual_configs.items():
                                for field_type in ['rows', 'columns', 'values']:
                                    for item in visual_config.get(field_type, []):
                                        if item.get('type').lower() == 'measure':
                                            lookup_key = f"{visual_key}_{item['pbi_expression']}"
                                            if lookup_key in ai_results_cache:
                                                ai_output = ai_results_cache[lookup_key]
                                                generated_dax = ai_output.get('measure')
                                                if generated_dax and not generated_dax.startswith("Error"):
                                                    item['ai_generated_dax'] = generated_dax
                                                    item['ai_data_type'] = ai_output.get('dataType', 'text')
                                                    config_updated = True
                            
                            # 4. Update session state for the display section and rerun
                            st.session_state.measure_ai_dax_results = ai_results_cache
                            st.success(f"✅ AI DAX generation complete. Configuration has been updated.")
                            # --- FIX: Automatically update the final config for immediate display ---
                            if config_updated:
                                st.session_state.final_config = {
                                    "report_name": st.session_state.mapped_data.get('report_name', 'Generated Report'),
                                    "visuals": list(st.session_state.visual_configs.values())
                                }
                
                # --- NEW: Display Generated DAX Section ---
                if st.session_state.measure_ai_dax_results:
                    st.markdown("---")
                    st.header("Generated DAX Measures")
                    st.info("The following DAX measures have been generated and applied to the configuration above. Review them and click 'Save Visual Configuration' to see the final JSON.")
                    for key, result in st.session_state.measure_ai_dax_results.items():
                        input_expr = result.get('input_expression', 'Unknown Measure')
                        dax_measure = result.get('measure', 'Error: Not generated.')
                        with st.expander(f"DAX for: `{input_expr}`"):
                            st.code(dax_measure, language='dax')
                
                if st.session_state.final_config:
                    st.markdown("---")
                    st.header("Final Configuration JSON")
                    st.json(st.session_state.final_config)

        with tab2:
            st.json(st.session_state.mapped_data)


if __name__ == "__main__":
    main()