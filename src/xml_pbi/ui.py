import streamlit as st
import pandas as pd
from src.xml_pbi.utils import parse_pbi_string, parse_filter_expression

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

def save_visual_configuration():
    """Reads the current state of the UI widgets and saves it to st.session_state.visual_configs."""
    if 'temp_visual_lookups' not in st.session_state:
        st.warning("Cannot save, configuration UI has not been generated yet.")
        return

    new_configs = {}
    for visual_key, lookups in st.session_state.temp_visual_lookups.items():
        visual_config_data = {
            "visual_name": lookups['original_visual_data'].get('visual_name'),
            "visual_type": 'matrix'
        }

        # Get selections from multiselects
        selected_rows_str = st.session_state.get(f"{visual_key}_rows", [])
        selected_cols_str = st.session_state.get(f"{visual_key}_cols", [])
        selected_vals_str = st.session_state.get(f"{visual_key}_vals", [])

        all_options_lookup = {**lookups['row_options_lookup'], **lookups['col_val_options_lookup']}

        # Rebuild objects from selections
        visual_config_data["rows"] = [all_options_lookup.get(s) for s in selected_rows_str if s in all_options_lookup]
        visual_config_data["columns"] = [all_options_lookup.get(s) for s in selected_cols_str if s in all_options_lookup]
        visual_config_data["values"] = [all_options_lookup.get(s) for s in selected_vals_str if s in all_options_lookup]

        # Re-process filters
        resolved_filters = []
        for f in lookups['original_visual_data'].get('filters', []):
            db_map = f.get('db_mapping')
            if db_map and db_map in st.session_state.ambiguity_choices:
                pbi_string = st.session_state.ambiguity_choices[db_map]
                table, column = parse_pbi_string(pbi_string)
                if table:
                    filter_values = parse_filter_expression(f.get('expression'))
                    if filter_values:
                        resolved_filters.append({
                            "pbi_expression": f"'{table}'[{column}]", "table": table, "column": column,
                            "type": "Column", "filter_type": "Categorical", "values": filter_values
                        })
        visual_config_data['filters'] = resolved_filters
        
        new_configs[visual_key] = visual_config_data
    
    st.session_state.visual_configs = new_configs
    st.success("✅ Visual configuration saved!")
    st.rerun()
    
def configure_visuals(mapped_data, ambiguity_choices):
    """Creates a UI for configuring Power BI visuals and their filters."""
    st.markdown("---")
    st.header("Step 4: Configure Visuals")

    if 'visual_configs' not in st.session_state:
        st.session_state.visual_configs = {}
    
    # This will hold the data needed by the save function
    st.session_state.temp_visual_lookups = {}

    for p_idx, page in enumerate(mapped_data.get('pages', [])):
        st.subheader(f"Page: {page.get('page_name', 'Unnamed Page')}")
        for v_idx, visual in enumerate(page.get('visuals', [])):
            visual_key = f"p{p_idx}_v{v_idx}"
            
            with st.container(border=True):
                st.markdown(f"**Visual:** `{visual.get('visual_name', 'Unnamed Visual')}`")

                if visual.get('visual_type') == 'crosstab':
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
                    
                    # Save lookups for the save button to use later
                    st.session_state.temp_visual_lookups[visual_key] = {
                        "row_options_lookup": row_options_lookup,
                        "col_val_options_lookup": col_val_options_lookup,
                        "original_visual_data": visual
                    }

                    row_options = list(row_options_lookup.keys())
                    col_val_options = list(col_val_options_lookup.keys())
                    
                    # Get defaults from the *last saved* configuration
                    current_config = st.session_state.visual_configs.get(visual_key, {})
                    
                    def format_item(item):
                        if isinstance(item, dict):
                            return f"'{item['table']}'[{item['column']}]"
                        return str(item)

                    default_rows = [format_item(item) for item in current_config.get('rows', [])]
                    default_cols = [format_item(item) for item in current_config.get('columns', [])]
                    default_vals = [format_item(item) for item in current_config.get('values', [])]

                    st.multiselect("Matrix Rows", options=row_options, default=default_rows, key=f"{visual_key}_rows")
                    st.multiselect("Matrix Columns", options=col_val_options, default=default_cols, key=f"{visual_key}_cols")
                    st.multiselect("Matrix Values", options=col_val_options, default=default_vals, key=f"{visual_key}_vals")
                    
                else:
                    st.info(f"Visual type '{visual.get('visual_type')}' will be implemented later.")
