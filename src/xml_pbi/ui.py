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
                for item in visual.get('values', []):
                    item['role'] = 'Value'
                    all_fields.append(item)
                for f in visual.get('filters', []):
                    filter_field = {
                        'role': 'Filter', 'name': f.get('column', 'N/A'), 'type': None,
                        'aggregation': None, 'pbi_mapping': f.get('pbi_mapping', 'N/A'),
                        'expression': f.get('expression')
                    }
                    all_fields.append(filter_field)

                if all_fields:
                    df = pd.DataFrame(all_fields)
                    df.fillna({'type': '-', 'aggregation': '-'}, inplace=True)
                    df_display = df[['role', 'name', 'type', 'aggregation', 'pbi_mapping', 'expression']]
                    df_display.columns = ['Role', 'Name', 'Type', 'Aggregation', 'Power BI Mapping', 'Cognos Expression']
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
    """Creates a UI for resolving ambiguous DB to Power BI mappings for each Cognos item."""
    if not pbi_data:
        return

    # This will hold the final choices
    choices = {}
    ambiguous_mappings_found = False

    def format_pbi_map(item):
        """Formats a PBI mapping object into a user-friendly string."""
        if isinstance(item, str):
            return item # Already in the correct format
        if isinstance(item, dict):
            # Use .strip() to handle potential whitespace issues in the mapping file
            table = item.get('table', '').strip()
            column = item.get('column', '').strip()
            return f"'{table}'[{column}]"
        return str(item) # Fallback for other types

    for mapping in pbi_data:
        cognos_expr = mapping['cognos_expression']
        db_col = mapping['db_column']
        pbi_maps = mapping['pbi_mappings']

        if not pbi_maps:
            choices[cognos_expr] = None
        elif len(pbi_maps) == 1:
            # Automatically resolved, store the string format
            choices[cognos_expr] = format_pbi_map(pbi_maps[0])
        else:
            # Ambiguous mapping, requires user input
            ambiguous_mappings_found = True
            with st.container(border=True):
                st.markdown(f"**Resolve for Cognos Item:** `{cognos_expr}`")
                st.markdown(f"*(Database Column: `{db_col}`)*")
                
                # The `options` are dictionaries, so we use `format_func` to display them nicely.
                choice_obj = st.radio(
                    "Select the correct Power BI column:",
                    options=pbi_maps,
                    format_func=format_pbi_map,
                    key=cognos_expr
                )
                # Store the formatted string representation, not the dictionary, for consistency.
                choices[cognos_expr] = format_pbi_map(choice_obj)

    if ambiguous_mappings_found:
        st.info("Review the selections above for any ambiguous mappings.")
    else:
        st.success("✅ All mappings were resolved automatically.")

    # Update session state with the new choices
    st.session_state.ambiguity_choices = choices

def save_visual_configuration():
    """
    Saves the user's visual configuration choices from the UI into st.session_state.visual_configs.
    The structure is hierarchical: a dictionary of pages, keyed by page name.
    """
    if 'temp_visual_lookups' not in st.session_state:
        st.warning("Cannot save, no configuration has been performed.")
        return

    mapped_data = st.session_state.get('mapped_data', {})
    if not mapped_data:
        st.error("Original mapped data not found in session state.")
        return

    new_config = {}  # The final configuration will be a dictionary of pages

    for p_idx, page_data in enumerate(mapped_data.get('pages', [])):
        page_name = page_data.get('page_name', f"Page {p_idx + 1}")
        page_visuals = []  # A temporary list to hold visuals for the current page

        for v_idx, visual_data in enumerate(page_data.get('visuals', [])):
            visual_key = f"p{p_idx}_v{v_idx}"
            lookups = st.session_state.temp_visual_lookups.get(visual_key)

            if not lookups:
                continue  # Skip visuals that weren't configured

            field_lookup = lookups.get('field_lookup', {})
            original_visual = lookups['original_visual_data']
            visual_type = original_visual.get('visual_type')
            
            new_visual_config = {
                "visual_name": original_visual.get('visual_name', 'Unnamed Visual'),
                "visual_type": 'matrix' if visual_type == 'crosstab' else visual_type,
                "rows": [], "columns": [], "values": [], "filters": []
            }

            # Process roles based on visual type
            if visual_type == 'crosstab':
                role_map = {'rows': 'rows', 'cols': 'columns', 'vals': 'values'}
                for role_key, config_key in role_map.items():
                    selected_exprs = st.session_state.get(f"{visual_key}_{role_key}", [])
                    for expr in selected_exprs:
                        if expr in field_lookup:
                            new_visual_config[config_key].append(field_lookup[expr])
            elif visual_type == 'table':
                selected_exprs = st.session_state.get(f"{visual_key}_table_cols", [])
                for expr in selected_exprs:
                    if expr in field_lookup:
                        # For PBI tables, all fields can be considered 'values'
                        new_visual_config['values'].append(field_lookup[expr])

            # Re-process and resolve filters
            resolved_filters = []
            print(original_visual.get('filters', []))
            for f in original_visual.get('filters', []):
                cognos_expr = f.get('column')
                if cognos_expr and cognos_expr in st.session_state.ambiguity_choices:
                    pbi_string = st.session_state.ambiguity_choices[cognos_expr]
                    if pbi_string:
                        table, column = parse_pbi_string(pbi_string)
                        if table:
                            filter_values = parse_filter_expression(f.get('expression'))
                            if filter_values:
                                resolved_filters.append({
                                    "pbi_expression": f"'{table}'[{column}]", "table": table, "column": column,
                                    "type": "Column", "filter_type": "Categorical", "values": filter_values
                                })
            new_visual_config['filters'] = resolved_filters
            
            page_visuals.append(new_visual_config)

        if page_visuals:  # Only add pages that have visuals
            new_config[page_name] = {
                "name": page_name,
                "visuals": page_visuals
            }

    st.session_state.visual_configs = new_config
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
                    # --- REFACTORED LOGIC FOR MATRIX ---
                    # 1. Create lists of resolved field objects for rows and columns/values
                    resolved_row_fields = []
                    for item in visual.get('rows', []):
                        cognos_expr = item.get('expression')
                        if cognos_expr and cognos_expr in ambiguity_choices:
                            pbi_string = ambiguity_choices[cognos_expr]
                            if pbi_string:
                                table, column = parse_pbi_string(pbi_string)
                                if table:
                                    pbi_type = 'Measure' if item.get('type').lower() == 'measure' else 'Column'
                                    detail = {
                                        "cognos_expression": cognos_expr, "seq": item.get('seq', 999),
                                        "pbi_expression": f"'{table}'[{column}]", "table": table, 
                                        "column": column, "type": pbi_type
                                    }
                                    if pbi_type == 'Measure':
                                        detail['aggregation'] = item.get('aggregation')
                                    resolved_row_fields.append(detail)

                    resolved_col_fields = []
                    for item in visual.get('columns', []):
                        cognos_expr = item.get('expression')
                        if cognos_expr and cognos_expr in ambiguity_choices:
                            pbi_string = ambiguity_choices[cognos_expr]
                            if pbi_string:
                                table, column = parse_pbi_string(pbi_string)
                                if table:
                                    pbi_type = 'Measure' if item.get('type').lower() == 'measure' else 'Column'
                                    detail = {
                                        "cognos_expression": cognos_expr, "seq": item.get('seq', 999),
                                        "pbi_expression": f"'{table}'[{column}]", "table": table, 
                                        "column": column, "type": pbi_type
                                    }
                                    if pbi_type == 'Measure':
                                        detail['aggregation'] = item.get('aggregation')
                                    resolved_col_fields.append(detail)

                    resolved_val_fields = []
                    for item in visual.get('values', []):
                        cognos_expr = item.get('expression')
                        if cognos_expr and cognos_expr in ambiguity_choices:
                            pbi_string = ambiguity_choices[cognos_expr]
                            if pbi_string:
                                table, column = parse_pbi_string(pbi_string)
                                if table:
                                    pbi_type = 'Measure' if item.get('type').lower() == 'measure' else 'Column'
                                    detail = {
                                        "cognos_expression": cognos_expr, "seq": item.get('seq', 999),
                                        "pbi_expression": f"'{table}'[{column}]", "table": table, 
                                        "column": column, "type": pbi_type
                                    }
                                    if pbi_type == 'Measure':
                                        detail['aggregation'] = item.get('aggregation')
                                    resolved_val_fields.append(detail)

                    # Sort fields based on original Cognos sequence number
                    resolved_row_fields.sort(key=lambda x: x.get('seq', 999), reverse=True)
                    resolved_col_fields.sort(key=lambda x: x.get('seq', 999), reverse=True)
                    resolved_val_fields.sort(key=lambda x: x.get('seq', 999))


                    # 2. Create a single lookup from cognos_expression to the detail object
                    all_fields = resolved_row_fields + resolved_col_fields + resolved_val_fields
                    field_lookup = {field['cognos_expression']: field for field in all_fields}

                    # 3. The `options` for the multiselects are the unique cognos_expressions
                    row_options_keys = [field['cognos_expression'] for field in resolved_row_fields]
                    col_options_keys = [field['cognos_expression'] for field in resolved_col_fields]
                    val_options_keys = [field['cognos_expression'] for field in resolved_col_fields + resolved_val_fields]


                    # 4. The format function displays the PBI string to the user
                    def format_multiselect_option(cognos_expr_key):
                        return field_lookup.get(cognos_expr_key, {}).get('pbi_expression', 'Unknown')

                    # Save the new lookup for the save function to use
                    st.session_state.temp_visual_lookups[visual_key] = {
                        "field_lookup": field_lookup,
                        "original_visual_data": visual
                    }

                    # 5. Determine default selections
                    current_config = st.session_state.visual_configs.get(visual_key, {})
                    
                    is_config_valid = False
                    if current_config:
                        saved_row_exprs = [item.get('cognos_expression') for item in current_config.get('rows', []) if item.get('cognos_expression')]
                        saved_col_exprs = [item.get('cognos_expression') for item in current_config.get('columns', []) if item.get('cognos_expression')]
                        saved_val_exprs = [item.get('cognos_expression') for item in current_config.get('values', []) if item.get('cognos_expression')]
                        
                        all_saved_exprs = saved_row_exprs + saved_col_exprs + saved_val_exprs
                        all_option_keys = row_options_keys + col_options_keys + val_options_keys
                        
                        is_config_valid = all(expr in all_option_keys for expr in all_saved_exprs)

                    if is_config_valid:
                        default_row_keys = [item['cognos_expression'] for item in current_config.get('rows', [])]
                        default_col_keys = [item['cognos_expression'] for item in current_config.get('columns', [])]
                        default_val_keys = [item['cognos_expression'] for item in current_config.get('values', [])]
                    else:
                        # Default to original Cognos roles
                        default_row_keys = row_options_keys
                        default_col_keys = col_options_keys
                        default_val_keys = [field['cognos_expression'] for field in resolved_val_fields]

                    # 6. Create the multiselect widgets
                    st.multiselect("Matrix Rows", options=row_options_keys, default=default_row_keys, format_func=format_multiselect_option, key=f"{visual_key}_rows")
                    st.multiselect("Matrix Columns", options=col_options_keys, default=default_col_keys, format_func=format_multiselect_option, key=f"{visual_key}_cols")
                    st.multiselect("Matrix Values", options=val_options_keys, default=default_val_keys, format_func=format_multiselect_option, key=f"{visual_key}_vals")


                elif visual.get('visual_type') == 'table':
                    # --- REFACTORED LOGIC FOR TABLES ---
                    # 1. Create a list of resolved field objects. This preserves order and duplicates.
                    resolved_fields = []
                    for item in visual.get('columns', []):
                        cognos_expr = item.get('expression')
                        if cognos_expr and cognos_expr in ambiguity_choices:
                            pbi_string = ambiguity_choices[cognos_expr]
                            if pbi_string:
                                table, column = parse_pbi_string(pbi_string)
                                if table:
                                    pbi_type = 'Measure' if item.get('type').lower() == 'measure' else 'Column'
                                    detail = {
                                        "cognos_expression": cognos_expr, # Keep track of the origin
                                        "seq": item.get('seq', 999),
                                        "pbi_expression": f"'{table}'[{column}]",
                                        "table": table,
                                        "column": column,
                                        "type": pbi_type
                                    }
                                    if pbi_type == 'Measure':
                                        detail['aggregation'] = item.get('aggregation')
                                    resolved_fields.append(detail)

                    # Sort the fields based on the original Cognos sequence number
                    resolved_fields.sort(key=lambda x: x.get('seq', 999))

                    # 2. Create a lookup from the unique key (cognos_expr) to the detail object
                    field_lookup = {field['cognos_expression']: field for field in resolved_fields}

                    # 3. The `options` for the multiselect are the unique cognos_expressions
                    options_keys = [field['cognos_expression'] for field in resolved_fields]

                    # 4. The format function displays the PBI string to the user
                    def format_multiselect_option(cognos_expr_key):
                        return field_lookup.get(cognos_expr_key, {}).get('pbi_expression', 'Unknown')

                    # Save the new lookup for the save function to use
                    st.session_state.temp_visual_lookups[visual_key] = {
                        "field_lookup": field_lookup,
                        "original_visual_data": visual
                    }

                    # 5. Determine default selections
                    current_config = st.session_state.visual_configs.get(visual_key, {})
                    
                    saved_cognos_exprs = []
                    if current_config:
                        # A saved item might not have the cognos_expression if it's from an old format
                        saved_cognos_exprs = [item['cognos_expression'] for item in current_config.get('columns', []) if 'cognos_expression' in item]

                    is_config_valid = current_config and all(expr in options_keys for expr in saved_cognos_exprs)

                    if is_config_valid:
                        default_keys = saved_cognos_exprs
                    else:
                        default_keys = options_keys

                    # 6. Create the multiselect widget
                    st.multiselect(
                        "Table Columns",
                        options=options_keys,
                        default=default_keys,
                        format_func=format_multiselect_option,
                        key=f"{visual_key}_table_cols"
                    )
                    
                else:
                    st.info(f"Visual type '{visual.get('visual_type')}' will be implemented later.")
