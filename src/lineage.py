import streamlit as st

from main import SQLLineageAnalyzer
from src.dax import generate_powerbi_equivalent_formula
from src.mapping import find_matching_powerbi_columns, normalize_column_identifier


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
