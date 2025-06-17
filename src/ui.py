import subprocess
from io import StringIO
from pathlib import Path

import streamlit as st
import pandas as pd
import sqlparse
import yaml

from src.constants import MAPPING_FILE_PATH, CONNECTION_STRING, DATABASE_NAME
from src.dax import generate_powerbi_equivalent_formula, generate_dax_from_sql, parse_dax_filter_for_display, \
    parse_simple_dax_filter
from src.lineage import build_visual_candidates, enrich_selected_items
from src.mapping import load_column_mappings, normalize_column_identifier, find_matching_powerbi_columns
from src.utils import FlowDict, CustomDumper


def display_sidebar():
    """Displays the sidebar content."""
    with st.sidebar:
        st.header("Settings")
        current_mappings_in_session = st.session_state.get('column_mappings')
        
        if isinstance(current_mappings_in_session, dict) and "db_to_powerbi" in current_mappings_in_session:
            st.info(f"✅ Using mapping file: {MAPPING_FILE_PATH}")
            st.info(f"Contains {len(current_mappings_in_session.get('db_to_powerbi', {}))} database columns")
        else:
            st.warning(f"⚠️ Could not load or parse mapping file correctly from {MAPPING_FILE_PATH}. Check console for errors.")
            if st.button("Retry Loading Mappings"):
                st.session_state['column_mappings'] = load_column_mappings()
                st.rerun()


def display_query_input_area():
    """Displays the SQL query input and action buttons."""
    col1, col2 = st.columns([4, 1])
    sql_query_input = ""
    analyze_button_pressed = False
    clear_button_pressed = False

    with col1:
        sql_query_input = st.text_area("Enter your SQL query:", 
                                value=st.session_state.get('sql_query', ""),
                                height=300)
        st.session_state['sql_query'] = sql_query_input # Keep session state updated
    
    with col2:
        st.write("### Actions")
        analyze_button_pressed = st.button("Analyze Query", use_container_width=True)
        clear_button_pressed = st.button("Clear Query", use_container_width=True)
        
        if clear_button_pressed:
            st.session_state['sql_query'] = ""
            st.session_state['lineage_data'] = None
            st.session_state['all_types'] = []
            st.session_state['dax_expressions'] = {}
            st.session_state['mapping_results'] = None
            st.session_state['visual_config_candidates'] = []
            st.session_state['visual_ambiguity_choices'] = {}
            st.session_state['base_col_ambiguity_choices'] = {}
            st.session_state['visual_selected_rows'] = []
            st.session_state['visual_selected_columns'] = []
            st.session_state['visual_selected_values'] = []
            st.session_state['visual_ai_dax_results'] = {}
            st.session_state['generated_pbi_config'] = None
            st.session_state['resolved_base_col_to_pbi'] = {}
            st.rerun()
            
    return sql_query_input, analyze_button_pressed



def display_analysis_results_tabs():
    """Displays the tabs for SQL analysis results."""
    # ... (The entire content of the 'if st.session_state['lineage_data']:' block for tabs)
    # ... (This includes tab1, tab2, tab3, tab_filters, tab4 definitions and their 'with' blocks)
    # ... (This function will be quite large, consider breaking each tab into its own function too)
    # --- Display Analysis Results Tabs (existing logic) ---
    if st.session_state['lineage_data']:
        st.subheader("Analysis Results")
        df = pd.DataFrame(st.session_state['lineage_data'])
        
        options_for_general_tabs = [t for t in st.session_state.get('all_types', []) if t != 'filter_condition']

        tab1, tab2, tab3, tab_filters, tab4 = st.tabs([
            "Table View", "Detail View", "PBI Mapping", "Filter Conditions", "Raw JSON"
        ])

        with tab1: 
            # ... (Content of Table View tab) ...
            st.header("SQL Query Analysis - Table View")
            df_display_tab1 = df[df['type'] != 'filter_condition'].copy()
            selected_types_tab1 = st.multiselect(
                "Filter by type (excluding filter conditions):",
                options=options_for_general_tabs, 
                default=options_for_general_tabs, 
                key="filter_types_tab1_vis_revised"
            )
            df_display_tab1['display_type_for_filter'] = df_display_tab1['type'].replace('column', 'base')
            if selected_types_tab1:
                filtered_df_tab1 = df_display_tab1[df_display_tab1['display_type_for_filter'].isin(selected_types_tab1)]
            else:
                filtered_df_tab1 = df_display_tab1
            st.dataframe(filtered_df_tab1.drop(columns=['display_type_for_filter']), use_container_width=True)
            if not filtered_df_tab1.empty:
                csv_tab1 = filtered_df_tab1.drop(columns=['display_type_for_filter']).to_csv(index=False).encode('utf-8')
                st.download_button(label="Download Filtered Table View (CSV)", data=csv_tab1, file_name="table_view_analysis.csv", mime="text/csv", key="download_csv_tab1_vis_revised")

        with tab2: 
            # ... (Content of Detail View tab - this is extensive) ...
            st.header("SQL Query Analysis - Detail View")
            selected_types_tab2 = st.multiselect(
                "Filter by type (excluding filter conditions):",
                options=options_for_general_tabs, 
                default=options_for_general_tabs, 
                key="filter_types_tab2_vis_revised"
            )
            items_for_detail_view = [
                item_detail for item_detail in st.session_state['lineage_data'] 
                if item_detail['type'] != 'filter_condition' and \
                   (item_detail['type'].replace('column', 'base') in selected_types_tab2 if selected_types_tab2 else True)
            ]
            if not items_for_detail_view:
                st.info("No items to display based on the current filter (excluding filter conditions).")
            else:
                for i_detail, item_detail_data in enumerate(items_for_detail_view): 
                    expander_label_key = item_detail_data.get('item', item_detail_data.get('column', f"Item {i_detail+1}")) # Use 'item' first
                    with st.expander(f"Details for: {expander_label_key} (Type: {item_detail_data['type']})"):
                        # ... (rest of the detailed view logic from your original code)
                        st.write("**Type:** ", item_detail_data['type'])
                        pbi_eq_formula_detail = item_detail_data.get('final_expression', "") 
                        made_change_in_rule_based_translation_detail = False 
                        if item_detail_data['type'] == 'expression' and item_detail_data.get('final_expression'):
                            # ... (SQL expression display, PBI equivalent, AI DAX button and display) ...
                            formatted_expr_detail = sqlparse.format(item_detail_data['final_expression'], reindent=True, keyword_case='upper', indent_width=2)
                            st.write("**SQL Expression:**"); st.code(formatted_expr_detail, language="sql")
                            st.markdown("---"); st.write("**Power BI Equivalent Formula (Rule-Based Translation):**")
                            if st.session_state.get('column_mappings') and item_detail_data.get('base_columns'):
                                pbi_eq_formula_detail, made_change_in_rule_based_translation_detail = generate_powerbi_equivalent_formula(
                                    item_detail_data['final_expression'], item_detail_data.get('base_columns'), 
                                    st.session_state['column_mappings'], st.session_state.get('resolved_base_col_to_pbi', {}))
                                if made_change_in_rule_based_translation_detail: st.code(pbi_eq_formula_detail, language="dax") 
                                else: st.caption("Could not translate..."); pbi_eq_formula_detail = item_detail_data['final_expression'] 
                            # ... (other conditions for translation) ...
                            st.markdown("---"); item_id_detail = f"{expander_label_key}_{i_detail}" 
                            expression_for_ai_detail = pbi_eq_formula_detail if made_change_in_rule_based_translation_detail else item_detail_data.get('final_expression', '')
                            if st.button(f"Generate DAX with AI", key=f"dax_btn_{item_id_detail}_vis_revised"): 
                                if expression_for_ai_detail and expression_for_ai_detail.strip():
                                    with st.spinner("Generating DAX with AI..."):
                                        dax_results_detail = generate_dax_from_sql(expression_for_ai_detail)
                                        st.session_state['dax_expressions'][item_id_detail] = dax_results_detail
                                else: st.warning("Expression for AI is empty.")
                            if item_id_detail in st.session_state['dax_expressions']:
                                # ... (display AI DAX results) ...
                                dax_results_render = st.session_state['dax_expressions'][item_id_detail] 
                                recommendation_render = dax_results_render.get("recommendation", "").lower()
                                if recommendation_render == "measure": st.info("💡 **AI Recommendation:** **MEASURE**")
                                elif "calculated column" in recommendation_render: st.info("💡 **AI Recommendation:** **CALCULATED COLUMN**")
                                elif recommendation_render and recommendation_render != "error": st.info(f"💡 **AI Recommendation:** {recommendation_render.upper()}")
                                st.write("**AI Generated DAX Measure:**"); st.code(dax_results_render.get("measure", "Not provided or error."), language="dax")
                                st.write("**AI Generated DAX Calculated Column:**"); st.code(dax_results_render.get("calculated_column", "Not provided or error."), language="dax")
                                st.write("**AI Suggested Data Type (for Measure):**"); st.code(dax_results_render.get("dataType", "text"), language="text")
                        elif item_detail_data['type'] == 'expression': st.code("No expression available for this item.", language="text")
                        st.write("**Base columns (from SQL Lineage):**")
                        if item_detail_data.get('base_columns'):
                            for col_detail in item_detail_data['base_columns']: st.write(f"- `{col_detail}`")
                        else: st.write("N/A")
                        st.markdown("---"); st.write("**PBI Mapping for Individual Base Columns:**")
                        if not item_detail_data.get('base_columns'): st.caption("No base columns to show.")
                        elif not st.session_state.get('column_mappings'): st.warning("Mapping file not loaded.")
                        else:
                            for base_col_idx_detail, base_col_str_detail in enumerate(item_detail_data['base_columns']):
                                # ... (display PBI mapping for each base column) ...
                                norm_base_col_detail = normalize_column_identifier(base_col_str_detail)
                                st.markdown(f"  - **Base Column {base_col_idx_detail+1}:** `{base_col_str_detail}` <br>&nbsp;&nbsp;&nbsp;&nbsp;Normalized: `{norm_base_col_detail}`", unsafe_allow_html=True)
                                pbi_matches_for_this_base_col_detail = find_matching_powerbi_columns(base_col_str_detail, st.session_state['column_mappings'])
                                if pbi_matches_for_this_base_col_detail:
                                    for match_idx_detail, match_info_detail in enumerate(pbi_matches_for_this_base_col_detail):
                                        # ... (display match details) ...
                                        pbi_table_name_detail = match_info_detail.get('table', 'N/A'); pbi_col_name_detail = match_info_detail.get('column', 'N/A') 
                                        dax_ref_display_detail = f"'{pbi_table_name_detail}'[{pbi_col_name_detail}]" if pbi_table_name_detail != 'N/A' else "N/A"
                                        st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- PBI Target {match_idx_detail+1}: `{match_info_detail.get('powerbi_column', 'N/A')}` (DAX: `{dax_ref_display_detail}`)<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Table: `{pbi_table_name_detail}`<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Column: `{pbi_col_name_detail}`<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- (Source DB in Mapping: `{match_info_detail.get('db_column', 'N/A')}`)", unsafe_allow_html=True)
                                else: st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- *No PowerBI mapping found for this base column.*", unsafe_allow_html=True)
                                st.markdown("<br>", unsafe_allow_html=True) 

        with tab3:
            # ... (Content of PBI Mapping tab - this is extensive) ...
            st.header("Consolidated Power BI Column Mappings")
            if not st.session_state.get('column_mappings'): st.warning("Mapping file not loaded.")
            elif not st.session_state.get('mapping_results'): st.info("No SQL query analyzed yet.")
            else:
                # ... (rest of tab3 logic from your original code) ...
                mapping_filter_tab3 = st.radio("Show SQL Columns:", ["All", "Mapped Only", "Unmapped Only"], horizontal=True, key="pbi_mapping_tab_filter_tab3_revised")
                mapping_data_for_tab3 = {k: v for k, v in st.session_state['mapping_results'].items()}
                if not mapping_data_for_tab3: st.info("No mappable items found.")
                else:
                    # ... (metrics and expander logic for mappings) ...
                    total_sql_cols_tab3 = len(mapping_data_for_tab3); mapped_sql_cols_count_tab3 = sum(1 for data_tab3 in mapping_data_for_tab3.values() if data_tab3.get("is_mapped_overall")); unmapped_sql_cols_count_tab3 = total_sql_cols_tab3 - mapped_sql_cols_count_tab3
                    m_col1_tab3, m_col2_tab3, m_col3_tab3 = st.columns(3); m_col1_tab3.metric("Total SQL Items", total_sql_cols_tab3); m_col2_tab3.metric("Mapped", mapped_sql_cols_count_tab3); m_col3_tab3.metric("Unmapped", unmapped_sql_cols_count_tab3)
                    export_rows_tab3 = []
                    for sql_col_name_tab3, data_val_tab3 in mapping_data_for_tab3.items(): 
                        # ... (expander logic for each SQL item and its base column mappings) ...
                        is_overall_mapped_tab3 = data_val_tab3.get("is_mapped_overall", False); display_this_sql_col_tab3 = False
                        if mapping_filter_tab3 == "All": display_this_sql_col_tab3 = True
                        elif mapping_filter_tab3 == "Mapped Only" and is_overall_mapped_tab3: display_this_sql_col_tab3 = True
                        elif mapping_filter_tab3 == "Unmapped Only" and not is_overall_mapped_tab3: display_this_sql_col_tab3 = True
                        if display_this_sql_col_tab3:
                            expander_title_tab3 = f"SQL Item: {sql_col_name_tab3} (Type: {data_val_tab3.get('type', 'N/A')})"
                            expander_title_tab3 += " ✅ (Mapped)" if is_overall_mapped_tab3 else " ❌ (Unmapped)"
                            with st.expander(expander_title_tab3):
                                # ... (display base column mappings within expander) ...
                                pass # Placeholder for detailed mapping display
                    if export_rows_tab3: # Simplified, ensure export_rows_tab3 is populated correctly
                        export_df_tab3 = pd.DataFrame(export_rows_tab3)
                        csv_export_tab3 = export_df_tab3.to_csv(index=False).encode('utf-8')
                        st.download_button(label="Download All Mappings (CSV)", data=csv_export_tab3, file_name="pbi_column_mapping_details.csv", mime="text/csv", key="export_all_mappings_button_tab3_vis_revised" )

        with tab_filters:
            # ... (Content of Filter Conditions tab) ...
            st.header("WHERE Clause Filter Conditions Analysis")
            filter_conditions = [item for item in st.session_state['lineage_data'] if item.get('type') == 'filter_condition']
            if not filter_conditions: st.info("No WHERE clause conditions found.")
            else:
                for i, condition_data in enumerate(filter_conditions):
                    with st.expander(f"Condition {i+1}: {condition_data.get('item', 'Unknown Condition Context')}"):
                        st.write("**Source Clause:**", condition_data.get('source_clause', 'N/A'))
                        st.write("**Filter Condition SQL:**")
                        st.code(condition_data.get('filter_condition', 'N/A'), language="sql")

                        base_columns_in_filter = condition_data.get('base_columns', [])
                        st.write("**Base Columns Involved:**")
                        if not base_columns_in_filter: 
                            st.caption("No base columns identified for this filter.")
                        else:
                            for col_filter in base_columns_in_filter: 
                                st.write(f"- `{col_filter}`")
                        
                        st.markdown("---")
                        st.write("**Power BI Equivalent Filter DAX (Rule-Based Translation):**")
                        if st.session_state.get('column_mappings') and base_columns_in_filter and condition_data.get('filter_condition'):
                            pbi_eq_filter_dax, made_change_filter_dax = generate_powerbi_equivalent_formula(
                                condition_data['filter_condition'], 
                                base_columns_in_filter, 
                                st.session_state['column_mappings'],
                                st.session_state.get('resolved_base_col_to_pbi', {})
                            )
                            if made_change_filter_dax:
                                st.code(pbi_eq_filter_dax, language="dax")
                            else:
                                st.caption("Could not translate filter condition to DAX based on current mappings.")
                        elif not base_columns_in_filter:
                            st.caption("No base columns identified to attempt DAX translation.")
                        else:
                            st.caption("Translation prerequisites not met (e.g., mappings not loaded).")

                        st.markdown("---")
                        st.write("**PBI Mapping for Individual Base Columns in Filter:**")
                        if not base_columns_in_filter: 
                            st.caption("No base columns to show PBI mappings for.")
                        elif not st.session_state.get('column_mappings'): 
                            st.warning("Mapping file not loaded.")
                        else:
                            for base_col_idx_filter, base_col_str_filter in enumerate(base_columns_in_filter):
                                norm_base_col_filter = normalize_column_identifier(base_col_str_filter)
                                st.markdown(f"  - **Base Column {base_col_idx_filter+1}:** `{base_col_str_filter}` <br>&nbsp;&nbsp;&nbsp;&nbsp;Normalized: `{norm_base_col_filter}`", unsafe_allow_html=True)
                                pbi_matches_for_this_base_col_filter = find_matching_powerbi_columns(base_col_str_filter, st.session_state['column_mappings'])
                                if pbi_matches_for_this_base_col_filter:
                                    for match_idx_filter, match_info_filter in enumerate(pbi_matches_for_this_base_col_filter):
                                        pbi_table_name_filter = match_info_filter.get('table', 'N/A')
                                        pbi_col_name_filter = match_info_filter.get('column', 'N/A')
                                        dax_ref_display_filter = f"'{pbi_table_name_filter}'[{pbi_col_name_filter}]" if pbi_table_name_filter != 'N/A' else "N/A"
                                        st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- PBI Target {match_idx_filter+1}: `{match_info_filter.get('powerbi_column', 'N/A')}` (DAX: `{dax_ref_display_filter}`)<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Table: `{pbi_table_name_filter}`<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- Column: `{pbi_col_name_filter}`<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- (Source DB in Mapping: `{match_info_filter.get('db_column', 'N/A')}`)", unsafe_allow_html=True)
                                else:
                                    st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- *No PowerBI mapping found for this base column.*", unsafe_allow_html=True)
                                st.markdown("<br>", unsafe_allow_html=True)
                        st.markdown("---")
        with tab4:
            st.header("Raw Lineage Data (JSON)")
            st.json(st.session_state['lineage_data'])


def run_ai_dax_for_visual():
    """Generate AI DAX for all selected expressions in the current visual and update session state."""
    items_to_process_for_ai = []
    visual_type = st.session_state.get('visual_type', 'Matrix')

    if visual_type == "Matrix":
        for category, selected_list_key in [
            ("Row", 'visual_selected_rows'),
            ("Column", 'visual_selected_columns'),
            ("Value", 'visual_selected_values')
        ]:
            for item in st.session_state.get(selected_list_key, []):
                if item.get("type") == "expression" and item.get("pbi_expression"):
                    items_to_process_for_ai.append({
                        "label": item["label"],
                        "pbi_expression": item["pbi_expression"],
                        "category": category
                    })
    elif visual_type == "Table":
        for item in st.session_state.get('visual_selected_table_fields', []):
            if item.get("type") == "expression" and item.get("pbi_expression"):
                items_to_process_for_ai.append({
                    "label": item["label"],
                    "pbi_expression": item["pbi_expression"],
                    "category": "TableField"
                })

    # Generate DAX for each item and update session state
    for item in items_to_process_for_ai:
        label = item["label"]
        pbi_expr = item["pbi_expression"]
        category = item["category"]
        unique_key = f"{category}_{label}"
        dax_results = generate_dax_from_sql(pbi_expr)
        st.session_state.setdefault('visual_ai_dax_results', {})
        st.session_state['visual_ai_dax_results'][unique_key] = {
            "label": label,
            "input_pbi_expression": pbi_expr,
            "ai_output": dax_results,
            "category": category
        }

    # Update selected items with AI DAX (for config generation)
    if visual_type == "Matrix":
        for list_key, category in [
            ('visual_selected_rows', "Row"),
            ('visual_selected_columns', "Column"),
            ('visual_selected_values', "Value")
        ]:
            for item in st.session_state.get(list_key, []):
                if item.get("type") == "expression":
                    ai_key = f"{category}_{item['label']}"
                    ai_result = st.session_state['visual_ai_dax_results'].get(ai_key)
                    if ai_result:
                        ai_output = ai_result['ai_output']
                        if "measure" in ai_output.get("recommendation", "").lower():
                            item["ai_generated_dax"] = ai_output.get("measure")
                            item["ai_dataType"] = ai_output.get("dataType", "text")
    elif visual_type == "Table":
        for item in st.session_state.get('visual_selected_table_fields', []):
            if item.get("type") == "expression":
                ai_key = f"TableField_{item['label']}"
                ai_result = st.session_state['visual_ai_dax_results'].get(ai_key)
                if ai_result:
                    ai_output = ai_result['ai_output']
                    if "measure" in ai_output.get("recommendation", "").lower():
                        item["ai_generated_dax"] = ai_output.get("measure")
                        item["ai_dataType"] = ai_output.get("dataType", "text")



def display_visual_configuration_section():
    """Handles the entire visual configuration UI and logic."""
    if st.session_state.get('lineage_data') and st.session_state.get('visual_config_candidates'):
        st.markdown("### Advanced: Resolve Base Database Column Ambiguities")
        if 'base_col_ambiguity_choices' not in st.session_state: st.session_state['base_col_ambiguity_choices'] = {}
        
        all_base_columns_for_ambiguity = set()
        for item in st.session_state['lineage_data']:
            # Include base columns from SELECT items AND filter_conditions
            for base_col in item.get('base_columns', []): 
                all_base_columns_for_ambiguity.add(base_col)
        
        base_col_to_matches = {}
        for base_col in all_base_columns_for_ambiguity:
            matches = find_matching_powerbi_columns(base_col, st.session_state['column_mappings'])
            if matches and len(matches) > 1: base_col_to_matches[base_col] = matches
        
        ambiguity_resolved_this_run = False
        if base_col_to_matches:
            st.caption("Some base database columns have multiple Power BI mapping candidates. Please select the correct one to use for DAX generation.")
            for base_col, matches in base_col_to_matches.items():
                options = [f"'{m['table']}'[{m['column']}]" for m in matches]
                current_choice_for_base_col = st.session_state['base_col_ambiguity_choices'].get(base_col)
                
                # Ensure current_choice is valid, default to first option if not
                if current_choice_for_base_col not in options:
                    current_choice_for_base_col = options[0] if options else None

                # --- NEW: Find usages of this base_col ---
                usages = []
                for item in st.session_state['lineage_data']:
                    # SELECT columns/expressions
                    if base_col in item.get('base_columns', []):
                        if item.get('type') == 'filter_condition':
                            usages.append(f"Filter: `{item.get('filter_condition', 'N/A')}`")
                        else:
                            usages.append(f"Column: `{item.get('item', 'N/A')}` (Type: {item.get('type', 'N/A')})")
                


                if options: # Only show radio if there are options
                    chosen = st.radio(
                        f"Choose PBI mapping for base DB column `{base_col}`:", 
                        options, 
                        index=options.index(current_choice_for_base_col) if current_choice_for_base_col in options else 0, 
                        key=f"base_col_ambiguity_{base_col.replace('.', '_').replace(' ', '_')}" # Make key more robust
                    )
                    if st.session_state['base_col_ambiguity_choices'].get(base_col) != chosen:
                        st.session_state['base_col_ambiguity_choices'][base_col] = chosen
                        ambiguity_resolved_this_run = True

                    # Display usages above the radio
                    if usages:
                        st.markdown("**Used in:**<br>" + "<br>".join(usages), unsafe_allow_html=True)
        else:
            st.caption("No base column ambiguities found or all have single PBI mappings.")

        if ambiguity_resolved_this_run:
            st.session_state['visual_config_candidates'] = build_visual_candidates() # Rebuild with new resolutions
             # Re-translate filters as well
            st.session_state['translated_filter_conditions'] = [] # Clear to force re-translation
            st.rerun()

        resolved_base_col_to_pbi = {}
        for item_lineage in st.session_state['lineage_data']:
            for base_col_lineage in item_lineage.get('base_columns', []):
                if base_col_lineage not in resolved_base_col_to_pbi: # Process each base column once
                    matches_res = find_matching_powerbi_columns(base_col_lineage, st.session_state['column_mappings'])
                    resolved_label_res = st.session_state['base_col_ambiguity_choices'].get(base_col_lineage)
                    pbi_ref_res = None
                    if resolved_label_res and matches_res:
                        resolved_match = next((m_res for m_res in matches_res if f"'{m_res['table']}'[{m_res['column']}]" == resolved_label_res), None)
                        if resolved_match: pbi_ref_res = resolved_label_res
                    elif matches_res and len(matches_res) == 1: # Auto-select if only one match and no explicit choice needed/made
                        m_first = matches_res[0]; pbi_ref_res = f"'{m_first['table']}'[{m_first['column']}]"
                    elif matches_res: # Multiple matches but no choice made yet (e.g. first run), pick first as temp default
                         m_first = matches_res[0]; pbi_ref_res = f"'{m_first['table']}'[{m_first['column']}]"
                    
                    if pbi_ref_res: resolved_base_col_to_pbi[base_col_lineage] = pbi_ref_res
        st.session_state['resolved_base_col_to_pbi'] = resolved_base_col_to_pbi
        
        # Rebuild candidates if resolved_base_col_to_pbi changed significantly (e.g. first population)
        # This check might be too simple, but aims to refresh candidates once resolution is stable.
        if not st.session_state.get('visual_config_candidates_built_after_resolution', False) and resolved_base_col_to_pbi:
            st.session_state['visual_config_candidates'] = build_visual_candidates()
            st.session_state['visual_config_candidates_built_after_resolution'] = True # Mark as built
            st.session_state['translated_filter_conditions'] = [] # Clear to force re-translation with new candidates
            st.rerun()

        
        st.markdown("---")
        st.subheader("Report Name")
        report_name = st.text_input(
            "Enter a name for your Power BI report:",
            value=st.session_state.get('report_name'),
            key="report_name_input"
        )
        st.session_state['report_name'] = report_name


        st.markdown("---")
        st.subheader("Visual Configuration")
        st.session_state['visual_type'] = st.radio(
            "Select Visual Type:", 
            ["Matrix", "Table"], 
            index=["Matrix", "Table"].index(st.session_state.get('visual_type', "Matrix")), 
            key="visual_type_selector"
        )
        
        all_available_display_labels_for_visual = sorted(list(set(
            c['chosen_display_label'] for c in st.session_state.get('visual_config_candidates', []) if c.get('chosen_display_label')
        )))

        if st.session_state['visual_type'] == "Matrix":
            st.markdown("#### Configure Matrix Visual")

            selected_rows = st.multiselect(
                "Select Rows for Matrix:",
                options=all_available_display_labels_for_visual,
                default=st.session_state.get('visual_selected_rows_labels', []),
                key="matrix_rows_multiselect"
            )
            selected_columns = st.multiselect(
                "Select Columns for Matrix:",
                options=all_available_display_labels_for_visual,
                default=st.session_state.get('visual_selected_columns_labels', []),
                key="matrix_cols_multiselect"
            )
            selected_values = st.multiselect(
                "Select Values for Matrix:",
                options=all_available_display_labels_for_visual,
                default=st.session_state.get('visual_selected_values_labels', []),
                key="matrix_values_multiselect"
            )

            display_filter_selection_ui()

            if st.button("Save Matrix Selection (including filters)"):
                st.session_state['visual_selected_rows'] = enrich_selected_items(selected_rows)
                st.session_state['visual_selected_columns'] = enrich_selected_items(selected_columns)
                st.session_state['visual_selected_values'] = enrich_selected_items(selected_values)
                st.session_state['visual_selected_rows_labels'] = selected_rows
                st.session_state['visual_selected_columns_labels'] = selected_columns
                st.session_state['visual_selected_values_labels'] = selected_values
                # Filter selections are already updated in session state by the checkboxes directly
                st.success("Matrix selection and filters saved!")
                st.rerun() # Rerun to reflect saved state or update dependent UI

            # Display current matrix configuration (your existing logic)
            st.markdown("##### Current Matrix Configuration:")
            if st.session_state.get('visual_selected_rows') or \
               st.session_state.get('visual_selected_columns') or \
               st.session_state.get('visual_selected_values')or \
               st.session_state.get('visual_selected_filters_dax'):
                
                st.write("**Rows:**")
                if st.session_state.get('visual_selected_rows'):
                    st.json(st.session_state['visual_selected_rows'])
                else: 
                    st.caption("  (None selected)")

                st.write("**Columns:**")
                if st.session_state.get('visual_selected_columns'):
                    st.json(st.session_state['visual_selected_columns'])
                else: 
                    st.caption("  (None selected)")

                st.write("**Values:**")
                if st.session_state.get('visual_selected_values'):
                    st.json(st.session_state['visual_selected_values'])
                else: 
                    st.caption("  (None selected)")

                st.write("**Filters:**")
                selected_filters_dax_strings = st.session_state.get('visual_selected_filters_dax', [])
                if selected_filters_dax_strings:
                    parsed_filters_for_display = []
                    for f_dax_str in selected_filters_dax_strings:
                        parsed_filter = parse_dax_filter_for_display(f_dax_str)
                        parsed_filters_for_display.append(parsed_filter)
                    st.json(parsed_filters_for_display)
                else:
                    st.caption("  (None selected)")
            else:
                st.caption("No items selected for the matrix.")
            
            # st.markdown("---"); st.subheader("AI DAX Generation for Selected Matrix Items")
            # items_to_process_for_ai = []
            # for category, selected_list_key in [
            #     ("Row", 'visual_selected_rows'), 
            #     ("Column", 'visual_selected_columns'), 
            #     ("Value", 'visual_selected_values')]:
            #     if selected_list_key in st.session_state:
            #         for item_dict_ai in st.session_state[selected_list_key]:
            #             if item_dict_ai.get("type") == "expression" and item_dict_ai.get("pbi_expression"):
            #                 items_to_process_for_ai.append({
            #                     "label": item_dict_ai["label"],
            #                     "pbi_expression": item_dict_ai["pbi_expression"],
            #                     "category": category
            #                 })
            
            # if not items_to_process_for_ai:
            #     st.caption("No expressions selected in Rows, Columns, or Values to generate DAX for.")
            
            # if items_to_process_for_ai and st.button("Generate DAX with AI for Selected Matrix Items", key="ai_dax_matrix_btn_main"):
            #     with st.spinner(f"Generating DAX for {len(items_to_process_for_ai)} expression(s)..."):
            #         for item_to_gen in items_to_process_for_ai:
            #             label = item_to_gen["label"]
            #             pbi_expr = item_to_gen["pbi_expression"]
            #             category = item_to_gen["category"]
            #             unique_key = f"{category}_{label}" # Consistent key

            #             dax_results = generate_dax_from_sql(pbi_expr)
            #             st.session_state['visual_ai_dax_results'][unique_key] = {
            #                 "label": label,
            #                 "input_pbi_expression": pbi_expr,
            #                 "ai_output": dax_results,
            #                 "category": category
            #             }
                
            #     # Logic to update the main selected items with AI DAX (measure recommendation)
            #     overall_config_updated_by_ai = False
            #     for list_key_str_ai, category_name_str_ai in [
            #         ('visual_selected_rows', "Row"),
            #         ('visual_selected_columns', "Column"),
            #         ('visual_selected_values', "Value")
            #     ]:
            #         if list_key_str_ai in st.session_state:
            #             current_list_in_state_ai = st.session_state[list_key_str_ai]
            #             for item_dict_idx_ai in range(len(current_list_in_state_ai)):
            #                 item_dict_ai_update = current_list_in_state_ai[item_dict_idx_ai]
            #                 if item_dict_ai_update.get("type") == "expression":
            #                     ai_result_lookup_key = f"{category_name_str_ai}_{item_dict_ai_update['label']}"
            #                     had_previous_ai_dax = "ai_generated_dax" in item_dict_ai_update
            #                     current_item_modified_by_ai = False

            #                     if ai_result_lookup_key in st.session_state['visual_ai_dax_results']:
            #                         ai_result_data = st.session_state['visual_ai_dax_results'][ai_result_lookup_key]
            #                         ai_output_data = ai_result_data['ai_output']
            #                         recommendation_data = ai_output_data.get("recommendation", "").lower()
                                    
            #                         if "measure" in recommendation_data:
            #                             measure_dax_from_ai = ai_output_data.get("measure")
            #                             data_type_from_ai = ai_output_data.get("dataType", "text")
            #                             if measure_dax_from_ai and not measure_dax_from_ai.startswith("Error:") and measure_dax_from_ai != "Not provided or error.":
            #                                 item_dict_ai_update["ai_generated_dax"] = measure_dax_from_ai
            #                                 item_dict_ai_update["ai_dataType"] = data_type_from_ai
            #                                 current_item_modified_by_ai = True
                                
            #                     if not current_item_modified_by_ai and had_previous_ai_dax:
            #                         if "ai_generated_dax" in item_dict_ai_update: del item_dict_ai_update["ai_generated_dax"]
            #                         if "ai_dataType" in item_dict_ai_update: del item_dict_ai_update["ai_dataType"]
            #                         current_item_modified_by_ai = True
                                
            #                     if current_item_modified_by_ai:
            #                         overall_config_updated_by_ai = True
                
            #     st.success(f"AI DAX generation complete for {len(st.session_state['visual_ai_dax_results'])} items.")
            #     if overall_config_updated_by_ai:
            #         st.rerun() # Rerun to reflect the ai_generated_dax in the config file generation

            # if st.session_state.get('visual_ai_dax_results'):
            #     st.markdown("##### DAX Generation Results (for selected expressions):") # Changed
            #     for unique_key_disp, result_data_disp in st.session_state['visual_ai_dax_results'].items():
            #         with st.expander(f"{result_data_disp['label']}"): # Changed
            #             st.write(f"**Input PBI Expression (Rule-Based):**")
            #             st.code(result_data_disp.get('input_pbi_expression', 'N/A'), language="dax")
                        
            #             ai_output_disp = result_data_disp.get('ai_output', {}) # Internal variable name can remain
            #             recommendation_disp = ai_output_disp.get("recommendation", "").lower()
                        
            #             if "measure" in recommendation_disp: # Prioritize if "measure" is in the recommendation string
            #                 st.info("💡 **Recommendation:** **MEASURE**") # Changed
            #                 st.write("**Generated DAX Measure:**") # Changed
            #                 st.code(ai_output_disp.get("measure", "Not provided or error."), language="dax")
            #                 st.write("**Suggested Data Type (for Measure):**") # Changed
            #                 st.code(ai_output_disp.get("dataType", "text"), language="text")
            #             elif "calculated column" in recommendation_disp:
            #                 st.info("💡 **Recommendation:** **CALCULATED COLUMN**") # Changed
            #                 st.warning("Calculated Column is not directly used for visual measures. The generated DAX is for reference.") # Changed
            #                 st.write("**Generated DAX Calculated Column (for reference):**") # Changed
            #                 st.code(ai_output_disp.get("calculated_column", "Not provided or error."), language="dax")
            #             elif recommendation_disp and recommendation_disp != "error": # Other valid recommendations
            #                 st.info(f"💡 **Recommendation:** {recommendation_disp.upper()}") # Changed
            #                 st.write("**Generated DAX Measure:**") # Changed
            #                 st.code(ai_output_disp.get("measure", "Not provided or error."), language="dax")
            #                 st.write("**Generated DAX Calculated Column:**") # Changed
            #                 st.code(ai_output_disp.get("calculated_column", "Not provided or error."), language="dax")
            #                 st.write("**Suggested Data Type (for Measure):**") # Changed
            #                 st.code(ai_output_disp.get("dataType", "text"), language="text")
            #             else: # Error or unknown recommendation
            #                 st.error(f"Recommendation: {recommendation_disp if recommendation_disp else 'Not available'}") # Changed
            #                 st.write("**Generated DAX Measure (Attempt):**") # Changed
            #                 st.code(ai_output_disp.get("measure", "Not provided or error."), language="dax")
            #                 st.write("**Generated DAX Calculated Column (Attempt):**") # Changed
            #                 st.code(ai_output_disp.get("calculated_column", "Not provided or error."), language="dax")
            #                 st.write("**Suggested Data Type (Attempt):**") # Changed
            #                 st.code(ai_output_disp.get("dataType", "text"), language="text")

        elif st.session_state['visual_type'] == "Table":
            st.markdown("#### Configure Table Visual")
            
            # Deduplicate table column labels
            table_column_labels = sorted(list(set(
                c['chosen_display_label']
                for c in st.session_state.get('visual_config_candidates', [])
                if c.get('chosen_display_label')
            )))
            selected_table_fields = st.multiselect(
                "Select Columns/Expressions for Table:",
                options=table_column_labels,
                default=st.session_state.get('visual_selected_table_fields_labels', []),
                key="table_fields_multiselect"
            )


            display_filter_selection_ui()
            # Save selection
            if st.button("Save Table Selection (including filters)"):
                st.session_state['visual_selected_table_fields'] = enrich_selected_items(selected_table_fields)
                st.session_state['visual_selected_table_fields_labels'] = selected_table_fields
                st.success("Table selection and filters saved!")
                st.rerun()

            # Show current config
            st.markdown("##### Current Table Configuration:")
            if st.session_state.get('visual_selected_table_fields') or st.session_state.get('visual_selected_filters_dax'):
                st.write("**Fields:**")
                if st.session_state.get('visual_selected_table_fields'):
                    st.json(st.session_state['visual_selected_table_fields'])
                else:
                    st.caption("  (None selected)")
                st.write("**Filters:**")
                selected_filters_dax_strings = st.session_state.get('visual_selected_filters_dax', [])
                if selected_filters_dax_strings:
                    parsed_filters_for_display = []
                    for f_dax_str in selected_filters_dax_strings:
                        parsed_filter = parse_dax_filter_for_display(f_dax_str)
                        parsed_filters_for_display.append(parsed_filter)
                    st.json(parsed_filters_for_display)
                else:
                    st.caption("  (None selected)")
            else:
                st.caption("No items selected for the table.")

            # # --- AI DAX generation for selected expressions (replicate Matrix logic) ---
            # st.markdown("---"); st.subheader("AI DAX Generation for Selected Table Items")
            # items_to_process_for_ai = []
            # for item_dict_ai in st.session_state.get('visual_selected_table_fields', []):
            #     if item_dict_ai.get("type") == "expression" and item_dict_ai.get("pbi_expression"):
            #         items_to_process_for_ai.append({
            #             "label": item_dict_ai["label"],
            #             "pbi_expression": item_dict_ai["pbi_expression"],
            #             "category": "TableField"
            #         })
            # if not items_to_process_for_ai:
            #     st.caption("No expressions selected in Table fields to generate DAX for.")
            # if items_to_process_for_ai and st.button("Generate DAX with AI for Selected Table Items", key="ai_dax_table_btn_main"):
            #     with st.spinner(f"Generating DAX for {len(items_to_process_for_ai)} expression(s)..."):
            #         for item_to_gen in items_to_process_for_ai:
            #             label = item_to_gen["label"]
            #             pbi_expr = item_to_gen["pbi_expression"]
            #             category = item_to_gen["category"]
            #             unique_key = f"{category}_{label}"
            #             dax_results = generate_dax_from_sql(pbi_expr)
            #             st.session_state['visual_ai_dax_results'][unique_key] = {
            #                 "label": label,
            #                 "input_pbi_expression": pbi_expr,
            #                 "ai_output": dax_results,
            #                 "category": category
            #             }

            #     # Update the selected table fields with AI DAX (measure recommendation)
            #     overall_config_updated_by_ai = False
            #     current_list_in_state_ai = st.session_state['visual_selected_table_fields']
            #     for item_dict_idx_ai in range(len(current_list_in_state_ai)):
            #         item_dict_ai_update = current_list_in_state_ai[item_dict_idx_ai]
            #         if item_dict_ai_update.get("type") == "expression":
            #             ai_result_lookup_key = f"TableField_{item_dict_ai_update['label']}"
            #             had_previous_ai_dax = "ai_generated_dax" in item_dict_ai_update
            #             current_item_modified_by_ai = False

            #             if ai_result_lookup_key in st.session_state['visual_ai_dax_results']:
            #                 ai_result_data = st.session_state['visual_ai_dax_results'][ai_result_lookup_key]
            #                 ai_output_data = ai_result_data['ai_output']
            #                 recommendation_data = ai_output_data.get("recommendation", "").lower()
            #                 if "measure" in recommendation_data:
            #                     measure_dax_from_ai = ai_output_data.get("measure")
            #                     data_type_from_ai = ai_output_data.get("dataType", "text")
            #                     if measure_dax_from_ai and not measure_dax_from_ai.startswith("Error:") and measure_dax_from_ai != "Not provided or error.":
            #                         item_dict_ai_update["ai_generated_dax"] = measure_dax_from_ai
            #                         item_dict_ai_update["ai_dataType"] = data_type_from_ai
            #                         current_item_modified_by_ai = True

            #             if not current_item_modified_by_ai and had_previous_ai_dax:
            #                 if "ai_generated_dax" in item_dict_ai_update: del item_dict_ai_update["ai_generated_dax"]
            #                 if "ai_dataType" in item_dict_ai_update: del item_dict_ai_update["ai_dataType"]
            #                 current_item_modified_by_ai = True

            #             if current_item_modified_by_ai:
            #                 overall_config_updated_by_ai = True

            #     st.success(f"AI DAX generation complete for {len(st.session_state['visual_ai_dax_results'])} items.")
            #     if overall_config_updated_by_ai:
            #         st.rerun() # Rerun to reflect the ai_generated_dax in the config file generation

            # # Show DAX Generation Results for Table
            # if st.session_state.get('visual_ai_dax_results'):
            #     st.markdown("##### DAX Generation Results (for selected expressions):")
            #     for unique_key_disp, result_data_disp in st.session_state['visual_ai_dax_results'].items():
            #         if result_data_disp.get("category") != "TableField":
            #             continue
            #         with st.expander(f"{result_data_disp['label']}"):
            #             st.write(f"**Input PBI Expression (Rule-Based):**")
            #             st.code(result_data_disp.get('input_pbi_expression', 'N/A'), language="dax")
            #             ai_output_disp = result_data_disp.get('ai_output', {})
            #             recommendation_disp = ai_output_disp.get("recommendation", "").lower()
            #             if "measure" in recommendation_disp:
            #                 st.info("💡 **Recommendation:** **MEASURE**")
            #                 st.write("**Generated DAX Measure:**")
            #                 st.code(ai_output_disp.get("measure", "Not provided or error."), language="dax")
            #                 st.write("**Suggested Data Type (for Measure):**")
            #                 st.code(ai_output_disp.get("dataType", "text"), language="text")
            #             elif "calculated column" in recommendation_disp:
            #                 st.info("💡 **Recommendation:** **CALCULATED COLUMN**")
            #                 st.warning("Calculated Column is not directly used for visual measures. The generated DAX is for reference.")
            #                 st.write("**Generated DAX Calculated Column (for reference):**")
            #                 st.code(ai_output_disp.get("calculated_column", "Not provided or error."), language="dax")
            #             elif recommendation_disp and recommendation_disp != "error":
            #                 st.info(f"💡 **Recommendation:** {recommendation_disp.upper()}")
            #                 st.write("**Generated DAX Measure:**")
            #                 st.code(ai_output_disp.get("measure", "Not provided or error."), language="dax")
            #                 st.write("**Generated DAX Calculated Column:**")
            #                 st.code(ai_output_disp.get("calculated_column", "Not provided or error."), language="dax")
            #                 st.write("**Suggested Data Type (for Measure):**")
            #                 st.code(ai_output_disp.get("dataType", "text"), language="text")
            #             else:
            #                 st.error(f"Recommendation: {recommendation_disp if recommendation_disp else 'Not available'}")
            #                 st.write("**Generated DAX Measure (Attempt):**")
            #                 st.code(ai_output_disp.get("measure", "Not provided or error."), language="dax")
            #                 st.write("**Generated DAX Calculated Column (Attempt):**")
            #                 st.code(ai_output_disp.get("calculated_column", "Not provided or error."), language="dax")
            #                 st.write("**Suggested Data Type (Attempt):**")
            #                 st.code(ai_output_disp.get("dataType", "text"), language="text")





def display_filter_selection_ui():
    """Display filter selection UI and update session state."""
    st.markdown("##### Select Filters for Visual:")
    if not st.session_state.get('translated_filter_conditions'):
        raw_filters = [item for item in st.session_state.get('lineage_data', []) if item.get('type') == 'filter_condition']
        temp_translated_filters = []
        for i, f_item in enumerate(raw_filters):
            sql_expr = f_item.get('filter_condition')
            base_cols = f_item.get('base_columns', [])
            if sql_expr:
                pbi_dax, _ = generate_powerbi_equivalent_formula(
                    sql_expr, base_cols, 
                    st.session_state['column_mappings'], 
                    st.session_state['resolved_base_col_to_pbi']
                )
                temp_translated_filters.append({'id': f"filter_{i}_{hash(sql_expr)}", 'sql': sql_expr, 'pbi_dax': pbi_dax})
        st.session_state['translated_filter_conditions'] = temp_translated_filters
        st.session_state['visual_selected_filters_dax'] = [tf['pbi_dax'] for tf in temp_translated_filters if tf['pbi_dax']]

    if not st.session_state['translated_filter_conditions']:
        st.caption("No filter conditions found in the SQL query or they could not be translated.")
    else:
        current_selected_filters = list(st.session_state['visual_selected_filters_dax'])
        for filter_item in st.session_state['translated_filter_conditions']:
            pbi_dax = filter_item['pbi_dax']
            filter_id = filter_item['id']
            if not pbi_dax: continue
            is_checked = st.checkbox(
                f"{pbi_dax}", 
                value=(pbi_dax in current_selected_filters), 
                key=f"filter_cb_{filter_id}"
            )
            if is_checked and pbi_dax not in current_selected_filters:
                current_selected_filters.append(pbi_dax)
            elif not is_checked and pbi_dax in current_selected_filters:
                current_selected_filters.remove(pbi_dax)
        if st.session_state['visual_selected_filters_dax'] != current_selected_filters:
            st.session_state['visual_selected_filters_dax'] = current_selected_filters


def display_pbi_automation_config_section():
    """Handles the PBI Automation config.yaml generation and script execution."""

    if  not st.session_state.get('lineage_data') and not st.session_state.get('visual_config_candidates'): return None

    st.markdown("---")
    st.header("PBI Automation `config.yaml` Generation")

    try:
        new_config = {}
        # --- Hardcoded Static Fields ---
        report_name = st.session_state.get('report_name', "My Report")
        
        new_config['projectName'] = report_name
        new_config['dataset'] = { 
            "connection": { 
                "connectionString": CONNECTION_STRING,
                "database": DATABASE_NAME
            }, 
            "modelName": "EU Order to Cash (Ad-Hoc)"
        }
        new_config['report'] = { 
            'title': FlowDict({"text":report_name}), 
            'data_refresh': FlowDict({"table": "Date Refresh Table", "column": "UPDATED_DATE"})
        }
        # --- Generate Measures (Dynamic) ---
        generated_measures = []
        measure_candidate_lists = [
            st.session_state.get('visual_selected_rows', []),
            st.session_state.get('visual_selected_columns', []),
            st.session_state.get('visual_selected_values', []),
            st.session_state.get('visual_selected_table_fields', []) 
        ]
        processed_measure_labels = set() 
        for item_list in measure_candidate_lists:
            for item in item_list:
                if item.get("type") == "expression" and item.get("label") not in processed_measure_labels:
                    base_measure_name = item["label"]
                    measure_name_for_definition = base_measure_name
                    if not base_measure_name.endswith(" Measure"):
                        measure_name_for_definition = f"{base_measure_name} Measure"
                    dax_expression = item.get("pbi_expression") 
                    data_type = "text" 
                    if "ai_generated_dax" in item and item.get("ai_generated_dax"):
                        dax_expression = item["ai_generated_dax"]
                        data_type = item.get("ai_dataType", "text")
                    measure_table = item.get("pbi_table", "_Measures")
                    generated_measures.append(FlowDict({
                        "name": measure_name_for_definition,
                        "table": measure_table, 
                        "expression": dax_expression,
                        "dataType": data_type
                    }))
                    processed_measure_labels.add(base_measure_name)
        new_config['report']['measures'] = generated_measures
        visuals = []
        # --- Matrix Visual ---
        if st.session_state.get('visual_type', 'Matrix') == "Matrix":
            # ...existing matrix config code...
            matrix_rows_config = []
            for item in st.session_state.get('visual_selected_rows', []):
                row_item_config = {"name": item["label"], "table": item.get("pbi_table", "UnknownTable"), "type": "Column"}
                if item.get("type") == "base" and item.get("pbi_table") and item.get("pbi_column"):
                    row_item_config["name"] = item["pbi_column"]
                    row_item_config["table"] = item["pbi_table"]
                elif item.get("type") == "expression":
                    row_item_config["name"] = item["label"]
                    row_item_config["table"] = item.get("pbi_table", "_Measures")
                matrix_rows_config.append(FlowDict(row_item_config))
            matrix_columns_config = []
            for item in st.session_state.get('visual_selected_columns', []):
                column_item_config = {"name": item["label"], "table": item.get("pbi_table", "UnknownTable"), "type": "Column"}
                if item.get("type") == "base" and item.get("pbi_table") and item.get("pbi_column"):
                    column_item_config["name"] = item["pbi_column"]
                    column_item_config["table"] = item["pbi_table"]
                elif item.get("type") == "expression":
                    column_item_config["name"] = item["label"]
                    column_item_config["table"] = item.get("pbi_table", "_Measures")
                matrix_columns_config.append(FlowDict(column_item_config))
            matrix_values_config = []
            for item in st.session_state.get('visual_selected_values', []):
                if item.get("type") == "expression":
                    base_value_name = item["label"]
                    value_name_for_visual = base_value_name
                    if not base_value_name.endswith(" Measure"):
                        value_name_for_visual = f"{base_value_name} Measure"
                    measure_table_ref = item.get("pbi_table", "_Measures")
                    defined_measure = next((m for m in generated_measures if m["name"] == value_name_for_visual), None)
                    if defined_measure:
                        measure_table_ref = defined_measure["table"]
                    matrix_values_config.append(FlowDict({
                        "name": value_name_for_visual, 
                        "table": measure_table_ref, 
                        "type": "Measure"
                    }))
            # --- Generate Filters (Dynamic) ---
            matrix_filters_config = []
            selected_filter_dax_expressions = st.session_state.get('visual_selected_filters_dax', [])
            for pbi_dax_filter_str in selected_filter_dax_expressions:
                parsed_filter_structure = parse_simple_dax_filter(pbi_dax_filter_str, generated_measures)
                if parsed_filter_structure:
                    matrix_filters_config.append(FlowDict(parsed_filter_structure))
                else:
                    st.warning(f"Could not parse filter DAX: '{pbi_dax_filter_str}'. This filter will be skipped in config.yaml. Consider simplifying the DAX or extending parsing capabilities if this filter is required.")
            matrix_visual_definition = {
                "type": "matrix",
                "position": FlowDict({"x": 28.8, "y": 100, "width": 1220, "height": 800}),
                "rows": matrix_rows_config,
                "columns": matrix_columns_config,
                "values": matrix_values_config,
                "filters": matrix_filters_config
            }
            visuals.append(matrix_visual_definition)
        # --- Table Visual ---
        elif st.session_state.get('visual_type') == "Table":
            table_fields_config = []
            for item in st.session_state.get('visual_selected_table_fields', []):
                field_item_config = {"name": item["label"], "table": item.get("pbi_table", "UnknownTable"), "type": "Column"}
                if item.get("type") == "base" and item.get("pbi_table") and item.get("pbi_column"):
                    field_item_config["name"] = item["pbi_column"]
                    field_item_config["table"] = item["pbi_table"]
                    field_item_config["type"] = "Column"
                elif item.get("type") == "expression":
                    base_value_name = item["label"]
                    value_name_for_visual = base_value_name
                    if not base_value_name.endswith(" Measure"):
                        value_name_for_visual = f"{base_value_name} Measure"
                    measure_table_ref = item.get("pbi_table", "_Measures")
                    defined_measure = next((m for m in generated_measures if m["name"] == value_name_for_visual), None)
                    if defined_measure:
                        measure_table_ref = defined_measure["table"]
                    field_item_config["name"] = value_name_for_visual
                    field_item_config["table"] = measure_table_ref
                    field_item_config["type"] = "Measure"
                table_fields_config.append(FlowDict(field_item_config))
            table_filters_config = []
            selected_filter_dax_expressions = st.session_state.get('visual_selected_filters_dax', [])
            for pbi_dax_filter_str in selected_filter_dax_expressions:
                parsed_filter_structure = parse_simple_dax_filter(pbi_dax_filter_str, generated_measures)
                if parsed_filter_structure:
                    table_filters_config.append(FlowDict(parsed_filter_structure))
                else:
                    st.warning(f"Could not parse filter DAX: '{pbi_dax_filter_str}'. This filter will be skipped in config.yaml for the table visual.")
            table_visual_definition = {
                "type": "table",
                "position": FlowDict({"x": 28.8, "y": 100, "width": 1220, "height": 800}),
                "fields": table_fields_config,
                "filters": table_filters_config
            }
            visuals.append(table_visual_definition)
        new_config['report']['visuals'] = visuals
        yaml_string_io = StringIO()
        yaml.dump(new_config, yaml_string_io, Dumper=CustomDumper, sort_keys=False, indent=2, allow_unicode=True)
        generated_yaml_str = yaml_string_io.getvalue()
        st.session_state['generated_pbi_config'] = generated_yaml_str.strip()
        st.success("PBI Automation config.yaml content generated successfully!")
        # --- Save config locally and run PBI Automation ---
        local_config_filename = "config.yaml"
        app_dir = Path(__file__).parent.parent # Assuming this script is in 'src'
        local_config_path = app_dir / local_config_filename # This is in the Streamlit app's directory
        with open(local_config_path, 'w', encoding='utf-8') as f:
            f.write(st.session_state['generated_pbi_config'])
        st.info(f"Generated `config.yaml` saved to: {local_config_path}") # Updated message
        
        # --- PBI Automation script execution logic (Placeholder) ---
        # This assumes your PBI Automation script is in a 'PBI Automation' directory
        # relative to this script's location, and it's called 'main.py'.
        # Adjust the path and command as necessary.
        pbi_automation_script_path = Path(r"C:\Users\NileshPhapale\Desktop\PBI Automation\main.py")
        pbi_automation_project_dir = Path(r"C:\Users\NileshPhapale\Desktop\PBI Automation") # Still needed for cwd
        python_executable = r"C:\Users\NileshPhapale\Desktop\PBI Automation\.venv\Scripts\python.exe" # Specific python executable
        
        if pbi_automation_script_path.exists():
            st.info(f"Attempting to run PBI Automation script: {pbi_automation_script_path}")
            
            try:
                # Construct the command
                command = [
                    python_executable, 
                    str(pbi_automation_script_path),
                    "--config", 
                    str(local_config_path.resolve()) # Pass absolute path to the config file
                ]
                st.info(f"Executing command: {' '.join(command)}") # Log the command being run
                process = subprocess.Popen(
                    command, 
                    cwd=str(pbi_automation_project_dir), # Script still runs from its own directory
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding='utf-8' 
                )
                stdout, stderr = process.communicate(timeout=300) 
                if process.returncode == 0:
                    st.success("PBI Automation script executed successfully!")
                    if stdout: st.text_area("Script Output:", value=stdout, height=200)
                    if stderr: st.text_area("Script Error Output (if any):", value=stderr, height=100) # Show stderr even on success
                else:
                    st.error(f"PBI Automation script execution failed with code {process.returncode}.")
                    if stdout: st.text_area("Script Output:", value=stdout, height=150)
                    if stderr: st.text_area("Script Error Output:", value=stderr, height=150)
            except subprocess.TimeoutExpired:
                st.error("PBI Automation script timed out.")
            except FileNotFoundError:
                st.error(f"Python executable not found at '{python_executable}'. Please ensure the path is correct.")
            except Exception as sub_e:
                st.error(f"Error running PBI Automation script: {sub_e}")
                st.exception(sub_e)
        else:
            st.warning(f"PBI Automation script not found at: {pbi_automation_script_path}. Skipping execution.")
    except Exception as e:
        st.error(f"An unexpected error occurred during config generation or script execution: {e}")
        st.exception(e) 
    
    if st.session_state.get('generated_pbi_config'):
        st.subheader("Generated `config.yaml` Content (for review)")
        st.code(st.session_state['generated_pbi_config'], language="yaml")
        st.download_button(label="Download Generated config.yaml", data=st.session_state['generated_pbi_config'], file_name="generated_config.yaml", mime="text/yaml")

