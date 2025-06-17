import streamlit as st

from src.mapping import load_column_mappings


def initialize_session_state():
    """Initializes all necessary session state variables."""
    if 'sql_query' not in st.session_state:
        st.session_state['sql_query'] = ""
    if 'lineage_data' not in st.session_state:
        st.session_state['lineage_data'] = None
    if 'all_types' not in st.session_state:
        st.session_state['all_types'] = []
    if 'dax_expressions' not in st.session_state:
        st.session_state['dax_expressions'] = {}
    if 'column_mappings' not in st.session_state:
        st.session_state['column_mappings'] = load_column_mappings()
    if 'mapping_results' not in st.session_state:
        st.session_state['mapping_results'] = None
    if 'base_col_ambiguity_choices' not in st.session_state:
        st.session_state['base_col_ambiguity_choices'] = {}
    if 'visual_selected_values' not in st.session_state:
        st.session_state['visual_selected_values'] = []
    if 'visual_ai_dax_results' not in st.session_state:
        st.session_state['visual_ai_dax_results'] = {}
    if 'visual_type' not in st.session_state:
        st.session_state['visual_type'] = "Matrix"
    if 'visual_config_candidates' not in st.session_state:
        st.session_state['visual_config_candidates'] = []
    if 'visual_ambiguity_choices' not in st.session_state:
        st.session_state['visual_ambiguity_choices'] = {}
    if 'visual_selected_rows' not in st.session_state:
        st.session_state['visual_selected_rows'] = []
    if 'visual_selected_columns' not in st.session_state:
        st.session_state['visual_selected_columns'] = []
    if 'generated_pbi_config' not in st.session_state:
        st.session_state['generated_pbi_config'] = None
    if 'resolved_base_col_to_pbi' not in st.session_state:
        st.session_state['resolved_base_col_to_pbi'] = {}
    if 'translated_filter_conditions' not in st.session_state:
        st.session_state['translated_filter_conditions'] = [] # For storing {sql, pbi_dax, id}
    if 'visual_selected_filters_dax' not in st.session_state:
        st.session_state['visual_selected_filters_dax'] = [] 
