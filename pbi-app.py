import streamlit as st

from src.lineage import perform_sql_analysis
from src.session import initialize_session_state
from src.ui import display_sidebar, display_query_input_area, display_analysis_results_tabs, \
    display_visual_configuration_section, display_pbi_automation_config_section


def main():
    st.set_page_config(page_title="SQL to Power BI Mapper", page_icon="📊", layout="wide")
    
    initialize_session_state()
    
    st.title("SQL to Power BI Column Mapper & Visual Configurator") # Updated title
    st.markdown("""
    This tool analyzes SQL queries, maps columns to Power BI, helps configure visuals, 
    and generates configuration for PBI Automation.
    """)

    display_sidebar()
    sql_query, analyze_button_pressed = display_query_input_area()

    if analyze_button_pressed and sql_query.strip():
        perform_sql_analysis(sql_query)
        # After analysis, mapping_results and visual_config_candidates are populated
        # No explicit call to build_visual_candidates here as perform_sql_analysis handles it.

    display_analysis_results_tabs()
    display_visual_configuration_section() # This will handle its own conditions for display
    display_pbi_automation_config_section() # This will handle its own conditions for display



if __name__ == "__main__":
    main()