import streamlit as st

from src.sql_pbi.lineage import perform_sql_analysis
from src.sql_pbi.session import initialize_session_state
from src.sql_pbi.ui import display_sidebar, display_query_input_area, display_analysis_results_tabs, \
    display_visual_configuration_section, display_pbi_automation_config_section, run_ai_dax_for_visual

from dotenv import load_dotenv
load_dotenv(dotenv_path='.env')

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
    display_visual_configuration_section() 
    
    # --- Implicit AI DAX and config generation workflow ---
    if st.session_state.get('visual_selected_rows') or st.session_state.get('visual_selected_table_fields') or st.session_state.get('visual_selected_filters_dax'):
        if st.button("Build Report", key="build_report_btn"):
            st.session_state['build_report_triggered'] = True
            st.session_state['build_report_phase'] = "dax"
            st.rerun()

        if st.session_state.get('build_report_triggered'):
            if st.session_state['build_report_phase'] == "dax":
                with st.spinner("Generating DAX with AI..."):
                    run_ai_dax_for_visual()  # <-- This should update session state with AI DAX results
                st.session_state['build_report_phase'] = "config"
                st.rerun()
            elif st.session_state['build_report_phase'] == "config":
                with st.spinner("Generating config and building report..."):
                    display_pbi_automation_config_section()
                st.session_state['build_report_triggered'] = False
                st.session_state['build_report_phase'] = None


    
    # display_pbi_automation_config_section() # This will handle its own conditions for display



if __name__ == "__main__":
    main()