import streamlit as st

from src.utils.cog_report_parser import extract_cognos_report_info
from src.xml_pbi.dax import generate_dax_for_measure

from dotenv import load_dotenv

from src.xml_pbi.utils import load_all_mappings
from src.xml_pbi.mapping import map_cognos_to_db, find_pbi_mappings
from src.xml_pbi.ui import (
    display_structured_data,
    display_pbi_mappings,
    resolve_ambiguities,
    configure_visuals,
    save_visual_configuration
)
from src.xml_pbi.automation import generate_and_run_pbi_automation

load_dotenv(dotenv_path='.env')


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
    if 'visual_configs' not in st.session_state:
        st.session_state.visual_configs = {}
    if 'measure_ai_dax_results' not in st.session_state:
        st.session_state.measure_ai_dax_results = {}

    xml_input = st.text_area("Paste XML content here", height=300, placeholder="<report>...</report>")

    if st.button("Analyze and Find All Mappings"):

        for key in list(st.session_state.keys()):
            del st.session_state[key]
        # Reset choices on new analysis
        st.session_state.mapped_data = None
        st.session_state.pbi_mappings = None
        st.session_state.ambiguity_choices = {}
        st.session_state.visual_configs = {}
        st.session_state.measure_ai_dax_results = {}
        st.session_state.generated_pbi_config = None 


        if xml_input:
            try:
                report_data = extract_cognos_report_info(xml_input)
                if not report_data:
                    st.error("Could not extract information from the XML.")
                    st.session_state.mapped_data = None
                    st.session_state.pbi_mappings = None
                else:
                    all_mappings = load_all_mappings('data/column_mappings.json')
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

        with tab2:
            st.json(st.session_state.mapped_data)

        
        if st.session_state.pbi_mappings is not None:

            old_ambiguity_choices = st.session_state.ambiguity_choices.copy()
            # The 'display_pbi_mappings' function is no longer needed and has been removed.
            # The 'resolve_ambiguities' function now handles all display and resolution logic.
            resolve_ambiguities(st.session_state.pbi_mappings)


            if old_ambiguity_choices != st.session_state.ambiguity_choices:
                st.session_state.visual_configs = {} # Reset the visual configuration
                st.rerun() # Rerun to rebuild the UI with a clean state
            # This function populates st.session_state.visual_configs on every interaction
            configure_visuals(st.session_state.mapped_data, st.session_state.ambiguity_choices)
            # --- RESTRUCTURED UI FLOW ---
            if st.button("Save Visual Configuration"):
                save_visual_configuration() # This will save the state and rerun the script
            if st.button("Generate DAX for Measures"):
                if not st.session_state.get('visual_configs'):
                    st.warning("Please save a visual configuration before generating DAX.")
                else:
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
                        ai_results_cache = {}
                        with st.spinner(f"🤖 Generating DAX for {len(items_to_process)} measure(s)..."):
                            for unique_key, task in items_to_process:
                                ai_results = generate_dax_for_measure(task['pbi_expression'], task['aggregation'])
                                ai_results['input_expression'] = task['pbi_expression']
                                ai_results_cache[unique_key] = ai_results
                        
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
                        
                        st.session_state.measure_ai_dax_results = ai_results_cache
                        st.success("✅ AI DAX generation complete. Configuration has been updated.")
                        if config_updated:
                            st.rerun()
            
            if st.session_state.measure_ai_dax_results:
                st.info("The following DAX measures have been generated and applied to the configuration above.")
                for key, result in st.session_state.measure_ai_dax_results.items():
                    input_expr = result.get('input_expression', 'Unknown Measure')
                    dax_measure = result.get('measure', 'Error: Not generated.')
                    with st.expander(f"DAX for: `{input_expr}`"):
                        st.code(dax_measure, language='dax')
            # --- Step 5: Generate Report ---

            if st.session_state.get('visual_configs'):
                st.markdown("---")
                st.header("Step 5: Generate Power BI Report")
                if st.button("Generate Report", type="primary"):
                    generate_and_run_pbi_automation()
                
                if st.session_state.get('generated_pbi_config'):
                    st.subheader("Generated `config.yaml` Content")
                    st.code(st.session_state['generated_pbi_config'], language="yaml")
                    st.download_button(
                        label="Download config.yaml",
                        data=st.session_state['generated_pbi_config'],
                        file_name="config.yaml",
                        mime="text/yaml"
                    )
            
            # --- (For Debugging) Final Configuration ---
            st.markdown("---")
            with st.expander("Show Current Visual Configuration (for debugging)"):
                st.json(st.session_state.get('visual_configs', {}))
if __name__ == "__main__":
    main()