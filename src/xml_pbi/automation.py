import streamlit as st
import yaml
from io import StringIO
from pathlib import Path
import subprocess
from src.xml_pbi.utils import FlowDict, CustomDumper



def generate_and_run_pbi_automation():
    """Generates config.yaml from session state and runs the PBI Automation script."""
    if not st.session_state.get('visual_configs'):
        st.error("No visual configurations found. Please configure visuals first.")
        return

    try:
        # --- 1. Build the configuration dictionary ---
        config = {}
        report_name = st.session_state.mapped_data.get('report_name', 'Generated Report')
        
        config['projectName'] = report_name
        config['dataset'] = {
            "connection": {
                "connectionString": "Data Source=powerbi://api.powerbi.com/v1.0/myorg/EMEA Development;Initial Catalog=\"EU Order to Cash (Ad-hoc)\";Access Mode=readonly;Integrated Security=ClaimsToken",
                "database": "7f97f9b2-2c89-4359-966b-4612b960fbb1"
            },
            "modelName": "EU Order to Cash (Ad-Hoc)"
        }
        config['report'] = {
            'title': FlowDict({"text": report_name}),
            'data_refresh': FlowDict({"table": "Date Refresh Table", "column": "UPDATED_DATE"})
        }

        # --- 2. Generate Measures ---
        generated_measures = []
        processed_expressions = set()
        for visual_config in st.session_state.visual_configs.values():
            for field_type in ['rows', 'columns', 'values']:
                for item in visual_config.get(field_type, []):
                    if item.get('type') == 'Measure' and item['pbi_expression'] not in processed_expressions:
                        measure_name = f"{item['column']} Measure"
                        dax_expr = item.get('ai_generated_dax', f"SUM({item['pbi_expression']})")
                        data_type = item.get('ai_data_type', 'decimal number')
                        
                        generated_measures.append(FlowDict({
                            "name": measure_name,
                            "table": item['table'],
                            "expression": dax_expr,
                            "dataType": data_type
                        }))
                        processed_expressions.add(item['pbi_expression'])
        config['report']['measures'] = generated_measures

        # --- 3. Generate Visuals ---
        visuals = []
        for visual_config in st.session_state.visual_configs.values():
            if visual_config['visual_type'] == 'matrix':
                transformed_filters = []
                for f in visual_config.get('filters', []):
                    transformed_filters.append(FlowDict({
                        "field": FlowDict({
                            "name": f.get('column'),
                            "table": f.get('table'),
                            "type": "column"
                        }),
                        "filterType": f.get('filter_type'),
                        "values": f.get('values')
                    }))
                matrix_def = {
                    "type": "matrix",
                    "position": FlowDict({"x": 28.8, "y": 100, "width": 1220, "height": 800}),
                    "rows": [FlowDict({"name": item['column'], "table": item['table'], "type": "Column"}) for item in visual_config.get('rows', []) if item['type'] == 'Column'],
                    "columns": [FlowDict({"name": item['column'], "table": item['table'], "type": "Column"}) for item in visual_config.get('columns', []) if item['type'] == 'Column'],
                    "values": [FlowDict({"name": f"{item['column']} Measure", "table": item['table'], "type": "Measure"}) for item in visual_config.get('values', []) if item['type'] == 'Measure'],
                    "filters": transformed_filters
                }
                visuals.append(matrix_def)
        config['report']['visuals'] = visuals

        # --- 4. Generate YAML string and save to session state ---
        yaml_string_io = StringIO()
        yaml.dump(config, yaml_string_io, Dumper=CustomDumper, sort_keys=False, indent=2, allow_unicode=True)
        generated_yaml_str = yaml_string_io.getvalue()
        st.session_state['generated_pbi_config'] = generated_yaml_str.strip()
        st.success("`config.yaml` content generated successfully!")

        # --- 5. Save config locally and run PBI Automation script ---
        app_dir = Path(__file__).parent
        local_config_path = app_dir / "config.yaml"
        with open(local_config_path, 'w', encoding='utf-8') as f:
            f.write(st.session_state['generated_pbi_config'])
        st.info(f"Generated `config.yaml` saved to: {local_config_path}")

        pbi_automation_script_path = Path(r"c:\Users\NileshPhapale\Desktop\PBI Automation\main.py")
        pbi_automation_project_dir = pbi_automation_script_path.parent
        python_executable = pbi_automation_project_dir / ".venv" / "Scripts" / "python.exe"

        if pbi_automation_script_path.exists():
            command = [
                str(python_executable), 
                str(pbi_automation_script_path),
                "--config", 
                str(local_config_path.resolve())
            ]
            st.info(f"Executing command: `{' '.join(command)}`")
            process = subprocess.Popen(
                command, 
                cwd=str(pbi_automation_project_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8'
            )
            stdout, stderr = process.communicate(timeout=300)
            if process.returncode == 0:
                st.success("PBI Automation script executed successfully!")
                if stdout: st.text_area("Script Output:", value=stdout, height=200)
                if stderr: st.text_area("Script Warnings:", value=stderr, height=100)
            else:
                st.error(f"PBI Automation script failed with code {process.returncode}.")
                if stdout: st.text_area("Script Output:", value=stdout, height=150)
                if stderr: st.text_area("Script Error Output:", value=stderr, height=150)
        else:
            st.warning(f"PBI Automation script not found at: {pbi_automation_script_path}. Skipping execution.")

    except Exception as e:
        st.error(f"An unexpected error occurred during config generation or script execution: {e}")
        st.exception(e)
