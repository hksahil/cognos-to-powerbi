import streamlit as st
import yaml
import zipfile
from io import StringIO, BytesIO
from pathlib import Path
import subprocess

from src.report_gen.report_gen import report_generator
from src.xml_pbi.utils import FlowDict, CustomDumper, load_json_file


def generate_and_run_pbi_automation():
    """Generates config.yaml from session state and runs the PBI Automation script."""
    if not st.session_state.get('visual_configs'):
        st.error("No visual configurations found. Please configure visuals first.")
        return

    try:
        # --- 1. Build the base configuration dictionary ---
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

        # --- 2. Generate Measures from all visuals across all pages ---
        generated_measures = []
        processed_expressions = set()
        for page_data in st.session_state.visual_configs.values():
            for visual_config in page_data.get('visuals', []):
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

        # --- 3. Generate Pages and Visuals ---
        pages = []
        for page_data in st.session_state.visual_configs.values():
            page_visuals = []
            for visual_config in page_data.get('visuals', []):
                # The visual_config is now the correct dictionary with 'visual_type'
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
                    page_visuals.append(matrix_def)
                elif visual_config['visual_type'] == 'table':
                    sorted_cols = sorted(visual_config.get('values', []), key=lambda i: i.get('seq', 0))
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
                    table_columns = []
                    for item in sorted_cols:
                        item_type = item.get('type', 'Column')
                        name = f"{item['column']} Measure" if item_type == 'Measure' else item['column']
                        table_columns.append(FlowDict({
                            "name": name,
                            "table": item['table'],
                            "type": item_type
                        }))
        
                    table_def = {
                        "type": "table",
                        "position": FlowDict({"x": 28.8, "y": 100, "width": 1220, "height": 800}),
                        "fields": table_columns,
                        "filters": transformed_filters
                    }
                    page_visuals.append(table_def)
            
            if page_visuals:
                pages.append(FlowDict({
                    "displayName": page_data.get('name'),
                    "visuals": page_visuals
                }))
        config['report']['pages'] = pages

        # --- 4. Generate YAML string and save to session state ---
        yaml_string_io = StringIO()
        yaml.dump(config, yaml_string_io, Dumper=CustomDumper, sort_keys=False, indent=2, allow_unicode=True)
        generated_yaml_str = yaml_string_io.getvalue()
        st.session_state['generated_pbi_config'] = generated_yaml_str.strip()
        st.success("`config.yaml` content generated successfully!")

        app_dir = Path(__file__).parent.parent.parent
        config_dir = app_dir / 'config'
        local_settings_template = load_json_file(config_dir / 'localSettings.json')
        theme_template = load_json_file(config_dir / 'theme.json')
        semantic_layout_content = load_json_file(config_dir / 'semantic.json')

        if not all([local_settings_template, theme_template, semantic_layout_content]):
            st.error("Failed to load one or more template files. Aborting report generation.")
            return

        # --- 5. Call Report Generator to get file contents ---
        st.info("Generating report files in memory...")
        files_to_create = report_generator(
            config=config,
            local_settings_template=local_settings_template,
            theme_template=theme_template,
            semantic_layout_content=semantic_layout_content
        )

        # --- 6. Create a ZIP archive in memory ---
        st.info("Creating ZIP archive...")
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for file_path_str, content in files_to_create.items():
                path_parts = Path(file_path_str).parts
                try:
                    dump_index = path_parts.index('dump')
                    archive_path = Path(*path_parts[dump_index + 1:])
                    zip_file.writestr(str(archive_path), content)
                except ValueError:
                    st.warning(f"Could not determine archive path for {file_path_str}. Skipping.")
        
        zip_buffer.seek(0)
        st.session_state['generated_report_zip'] = zip_buffer.getvalue()
        st.session_state['report_zip_filename'] = f"{config.get('projectName', 'report')}.zip"
        st.success("✅ Report ZIP archive is ready for download.")

    except Exception as e:
        st.error(f"An unexpected error occurred during report generation: {e}")
        st.exception(e)