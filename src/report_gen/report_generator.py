import json
import uuid
from src.report_gen.visual_factory import (_create_shape_config, _create_image_config, _create_textbox_config,
                                _create_data_updated_card_config, _create_footer_shape_config,
                                _create_footer_textbox_config, _create_matrix_config, _generate_filter_json_string,
                                _create_table_config)


# --- ** NEW PAGE GENERATION FUNCTION ** ---
def _generate_page_section(page_def, report_config, config, page_number, registered_image_name=None):
    """Builds the complete JSON object for a single report page (a 'section')."""
    visual_containers = []

    # --- Static Header Elements ---
    shape_config = {"name": uuid.uuid4().hex[:20],
                    "layouts": [{"id": 0, "position": {"x": 0, "y": 0, "z": 0, "width": 1280, "height": 78}}],
                    "singleVisual": _create_shape_config()}
    visual_containers.append(
        {"config": json.dumps(shape_config), "filters": "[]", "x": 0, "y": 0, "z": 0, "width": 1280, "height": 78})
    if registered_image_name:
        image_config = {"name": uuid.uuid4().hex[:20],
                        "layouts": [{"id": 0, "position": {"x": 0, "y": 0, "z": 2000, "width": 239, "height": 79}}],
                        "singleVisual": _create_image_config(registered_image_name)}
        visual_containers.append(
            {"config": json.dumps(image_config), "filters": "[]", "x": 0, "y": 0, "z": 2000, "width": 239,
             "height": 79})

    # ** MODIFIED **: Use the global report-level title for every page
    title_text = report_config.get('title', {}).get('text', 'Report Title')
    textbox_config = {"name": uuid.uuid4().hex[:20],
                      "layouts": [{"id": 0, "position": {"x": 389, "y": 2, "z": 1000, "width": 519, "height": 68}}],
                      "singleVisual": _create_textbox_config(title_text)}
    visual_containers.append(
        {"config": json.dumps(textbox_config), "filters": "[]", "x": 389, "y": 2, "z": 1000, "width": 519,
         "height": 68})

    data_refresh_config = report_config.get('data_refresh')
    if data_refresh_config and data_refresh_config.get('table') and data_refresh_config.get('column'):
        table, column = data_refresh_config['table'], data_refresh_config['column']
        card_visual_config = {"name": uuid.uuid4().hex[:20], "layouts": [
            {"id": 0, "position": {"x": 1057.69, "y": 11.54, "z": 6000, "width": 221.79, "height": 60.26}}],
                              "singleVisual": _create_data_updated_card_config(table, column)}
        visual_containers.append(
            {"config": json.dumps(card_visual_config), "filters": "[]", "x": 1057.69, "y": 11.54, "z": 6000,
             "width": 221.79, "height": 60.26})

    # --- Static Footer Elements ---
    footer_shape_config = {"name": uuid.uuid4().hex[:20], "layouts": [
        {"id": 0, "position": {"x": 28.21, "y": 923.08, "z": 4000, "width": 1220.51, "height": 58.97}}],
                           "singleVisual": _create_footer_shape_config()}
    visual_containers.append(
        {"config": json.dumps(footer_shape_config), "filters": "[]", "x": 28.21, "y": 923.08, "z": 4000,
         "width": 1220.51, "height": 58.97})
    model_name = config.get('dataset', {}).get('modelName', 'Unknown Model')
    footer_textbox_config = {"name": uuid.uuid4().hex[:20], "layouts": [
        {"id": 0, "position": {"x": 35.94, "y": 934.38, "z": 5000, "width": 512.50, "height": 45.31}}],
                             "singleVisual": _create_footer_textbox_config(model_name)}
    visual_containers.append(
        {"config": json.dumps(footer_textbox_config), "filters": "[]", "x": 35.94, "y": 934.38, "z": 5000,
         "width": 512.50, "height": 45.31})

    # --- Dynamic Data Visuals for THIS page ---
    for visual_def in page_def.get('visuals', []):
        if not isinstance(visual_def, dict): continue
        visual_config_json, filter_string = None, "[]"
        if visual_def.get('type') == 'matrix':
            visual_config_json = _create_matrix_config(visual_def)
        elif visual_def.get('type') == 'table':
            visual_config_json = _create_table_config(visual_def)
        else:
            print(f"Warning: Visual type '{visual_def.get('type')}' not recognized. Skipping.")
            continue
        filter_string = _generate_filter_json_string(visual_def.get('filters'))
        pos = visual_def.get('position', {})
        full_visual_config = {"name": uuid.uuid4().hex[:20], "layouts": [{"id": 0, "position": {**pos}}],
                              "singleVisual": visual_config_json}
        visual_containers.append({"config": json.dumps(full_visual_config), "filters": filter_string, **pos})

    # --- Assemble the final page (section) object ---
    page_section_config = {"objects": {
        "background": [{"properties": {"color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#E6E6E6'"}}}}}}}],
        "wallpaper": [{"properties": {"color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#FFFFFF'"}}}}},
                                      "transparency": {"expr": {"Literal": {"Value": "0D"}}}}}]}}
    return {
        "name": uuid.uuid4().hex[:20],
        "displayName": page_def.get('displayName', f"Page {page_number}"),
        "height": 1000.0,
        "width": 1280.0,
        "visualContainers": visual_containers,
        "config": json.dumps(page_section_config)
    }


def generate_report_file(config, theme_name, image_resources, registered_image_name=None):
    """Generate the complete report.json file content, including report-level measures with correct data types."""
    report_config = config.get('report', {})

    # --- ** MODIFIED LOGIC **: Generate Model Extensions with a robust data type map ---
    model_extensions = []

    # Map user-friendly names (from the image) to Power BI's internal integer codes (from your table)
    data_type_map = {
        "text": 1,
        "whole number": 2,
        "decimal number": 3,
        "date/time": 4,
        "date": 4,  # Both map to the general DateTime type
        "time": 4,  # Both map to the general DateTime type
        "true/false": 5,
        "fixed decimal number": 6,
        "binary": 7
    }

    if report_config.get('measures'):
        measures_by_table = {}
        for measure in report_config.get('measures', []):
            table = measure.get('table')
            if not table: continue

            if table not in measures_by_table:
                measures_by_table[table] = []

            # Read the data type from YAML, default to 'text' if not specified
            yaml_data_type = measure.get('dataType', 'text').lower()
            # Look up the integer code from the map, default to 1 (Text) if not found
            pbi_data_type = data_type_map.get(yaml_data_type, 1)

            dax_expression = measure.get('expression', '')

            measures_by_table[table].append({
                "name": measure.get('name'),
                "dataType": pbi_data_type,  # Use the mapped integer code
                "expression": dax_expression,
                "hidden": False,
                "formatInformation": {"formatString": measure.get('formatString', '')}
            })

        entities = [{"name": table, "extends": table, "measures": measures} for table, measures in
                    measures_by_table.items()]
        model_extensions.append({"name": "extension", "entities": entities})
        print(f"Generated {len(report_config.get('measures', []))} report-level measures with correct data types.")
    else:
        print("No report-level measures defined in config.")

    # --- Generate each page by looping through the config ---
    sections = []
    pages_config = report_config.get('pages', [])
    if not pages_config:
        print("Warning: No pages defined in config.yaml. Creating one default blank page.")
        pages_config.append({})

    for i, page_def in enumerate(pages_config):
        page_section = _generate_page_section(page_def, report_config, config, i + 1, registered_image_name)
        sections.append(page_section)

    # Assemble final report.json
    resource_packages = [{"resourcePackage": {"type": 2, "name": "SharedResources", "items": [
        {"type": 202, "name": theme_name, "path": f"StaticResources/SharedResources/BaseThemes/{theme_name}.json"}]}}]

    if image_resources:
        resource_packages.append(
            {"resourcePackage": {"type": 1, "name": "RegisteredResources", "items": image_resources}})

    report_settings = {"useNewFilterPaneExperience": True, "useStylableVisualContainerHeader": True}
    page_section_config = {
        "objects": {
            "background": [
                {"properties": {"color": {"solid": {"color": {"expr": {"Literal": {"Value": "'#e6e6e6'"}}}}}}}]
        }
    }
    top_level_objects = {"section": [{"properties": {"verticalAlignment": {"expr": {"Literal": {"Value": "'Top'"}}}}}]}

    report_data = {
        "config": json.dumps({"version": "5.59", "activeSectionIndex": 0, "modelExtensions": model_extensions,
                              "themeCollection": {"baseTheme": {"name": theme_name}},
                              "settings": report_settings, "objects": top_level_objects}),
        "resourcePackages": resource_packages, "sections": sections
    }

    return report_data