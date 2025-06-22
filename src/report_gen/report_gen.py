import json
import os
import uuid
import yaml
import shutil
from pathlib import Path
import argparse

from src.report_gen.utils import load_yaml_file, load_json_file, create_and_write_json
from src.report_gen.content_generators import (generate_platform_file_content, generate_pbir_file_content,
                              generate_pbip_file_content)
from src.report_gen.report_generator import generate_report_file


def report_generator(config, local_settings_template, theme_template, semantic_layout_content):
    """
    Takes config and templates, orchestrates content generation, and returns a
    dictionary of file paths and their string contents.
    """
    files_to_create = {}

    # --- 1. Setup Initial Variables ---
    project_name = config.get('projectName')
    report_folder_name = f"{project_name}.Report"
    theme_name = theme_template.get("name", "MyCustomTheme")

    # Define file paths using Path objects for consistency
    base_output_path = Path("../../dump")
    report_folder_path = base_output_path / report_folder_name

    # --- 2. Handle Image Resources (logic remains the same, but saves to dictionary) ---
    image_resources = []
    registered_image_name = None
    source_image_path = Path('../../assets/Goodyear_Tire_and_Rubber_Compa15066536882930803.png')  # Example path

    if source_image_path.exists():
        target_image_dir = report_folder_path / 'StaticResources' / 'RegisteredResources'

        safe_name_base = "".join(c if c.isalnum() else "_" for c in source_image_path.stem)
        unique_suffix = str(uuid.uuid4().int)[:16]
        registered_image_name = f"{safe_name_base}_{unique_suffix}{source_image_path.suffix}"

        # Instead of writing the file, we read its content to save later
        with open(source_image_path, 'rb') as f:
            image_content_bytes = f.read()

        # Add the image file to our dictionary of files to create
        # Note: We handle binary files differently
        files_to_create[str(target_image_dir / registered_image_name)] = image_content_bytes

        image_resources.append({"type": 100, "name": registered_image_name, "path": registered_image_name})
    else:
        print(f"Warning: '{source_image_path.name}' not found. Skipping image generation.")

    # --- 3. Generate Content for Each File ---

    # .pbip file
    pbip_content = {"version": "1.0", "artifacts": [{"report": {"path": report_folder_name}}],
                    "settings": {"enableAutoRecovery": True}}
    files_to_create[str(base_output_path / f"{project_name}.pbip")] = json.dumps(pbip_content, indent=2)

    # .platform file
    platform_content = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Report", "displayName": project_name},
        "config": {"version": "2.0", "logicalId": str(uuid.uuid4())}}
    files_to_create[str(report_folder_path / ".platform")] = json.dumps(platform_content, indent=2)

    # definition.pbir file
    dataset_config = config.get('dataset', {})
    pbir_content = {"version": "1.0", "datasetReference": {
        "byConnection": {"connectionString": dataset_config.get('connection', {}).get('connectionString'),
                         "pbiServiceModelId": None, "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                         "pbiModelDatabaseName": dataset_config.get('connection', {}).get('database'),
                         "name": "EntityDataSource", "connectionType": "pbiServiceXmlaStyleLive"}}}
    files_to_create[str(report_folder_path / "definition.pbir")] = json.dumps(pbir_content, indent=2)

    # localSettings.json file
    local_settings_template.pop('activeSection', None)
    files_to_create[str(report_folder_path / ".pbi" / "localSettings.json")] = json.dumps(local_settings_template,
                                                                                          indent=2)

    # theme.json file
    theme_file_path = report_folder_path / "StaticResources" / "SharedResources" / "BaseThemes" / f"{theme_name}.json"
    files_to_create[str(theme_file_path)] = json.dumps(theme_template, indent=2)

    # semanticModelDiagramLayout.json file
    files_to_create[str(report_folder_path / "semanticModelDiagramLayout.json")] = json.dumps(semantic_layout_content,
                                                                                              indent=2)

    # --- 4. Generate the main report.json (the most complex part) ---
    report_content_dict = generate_report_file(config, theme_name, image_resources, registered_image_name)
    files_to_create[str(report_folder_path / "report.json")] = json.dumps(report_content_dict, indent=2)

    return files_to_create


def main():
    parser = argparse.ArgumentParser(description="Generate PBIP project from a configuration file.")
    parser.add_argument(
        '--config',
        type=str,
        default='../../dump/config.yaml',
        help='Path to the configuration YAML file (default: config.yaml)'
    )
    args = parser.parse_args()

    try:
        # --- 1. Load all inputs ---
        print("Loading configuration files...")
        config = load_yaml_file(args.config)
        if not config:
            raise ValueError(f"The config file '{args.config}' is empty or invalid.")

        local_settings_template = load_json_file('../../config/localSettings.json')
        theme_template = load_json_file('../../config/theme.json')
        semantic_layout_content = load_json_file('../../config/semantic.json')

        project_name = config.get('projectName')
        if not project_name:
            raise ValueError("'projectName' is missing from config.yaml")

        # --- 2. Clean up output directory ---
        output_dir = Path("dump")
        if output_dir.exists():
            shutil.rmtree(output_dir)
            print(f"Cleaned up existing output directory: '{output_dir}'")
        output_dir.mkdir(parents=True, exist_ok=True)

        # --- 3. Call the generator to get all file contents ---
        print("\nStarting PBIP generation...")
        files_to_generate = report_generator(
            config=config,
            local_settings_template=local_settings_template,
            theme_template=theme_template,
            semantic_layout_content=semantic_layout_content
        )

        # --- 4. Write the files to disk ---
        for filepath_str, content in files_to_generate.items():
            filepath = Path(filepath_str)
            filepath.parent.mkdir(parents=True, exist_ok=True)

            # Handle binary content (images) vs. text content (json)
            if isinstance(content, bytes):
                with open(filepath, 'wb') as f:
                    f.write(content)
            else:
                with open(filepath, 'w') as f:
                    f.write(content)
            print(f"Generated '{filepath}'")

        print(f"\nPBIP Generation Complete! To open, double-click 'dump/{project_name}.pbip'")

    except FileNotFoundError as e:
        print(f"ERROR: Missing a required input file: {e.filename}. Please check your 'config' folder.")
    except (ValueError, yaml.YAMLError) as e:
        print(f"ERROR: A configuration file is empty or has an invalid format: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()