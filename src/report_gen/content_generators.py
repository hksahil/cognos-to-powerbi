import uuid

def generate_platform_file_content(project_name):
    """Generate content for .platform file."""
    return {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Report", "displayName": project_name},
        "config": {"version": "2.0", "logicalId": str(uuid.uuid4())}
    }

def generate_pbir_file_content(config):
    """Generate content for definition.pbir file."""
    dataset_config = config.get('dataset', {})
    return {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/1.0.0/schema.json",
        "version": "1.0", 
        "datasetReference": {
            "byConnection": {
                "connectionString": dataset_config.get('connection', {}).get('connectionString'),
                "pbiServiceModelId": None, 
                "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                "pbiModelDatabaseName": dataset_config.get('connection', {}).get('database'),
                "name": "EntityDataSource", 
                "connectionType": "pbiServiceXmlaStyleLive"
            }
        }
    }

def generate_pbip_file_content(report_folder_name):
    """Generate content for .pbip file."""
    return {
        "version": "1.0", 
        "artifacts": [{"report": {"path": report_folder_name}}],
        "settings": {"enableAutoRecovery": True}
    }