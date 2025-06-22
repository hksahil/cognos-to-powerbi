import json
import yaml
from pathlib import Path

def load_yaml_file(filename):
    """Load and parse a YAML file."""
    with open(filename, 'r') as f: 
        return yaml.safe_load(f)

def load_json_file(filename):
    """Load and parse a JSON file with validation."""
    try:
        with open(filename, 'r') as f:
            content = f.read().strip()
            if not content: 
                raise ValueError(f"File '{filename}' is empty.")
            return json.loads(content)
    except json.JSONDecodeError:
        raise ValueError(f"File '{filename}' does not contain valid JSON.")

def create_and_write_json(filepath, content):
    """Create directories if needed and write JSON content to file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w') as f: 
        json.dump(content, f, indent=2)
    print(f"Generated '{filepath}'")