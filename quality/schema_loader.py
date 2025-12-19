import yaml
from pathlib import Path

def load_schema(schema_name: str):
    schema_path = Path("config/schemas") / f"{schema_name}.yaml"
    with open(schema_path, "r") as f:
        return yaml.safe_load(f)
