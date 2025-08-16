from pathlib import Path

# Get the root directory of the project (src's parent)
ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent  # Adjust based on folder depth

print(ROOT_DIR)

CONFIG_FILE_PATH = ROOT_DIR / "config" / "config.yaml"
PARAMS_FILE_PATH = ROOT_DIR / "params.yaml"
SCHEMA_FILE_PATH = ROOT_DIR / "schema.yaml"
