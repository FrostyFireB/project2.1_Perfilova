import json
from pathlib import Path

DATA_DIR = Path("data")

def load_metadata(filepath):
    try:
        with open(filepath) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_metadata(filepath, data):
    with open(filepath, "w") as f:
        json.dump(data, f)

def load_table_data(table_name):
    filepath = DATA_DIR / f"{table_name}.json"
    try:
        with filepath.open("r") as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def save_table_data(table_name, data):
    DATA_DIR.mkdir(exist_ok=True)

    filepath = DATA_DIR / f"{table_name}.json"
    with filepath.open("w") as f:
        json.dump(data, f)
