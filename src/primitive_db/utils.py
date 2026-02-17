import json

def load_metadata(filepath):
    try:
        with open(filepath) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_metadata(filepath, data):
    with open(filepath, "w") as f:
        json.dump(data, f)
