import json


def string_to_values(string: str):
    try:
        clean_str = string.replace("`", "").replace("json", "").strip()

        data = json.loads(clean_str)

        return data

    except (json.JSONDecodeError, TypeError):
        print("Invalid result")
        return "Unknown", "Unknown", "Unknown"
