import json


def string_to_values(string: str):
    try:
        clean_str = string.strip("`").strip()
        if clean_str.startswith("json"):
            clean_str = clean_str[4:].strip()

        data = json.loads(clean_str)

        category = data.get("Category", "Unknown")
        sub_category = data.get("Sub Category", "Unknown")
        tags = (
            ", ".join(data.get("Tags", []))
            if isinstance(data.get("Tags"), list)
            else "Unknown"
        )

        return category, sub_category, tags

    except (json.JSONDecodeError, TypeError):
        print("Invalid result")
        return "Unknown", "Unknown", "Unknown"
