import json
import re


def string_to_values(string: str):
    try:
        clean_str = string.replace("`", "").strip()

        clean_str = re.sub(
            r'\{\s*"Ticket Id":\s*\d+,\s*"Category":\s*"Unknown",\s*"Sub Category":\s*"Unknown",\s*"Tags":\s*\[\s*"SaaS",\s*"Purchase Form"\s*(,\s*)?$',
            "",
            clean_str,
            flags=re.DOTALL,
        )

        pattern = r"\{[^{}]+\}"
        json_objects = re.findall(pattern, clean_str)

        result = [json.loads(obj) for obj in json_objects]
        return result
    except json.JSONDecodeError as e:
        print("JSON Decode Error:", e)
    except Exception as e:
        print("Error:", e)
