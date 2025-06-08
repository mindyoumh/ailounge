import os
import json
import pandas as pd
from bs4 import BeautifulSoup


def clean_csv_description(input_csv_path: str, output_csv_path: str = None) -> str:
    if not os.path.exists(input_csv_path):
        raise FileNotFoundError(f"CSV file not found: {input_csv_path}")

    base, ext = os.path.splitext(input_csv_path)
    if output_csv_path is None:
        output_csv_path = f"{base}_cleaned{ext}"

    df = pd.read_csv(input_csv_path)

    for index, row in df.iterrows():
        try:
            if pd.isna(row["Description"]):
                df.at[index, "Description"] = ""
            else:
                soup = BeautifulSoup(str(row["Description"]), "html.parser")
                df.at[index, "Description"] = soup.get_text().strip()
        except Exception as e:
            print(f"Error parsing row {index}: {e}")

    df.to_csv(output_csv_path, index=False)
    print(f"✅ Cleaned CSV saved to '{output_csv_path}'")
    return output_csv_path


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
