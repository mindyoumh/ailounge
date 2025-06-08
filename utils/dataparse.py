import os
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
