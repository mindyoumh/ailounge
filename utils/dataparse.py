import os
import pandas as pd
from bs4 import BeautifulSoup

def clean_csv_description(input_csv_path: str, output_csv_path: str = None) -> str:
    if not os.path.exists(input_csv_path):
        raise FileNotFoundError(f"CSV file not found: {input_csv_path}")

    base, ext = os.path.splitext(input_csv_path)
    if output_csv_path is None:
        output_csv_path = f"{base}_processed{ext}"

    df = pd.read_csv(input_csv_path)

    # List of columns to clean
    columns_to_clean = ["Ticket Id", "Ticket Reference Id", "Subject", "Description", "Category", "Sub Category", "Tags"]

    for col in columns_to_clean:
        if col in df.columns:
            df[col] = df[col].astype(str)  # Force column to string/object before cleaning

            for index, value in df[col].items():
                try:
                    if pd.isna(value) or value.strip().lower() == 'nan':
                        df.at[index, col] = ''
                    else:
                        soup = BeautifulSoup(str(value), 'html.parser')
                        df.at[index, col] = soup.get_text().strip()
                except Exception as e:
                    print(f"Error parsing row {index}, column '{col}': {e}")

    df.to_csv(output_csv_path, index=False)
    print(f"✅ Processed CSV saved to '{output_csv_path}'")
    return output_csv_path
