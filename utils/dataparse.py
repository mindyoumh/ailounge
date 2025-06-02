import os
import pandas as pd
from bs4 import BeautifulSoup

def clean_csv_description(input_csv_path: str) -> str:
    if not os.path.exists(input_csv_path):
        raise FileNotFoundError(f"CSV file not found: {input_csv_path}")

    # Generate output path by inserting "_cleaned" before .csv
    base, ext = os.path.splitext(input_csv_path)
    output_csv_path = f"{base}_cleaned{ext}"

    # Load and clean
    df = pd.read_csv(input_csv_path)
    
    for index, row in df.iterrows():
        try:
            if pd.isna(row['Description']):
                df.at[index, 'Description'] = ''
            else:
                soup = BeautifulSoup(str(row['Description']), 'html.parser')
                df.at[index, 'Description'] = soup.get_text().strip()
        except Exception as e:
            print(f"Error parsing row {index}: {e}")

    df.to_csv(output_csv_path, index=False)
    print(f"\n✅ Cleaned CSV saved to '{output_csv_path}'")
    return output_csv_path

input_csv = "data/Zoho Tickets 2024-2025.csv"
cleaned_csv = clean_csv_description(input_csv)