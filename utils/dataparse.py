import os
import pandas as pd
from bs4 import BeautifulSoup

def clean_csv_description(input_csv_path: str, output_csv_path: str = None) -> str:
    """
    Cleans the textual content of specific columns in a CSV file by stripping HTML tags,
    removing NaN or empty placeholders, and trimming whitespace. The cleaned data is
    saved to a new CSV file.

    Args:
        input_csv_path (str): Path to the original CSV file to be cleaned.
        output_csv_path (str, optional): Path to save the cleaned output file.
                                         If None, a new file with '_processed' suffix is created.

    Returns:
        str: The path to the cleaned CSV file that was saved.

    Raises:
        FileNotFoundError: If the input CSV path does not exist.

    Notes:
        Columns cleaned (if they exist in the CSV):
        - "Ticket Id"
        - "Ticket Reference Id"
        - "Subject"
        - "Description"
        - "Category"
        - "Sub Category"
        - "Tags"
    """
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
