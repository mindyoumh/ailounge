import os
import pandas as pd
from bs4 import BeautifulSoup

# Define paths
input_csv = os.path.join("data", "Zoho Tickets 2021-2024.csv")
output_csv = os.path.join("data", "Zoho Tickets 2021-2024_cleaned.csv")

# Read CSV
df = pd.read_csv(input_csv)

# Clean the 'Description' column
for index, row in df.iterrows():
    try:
        if pd.isna(row['Description']):
            df.at[index, 'Description'] = ''
        else:
            soup = BeautifulSoup(str(row['Description']), 'html.parser')
            df.at[index, 'Description'] = soup.get_text().strip()
    except Exception as e:
        print(f"Error parsing row {index}: {e}")

# Save cleaned CSV (better to save to a new file to avoid overwriting original)
df.to_csv(output_csv, index=False)
print(f"\n✅ Cleaned CSV saved to '{output_csv}'")
