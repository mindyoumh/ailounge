import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os

load_dotenv() 
csv_file = os.getenv("csvfile")
df = pd.read_csv(csv_file)

for index, row in df.iterrows():
    try:
        soup = BeautifulSoup(str(row['Description']), 'html.parser')
        
        if pd.isna(row['Description']):
            df.at[index, 'Description'] = ''
        else:
            df.at[index, 'Description'] = soup.get_text().strip()
    except Exception as e:
        print(f"Error parsing row {index}: {e}")

df.to_csv(csv_file, index=False)