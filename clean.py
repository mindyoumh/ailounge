import pandas as pd
from bs4 import BeautifulSoup

df = pd.read_csv("./Zoho Tickets 2021-2024.csv")

for index, row in df.iterrows():
    try:
        soup = BeautifulSoup(str(row["Description"]), "html.parser")

        if pd.isna(row["Description"]):
            df.at[index, "Description"] = ""
        else:
            df.at[index, "Description"] = soup.get_text().strip()
    except Exception as e:
        print(f"Error parsing row {index}: {e}")

df.to_csv("./Zoho Tickets 2021-2024_result.csv", index=False)
