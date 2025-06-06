import requests
import json
import pandas as pd
from tqdm import tqdm  # Import the tqdm library for progress bars
from dotenv import load_dotenv
import os
import math
from bs4 import BeautifulSoup

load_dotenv()
API_KEY = os.getenv("API_KEY")
API_URL = os.getenv("API_URL")
MODEL = os.getenv("MODEL")


class TicketProcessor:
    """
    TicketProcessor is responsible for cleaning, chunking, and categorizing support ticket data using an external API (such as an LLM).

    Attributes:
        api_key (str): The API key for authenticating requests.
        api_url (str): The endpoint URL for the API.
        model (str): The model identifier to use for the API.

    Methods:
        get_api_response(prompt: str) -> dict | None:
            Sends a prompt to the API and returns the response as a dictionary, or None if the request fails.
        categorize_ticket(subject: str, i: int) -> dict | None:
            Categorizes a ticket subject by sending it to the API and parsing the response into category and subcategory.
        process_csv(csv_file: str) -> None:
            Reads a CSV file, categorizes each subject, and writes the results back to the CSV file.
        clean(self, csv_file: str, csv_result_file: str) ->
            Cleans the 'Description' column in the CSV file by stripping HTML tags and writing the cleaned data to a new CSV file.
        chunk_tickets(self, csv_file: str, chunk_size: int) -> None:
            Splits the CSV file into smaller chunks of specified size and saves each chunk as a separate CSV file in the 'chunks' directory.
    """

    def __init__(self, api_key: str, api_url: str, model: str):
        self.api_key = api_key
        self.api_url = api_url
        self.model = model

    def get_api_response(self, prompt: str) -> dict | None:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
        }
        try:
            response = requests.post(
                self.api_url, headers=headers, data=json.dumps(payload)
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error: {e}")
            return None

    def categorize_ticket(self, subject: str, i: int) -> dict | None:
        prompt = f"{subject}"
        response_data = self.get_api_response(prompt)

        if (
            response_data is not None
            and "choices" in response_data
            and len(response_data["choices"]) > 0
        ):
            category_with_subcategory = response_data["choices"][0]["message"][
                "content"
            ].strip()
            if "|" in category_with_subcategory:
                categories = category_with_subcategory.split("|")
                if len(categories) == 2:
                    return {
                        "Category": categories[0].strip(),
                        "Sub Category": categories[1].strip(),
                    }
                else:
                    print(
                        f"Failed to categorize: {subject}, due length of categories being more than 2. Row: {i}"
                    )
                    return None
            else:
                print(
                    f"Failed to categorize: {subject}, due to invalid response: {category_with_subcategory}. Row: {i}"
                )
                return None
        else:
            print(f"Failed to categorize {subject}")
            return None

    def process_csv(self, csv_file: str) -> None:
        df = pd.read_csv(csv_file)

        for i, subject in enumerate(df["Subject"], start=1):
            if not isinstance(subject, str) or not subject.strip():
                print(f"Skip ticket: {i}, due to null or non-string value.")
                continue
            if subject.strip() == "(No Subject)":
                print(f"Skip ticket: {i}, due to (No Subject).")
                continue
            response_data = self.get_api_response(subject)

            if response_data is not None:
                category_info = self.categorize_ticket(subject, i)
                if category_info:
                    df.loc[df["Subject"] == subject, "Category"] = category_info[
                        "Category"
                    ]
                    df.loc[df["Subject"] == subject, "Sub Category"] = category_info[
                        "Sub Category"
                    ]
            else:
                print(f"Failed to categorize ticket: {i}, {subject}")

            print(f"Categorized {i} out of {len(df)} subjects", end="\r")

        df.to_csv(csv_file, index=False)
        print(f"\nCSV file '{csv_file}' updated with categories.")

    def clean(self, csv_file: str, csv_result_file: str) -> None:
        df = pd.read_csv(csv_file)

        for index, row in df.iterrows():
            try:
                soup = BeautifulSoup(str(row["Description"]), "html.parser")

                if pd.isna(row["Description"]):
                    df.at[index, "Description"] = ""
                else:
                    df.at[index, "Description"] = soup.get_text().strip()
            except Exception as e:
                print(f"Error parsing row {index}: {e}")

        df.to_csv(csv_result_file, index=False)

    def chunk_tickets(self, csv_file: str, chunk_size: int) -> None:
        df = pd.read_csv(csv_file)

        os.makedirs("chunks", exist_ok=True)

        num_chunks = math.ceil(len(df) / chunk_size)

        for i in range(num_chunks):
            start = i * chunk_size
            end = start + chunk_size
            chunk = df.iloc[start:end]
            chunk_filename = f"chunks/tickets_chunk_{i + 1}.csv"
            chunk.to_csv(chunk_filename, index=False)
            print(f"Saved: {chunk_filename}")


csv_file = os.getenv("csvfile")

if API_KEY and API_URL and MODEL and csv_file:
    assigner = TicketProcessor(API_KEY, API_URL, MODEL)
    assigner.process_csv(csv_file)
else:
    print("Missing API configuration or csvfile environment variable.")
