import sys
import os
import io
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import re
import json
import argparse
import tempfile
import pandas as pd
from collections import defaultdict
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from google import genai
from google.genai import types
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import service_account
from utils.classification_utils import normalize_name, merge_similar_items

class TicketClassifier:
    def __init__(self, year_range: str, chunk_size: int = 100, max_workers: int = 4):
        """
        Initializes the classifier by loading environment variables, authenticating services,
        loading the Gemini prompt, and fetching the relevant Google Sheet data for classification.

        Args:
            year_range (str): Year range used to identify the dataset folder (e.g., "2024-2025").
            chunk_size (int): Number of rows per chunk sent to Gemini for processing.
            max_workers (int): Number of threads for concurrent chunk processing.
        """
        
        load_dotenv()

        self.api_key = os.getenv("API_KEY")
        self.model_name = os.getenv("MODEL_NAME", "gemini-2.0-flash")
        self.raw_folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
        self.year_range = year_range
        self.chunk_size = chunk_size
        self.max_workers = max_workers

        if not self.api_key:
            raise ValueError("API_KEY not found in environment variables")

        self.client = genai.Client(api_key=self.api_key)
        self.drive_service = self.authenticate_drive()
        self.prompt = self._load_prompt()
        self.df = self.load_sheet_as_df()

        self.categories = defaultdict(lambda: defaultdict(set))
        self.tags = set()
        self.lock = Lock()

    def authenticate_drive(self):
        """
        Authenticates and builds the Google Drive and Sheets API services using a service account.

        Returns:
            Resource: Google Drive API client.
        """
        
        service_account_path = os.path.join(os.path.dirname(__file__), '..', 'service_account.json')
        creds = service_account.Credentials.from_service_account_file(
            service_account_path, scopes=[
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/spreadsheets.readonly'
            ]
        )
        self.sheets_service = build('sheets', 'v4', credentials=creds)
        return build('drive', 'v3', credentials=creds)

    def load_sheet_as_df(self):
        """
        Loads the '_processed' Google Sheet inside the year-specific folder from Google Drive,
        and returns it as a Pandas DataFrame.

        Returns:
            pd.DataFrame: The loaded sheet with support ticket data.
        """
        
        folder_name = f"Zoho Tickets {self.year_range}"
        folder_query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
        folder_results = self.drive_service.files().list(q=folder_query, fields="files(id, name)").execute()
        folders = folder_results.get('files', [])
        if not folders:
            raise FileNotFoundError(f"❌ Folder '{folder_name}' not found in Drive")
        self.dataset_folder_id = folders[0]['id']
        print(f"📁 Found Drive folder: {folder_name} (ID: {self.dataset_folder_id})")

        sheet_query = f"'{self.dataset_folder_id}' in parents and name contains '_processed' and mimeType='application/vnd.google-apps.spreadsheet'"
        sheet_results = self.drive_service.files().list(q=sheet_query, fields="files(id, name)").execute()
        sheets = sheet_results.get('files', [])
        if not sheets:
            raise FileNotFoundError(f"❌ No '_processed' sheet found in '{folder_name}'")

        sheet_id = sheets[0]['id']
        print(f"📄 Found Google Sheet: {sheets[0]['name']} (ID: {sheet_id})")

        sheet_metadata = self.sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        first_sheet_title = sheet_metadata['sheets'][0]['properties']['title']
        sheet_range = f"{first_sheet_title}"

        result = self.sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range=sheet_range
        ).execute()

        rows = result.get("values", [])
        if not rows:
            raise ValueError(f"❌ No data found in sheet '{sheets[0]['name']}'")

        df = pd.DataFrame(rows[1:], columns=rows[0])
        print(f"📊 Loaded sheet into DataFrame: {df.shape[0]} rows")
        return df

    def _load_prompt(self) -> str:
        """
        Loads the system prompt text used to instruct the Gemini model for ticket classification.

        Returns:
            str: The content of the system prompt file.
        """
        
        prompt_path = os.path.join(os.path.dirname(__file__), 'prompts', 'system_prompt_chunk.txt')
        with open(prompt_path, "r", encoding="utf-8") as f:
            return f.read()

    def _extract_json_from_response(self, response) -> dict:
        """
        Extracts and parses the JSON string from the Gemini model's output.
        
        Args:
            response: Gemini response object.
        
        Returns:
            dict: Parsed JSON data, or empty dict if decoding fails.
        """

        raw_text = response.candidates[0].content.parts[0].text.strip()

        if raw_text.startswith("```json"):
            raw_text = raw_text.replace("```json", "").replace("```", "").strip()

        try:
            return json.loads(raw_text)
        except json.JSONDecodeError as e:
            print("❌ Failed to decode JSON from Gemini response.")
            print(f"📍 Error: {e}")
            print(f"🧾 Partial content:\n{raw_text[:300]}...\n")
            return {}


    def _process_chunk(self, chunk: pd.DataFrame, chunk_index: int):
        """
        Sends a chunk of ticket data to Gemini for classification and collects
        categories, subcategories, and tags.

        Args:
            chunk (pd.DataFrame): A slice of the main dataset.
            chunk_index (int): Index of the chunk for logging and tracking.
        """
        
        try:
            print(f"📦 Processing chunk {chunk_index + 1}")
            csv_content = chunk[["Ticket Id", "Ticket Reference Id", "Subject", "Description", "Category", "Sub Category", "Tags"]].to_csv(index=False)
            response = self.client.models.generate_content(
                model=self.model_name,
                config=types.GenerateContentConfig(system_instruction=self.prompt),
                contents=[
                    "Here is a chunk of the support ticket dataset:",
                    csv_content
                ]
            )
            data = self._extract_json_from_response(response)

            with self.lock:
                for cat in data.get("categories", []):
                    main_cat = normalize_name(cat["name"])
                    for subcat in cat.get("subcategories", []):
                        subcat_name = normalize_name(subcat["name"])
                        tags = set(map(normalize_name, subcat.get("tags", [])))
                        self.categories[main_cat][subcat_name].update(tags)
                        self.tags.update(tags)
        except Exception as e:
            print(f"⚠️ Error in chunk {chunk_index + 1}: {e}")

    def run(self):
        """
        Initiates concurrent processing of the DataFrame in chunks using ThreadPoolExecutor.
        """
        
        futures = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for i in range(0, len(self.df), self.chunk_size):
                chunk = self.df.iloc[i:i + self.chunk_size]
                futures.append(executor.submit(self._process_chunk, chunk, i // self.chunk_size))
            for future in as_completed(futures):
                _ = future.result()

    def save_output(self):
        """
        Converts the aggregated classification result into a formatted DataFrame
        and uploads it to Google Drive as a new Google Sheet in the appropriate dataset folder.
        """
        
        rows = []
        for main_cat, subcats in self.categories.items():
            for subcat, tags in subcats.items():
                rows.append({
                    "Category": main_cat.title(),
                    "Subcategory": subcat.title(),
                    "Tags": ", ".join(sorted(list(merge_similar_items(tags))))
                })

        df_result = pd.DataFrame(rows)

        sheet_name = f"{self.year_range}_result"
        csv_bytes = df_result.to_csv(index=False).encode('utf-8')
        stream = io.BytesIO(csv_bytes)

        file_metadata = {
            'name': sheet_name,
            'mimeType': 'application/vnd.google-apps.spreadsheet',
            'parents': [self.dataset_folder_id]
        }

        media = MediaIoBaseUpload(stream, mimetype='text/csv', resumable=True)
        file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        print(f"\n✅ Final classification sheet uploaded: {sheet_name} (ID: {file['id']})")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-year", required=True, help="Year range to load processed CSV")
    args = parser.parse_args()

    classifier = TicketClassifier(args.year)
    classifier.run()
    classifier.save_output()
