import sys
import os
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
        load_dotenv()

        self.api_key = os.getenv("API_KEY")
        self.model_name = os.getenv("MODEL_NAME", "gemini-2.0-flash")
        self.raw_folder_id = os.getenv("RAW_DATA_FOLDER_ID")
        self.output_folder_id = os.getenv("GEMINI_ANALYSIS_OUTPUT_FOLDER_ID")
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
        # Step 1: Locate folder for this dataset (e.g., Zoho Tickets 2024-2025)
        folder_name = f"Zoho Tickets {self.year_range}"
        folder_query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
        folder_results = self.drive_service.files().list(q=folder_query, fields="files(id, name)").execute()
        folders = folder_results.get('files', [])
        if not folders:
            raise FileNotFoundError(f"❌ Folder '{folder_name}' not found in Drive")
        self.dataset_folder_id = folders[0]['id']
        print(f"📁 Found Drive folder: {folder_name} (ID: {self.dataset_folder_id})")

        # Step 2: Locate the _processed Google Sheet inside that folder
        sheet_query = f"'{self.dataset_folder_id}' in parents and name contains '_processed' and mimeType='application/vnd.google-apps.spreadsheet'"
        sheet_results = self.drive_service.files().list(q=sheet_query, fields="files(id, name)").execute()
        sheets = sheet_results.get('files', [])
        if not sheets:
            raise FileNotFoundError(f"❌ No '_processed' sheet found in '{folder_name}'")

        sheet_id = sheets[0]['id']
        print(f"📄 Found Google Sheet: {sheets[0]['name']} (ID: {sheet_id})")

        # Step 3: Load sheet data
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
        prompt_path = os.path.join(os.path.dirname(__file__), 'prompts', 'system_prompt_chunk.txt')
        with open(prompt_path, "r", encoding="utf-8") as f:
            return f.read()

    def _extract_json_from_response(self, response) -> dict:
        raw_text = response.candidates[0].content.parts[0].text.strip()
        if raw_text.startswith("```json"):
            raw_text = raw_text.replace("```json", "").replace("```", "").strip()
        return json.loads(raw_text)

    def _process_chunk(self, chunk: pd.DataFrame, chunk_index: int):
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
        futures = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for i in range(0, len(self.df), self.chunk_size):
                chunk = self.df.iloc[i:i + self.chunk_size]
                futures.append(executor.submit(self._process_chunk, chunk, i // self.chunk_size))
            for future in as_completed(futures):
                _ = future.result()

    def save_output(self):
        output = {
            "categories": [
                {
                    "name": main_cat.title(),
                    "subcategories": [
                        {
                            "name": subcat.title(),
                            "tags": sorted(list(merge_similar_items(tags)))
                        }
                        for subcat, tags in subcats.items()
                    ]
                }
                for main_cat, subcats in self.categories.items()
            ]
        }

        filename = f"{self.year_range}_result.json"
        local_path = os.path.join(tempfile.gettempdir(), filename)

        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)

        file_metadata = {
            'name': filename,
            'parents': [self.dataset_folder_id]
        }
        media = MediaIoBaseUpload(open(local_path, 'rb'), mimetype='application/json')
        uploaded_file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        print(f"\n✅ Final classification result uploaded to Drive folder '{self.year_range}' (ID: {uploaded_file['id']})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-year", required=True, help="Year range to load processed CSV")
    args = parser.parse_args()

    classifier = TicketClassifier(args.year)
    classifier.run()
    classifier.save_output()
