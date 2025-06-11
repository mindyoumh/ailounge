import os
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

        self.categories = defaultdict(set)
        self.tags = set()
        self.lock = Lock()  # For thread-safe updates

    def authenticate_drive(self):
        SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), 'service_account.json')
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=[
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/spreadsheets.readonly'
            ]
        )
        self.sheets_service = build('sheets', 'v4', credentials=creds)
        return build('drive', 'v3', credentials=creds)

    def load_sheet_as_df(self):
        sheet_name = f"Zoho Tickets {self.year_range}_cleaned"
        query = f"name='{sheet_name}' and mimeType='application/vnd.google-apps.spreadsheet'"
        results = self.drive_service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])

        if not files:
            raise FileNotFoundError(f"Google Sheet '{sheet_name}' not found in Drive")

        sheet_id = files[0]['id']
        print(f"📄 Found Google Sheet: {sheet_name} (ID: {sheet_id})")

        sheet_metadata = self.sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        first_sheet_title = sheet_metadata['sheets'][0]['properties']['title']
        sheet_range = f"{first_sheet_title}"
        result = self.sheets_service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range=sheet_range
        ).execute()

        rows = result.get("values", [])
        if not rows:
            raise ValueError(f"No data found in sheet '{sheet_name}'")

        df = pd.DataFrame(rows[1:], columns=rows[0])
        print(f"📊 Loaded sheet into DataFrame: {df.shape[0]} rows")
        return df

    def _load_prompt(self) -> str:
        prompt_path = os.path.join("prompts", "system_prompt_chunk.txt")
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
                    self.categories[main_cat].update(map(normalize_name, cat.get("subcategories", [])))
                self.tags.update(map(normalize_name, data.get("tags", [])))
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
                    "subcategories": sorted(list(merge_similar_items(subcats)))
                }
                for main_cat, subcats in self.categories.items()
            ],
            "tags": sorted(list(merge_similar_items(self.tags)))
        }

        filename = f"final_categories_tags({self.year_range}).json"
        local_path = os.path.join(tempfile.gettempdir(), filename)

        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)

        file_metadata = {
            'name': filename,
            'parents': [self.raw_folder_id]
        }
        media = MediaIoBaseUpload(open(local_path, 'rb'), mimetype='application/json')
        uploaded_file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        print(f"\n✅ Final aggregated result uploaded to Drive (ID: {uploaded_file['id']})")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-year", required=True, help="Year range to load processed CSV")
    args = parser.parse_args()

    classifier = TicketClassifier(args.year)
    classifier.run()
    classifier.save_output()
