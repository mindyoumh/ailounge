import os
import re
import json
import argparse
import tempfile
import pandas as pd
from collections import defaultdict
from dotenv import load_dotenv
from google import genai
from google.genai import types
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google.oauth2 import service_account
from utils.classification_utils import normalize_name, merge_similar_items

class TicketClassifier:
    def __init__(self, year_range: str, chunk_size: int = 100):
        load_dotenv()
        self.api_key = os.getenv("API_KEY")
        self.model_name = os.getenv("MODEL_NAME", "gemini-2.0-flash")
        self.raw_folder_id = os.getenv("RAW_DATA_FOLDER_ID")
        self.output_folder_id = os.getenv("GEMINI_ANALYSIS_OUTPUT_FOLDER_ID")
        self.year_range = year_range

        if not self.api_key:
            raise ValueError("API_KEY not found in environment variables")

        self.client = genai.Client(api_key=self.api_key)
        self.drive_service = self.authenticate_drive()

        self.csv_path = self.download_processed_csv()
        self.chunk_size = chunk_size
        self.prompt = self._load_prompt()
        self.df = self._load_csv()
        self.categories = defaultdict(set)
        self.tags = set()

    def authenticate_drive(self):
        SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), 'service_account.json')
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive']
        )
        return build('drive', 'v3', credentials=creds)

    def download_processed_csv(self):
        filename = f"Zoho Tickets {self.year_range}_processed.csv"
        query = f"'{self.raw_folder_id}' in parents and name='{filename}'"
        results = self.drive_service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])

        if not files:
            raise FileNotFoundError(f"CSV file {filename} not found in Google Drive raw_data folder")

        file_id = files[0]['id']
        request = self.drive_service.files().get_media(fileId=file_id)

        local_path = os.path.join(tempfile.gettempdir(), filename)
        with open(local_path, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        print(f"📥 Downloaded processed CSV: {filename}")
        return local_path

    def _load_prompt(self) -> str:
        prompt_path = os.path.join("prompts", "system_prompt_chunk.txt")
        with open(prompt_path, "r", encoding="utf-8") as f:
            return f.read()

    def _load_csv(self) -> pd.DataFrame:
        try:
            return pd.read_csv(self.csv_path)
        except Exception as e:
            raise ValueError(f"Failed to load CSV: {e}")

    def _extract_json_from_response(self, response) -> dict:
        raw_text = response.candidates[0].content.parts[0].text.strip()
        if raw_text.startswith("```json"):
            raw_text = raw_text.replace("```json", "").replace("```", "").strip()
        return json.loads(raw_text)

    def _process_chunk(self, chunk: pd.DataFrame):
        csv_content = chunk.to_string(index=False)
        response = self.client.models.generate_content(
            model=self.model_name,
            config=types.GenerateContentConfig(system_instruction=self.prompt),
            contents=[
                "Here is a chunk of the support ticket dataset:",
                csv_content
            ]
        )

        data = self._extract_json_from_response(response)

        for cat in data.get("categories", []):
            main_cat = normalize_name(cat["name"])
            self.categories[main_cat].update(map(normalize_name, cat.get("subcategories", [])))

        self.tags.update(map(normalize_name, data.get("tags", [])))

    def run(self):
        for i in range(0, len(self.df), self.chunk_size):
            chunk = self.df.iloc[i:i + self.chunk_size]
            try:
                print(f"\n📦 Processing chunk {i // self.chunk_size + 1}")
                self._process_chunk(chunk)
            except Exception as e:
                print(f"⚠️ Error processing chunk {i // self.chunk_size + 1}: {e}")

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