import sys
import os
import io
import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import service_account

# Add parent to path for utils import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.dataparse import clean_csv_description

load_dotenv()

class GoogleDriveUploader:
    def __init__(self):
        self.api_scopes = [os.getenv('GOOGLE_DRIVE_API')]
        self.folder_id = os.getenv('RAW_DATA_FOLDER_ID')
        self.service_account_file = os.path.join(os.path.dirname(__file__), '..', 'service_account.json')
        self.columns_to_include = [
            "Ticket Id", "Ticket Reference Id", "Subject", "Description",
            "Category", "Sub Category", "Tags"
        ]
        self.raw_dir = os.path.join(os.path.dirname(__file__), 'raw_csvs')
        self.service = self.authenticate()

    def authenticate(self):
        creds = service_account.Credentials.from_service_account_file(
            self.service_account_file, scopes=self.api_scopes
        )
        return build('drive', 'v3', credentials=creds)

    def upload_dataframe_as_sheet(self, df: pd.DataFrame, sheet_name: str) -> bool:
        csv_bytes = df.to_csv(index=False).encode('utf-8')
        stream = io.BytesIO(csv_bytes)

        file_metadata = {
            'name': sheet_name,
            'mimeType': 'application/vnd.google-apps.spreadsheet',
            'parents': [self.folder_id]
        }

        media = MediaIoBaseUpload(stream, mimetype='text/csv', resumable=True)
        file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        print(f"📤 Uploaded as Google Sheet: {sheet_name} (ID: {file.get('id')})")
        return True

    def process_and_upload_csv(self, filename: str):
        original_path = os.path.join(self.raw_dir, filename)
        processed_path = clean_csv_description(original_path)

        df = pd.read_csv(processed_path)
        df_filtered = df[[col for col in self.columns_to_include if col in df.columns]]

        sheet_name = os.path.splitext(filename)[0] + "_cleaned"
        if self.upload_dataframe_as_sheet(df_filtered, sheet_name):
            try:
                os.remove(original_path)
                os.remove(processed_path)
                print(f"🧹 Deleted local files: {filename}, {os.path.basename(processed_path)}")
            except Exception as e:
                print(f"⚠️ Failed to delete local files: {e}")

    def upload_all_csvs(self):
        for filename in os.listdir(self.raw_dir):
            if filename.endswith('.csv'):
                self.process_and_upload_csv(filename)

if __name__ == '__main__':
    uploader = GoogleDriveUploader()
    uploader.upload_all_csvs()
