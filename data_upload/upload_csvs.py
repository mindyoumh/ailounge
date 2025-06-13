import sys
import os
import io
import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import service_account

# Add parent path to import dataparse
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.dataparse import clean_csv_description

load_dotenv()

class GoogleDriveUploader:
    def __init__(self):
        """
        Initializes the uploader by loading environment variables,
        authenticating with Google Drive, and preparing upload parameters.
        """
        
        self.api_scopes = [os.getenv('GOOGLE_DRIVE_API')]
        self.root_folder_id = os.getenv('GOOGLE_DRIVE_FOLDER_ID')
        self.service_account_file = os.path.join(os.path.dirname(__file__), '..', 'service_account.json')
        self.columns_to_include = [
            "Ticket Id", "Ticket Reference Id", "Subject", "Description",
            "Category", "Sub Category", "Tags"
        ]
        self.raw_dir = os.path.join(os.path.dirname(__file__), 'raw_csvs')
        self.service = self.authenticate()

    def authenticate(self):
        """
        Authenticates with Google Drive using a service account.

        Returns:
            googleapiclient.discovery.Resource: Google Drive API client.
        """
        
        creds = service_account.Credentials.from_service_account_file(
            self.service_account_file, scopes=self.api_scopes
        )
        return build('drive', 'v3', credentials=creds)

    def create_folder(self, folder_name: str, parent_id: str) -> str:
        """
        Creates a new folder inside the specified parent folder on Google Drive.

        Args:
            folder_name (str): The name of the new folder.
            parent_id (str): The parent folder ID in which to create the new folder.

        Returns:
            str: The ID of the newly created folder.
        """
        
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        folder = self.service.files().create(body=file_metadata, fields='id').execute()
        print(f"📁 Created Drive folder: {folder_name} (ID: {folder['id']})")
        return folder['id']

    def upload_dataframe_as_sheet(self, df: pd.DataFrame, sheet_name: str, folder_id: str) -> bool:
        """
        Uploads a Pandas DataFrame as a Google Sheet to the specified Drive folder.

        Args:
            df (pd.DataFrame): The DataFrame to upload.
            sheet_name (str): The name of the resulting Google Sheet.
            folder_id (str): The Drive folder ID to upload into.

        Returns:
            bool: True if upload was successful.
        """
        
        csv_bytes = df.to_csv(index=False).encode('utf-8')
        stream = io.BytesIO(csv_bytes)

        file_metadata = {
            'name': sheet_name,
            'mimeType': 'application/vnd.google-apps.spreadsheet',
            'parents': [folder_id]
        }

        media = MediaIoBaseUpload(stream, mimetype='text/csv', resumable=True)
        file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        print(f"📤 Uploaded Google Sheet: {sheet_name} to folder {folder_id} (ID: {file.get('id')})")
        return True

    def process_and_upload_csv(self, filename: str):
        """
        Processes a single CSV file: cleans it, filters required columns,
        uploads it to Google Drive in a subfolder, and deletes the local files.

        Args:
            filename (str): The CSV filename to process (from raw_csvs/ directory).
        """
        
        csv_path = os.path.join(self.raw_dir, filename)
        processed_path = clean_csv_description(csv_path)

        df = pd.read_csv(processed_path)
        df_filtered = df[[col for col in self.columns_to_include if col in df.columns]]

        base_name = os.path.splitext(filename)[0].strip()
        year_part = base_name.split()[-1]
        cleaned_sheet_name = f"{year_part}_processed"

        subfolder_id = self.create_folder(base_name, self.root_folder_id)

        if self.upload_dataframe_as_sheet(df_filtered, cleaned_sheet_name, subfolder_id):
            try:
                os.remove(csv_path)
                os.remove(processed_path)
                print(f"🧹 Deleted local files: {filename}, {os.path.basename(processed_path)}")
            except Exception as e:
                print(f"⚠️ Failed to delete local files: {e}")

    def upload_all_csvs(self):
        """
        Processes and uploads all CSV files in the raw_csvs/ directory.
        Each file gets its own subfolder in Google Drive.
        """
        for filename in os.listdir(self.raw_dir):
            if filename.endswith('.csv'):
                self.process_and_upload_csv(filename)

if __name__ == '__main__':
    uploader = GoogleDriveUploader()
    uploader.upload_all_csvs()
