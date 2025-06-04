import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from utils.dataparse import clean_csv_description


load_dotenv()

GOOGLE_DRIVE_API = [os.getenv('GOOGLE_DRIVE_API')]
RAW_DATA_FOLDER_ID = os.getenv('RAW_DATA_FOLDER_ID')
SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), '..', 'service_account.json')

def authenticate():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=GOOGLE_DRIVE_API
    )
    return build('drive', 'v3', credentials=creds)

def upload_file(service, file_path, file_name):
    file_metadata = {
        'name': file_name,
        'parents': [RAW_DATA_FOLDER_ID]
    }
    media = MediaFileUpload(file_path, mimetype='text/csv')
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f"📤 Uploaded: {file_name} (ID: {file.get('id')})")
    return True

def upload_all_csvs():
    service = authenticate()
    current_dir = os.path.dirname(__file__)

    for filename in os.listdir(current_dir):
        if filename.endswith('.csv') and filename != os.path.basename(__file__):
            original_path = os.path.join(current_dir, filename)
            processed_path = clean_csv_description(original_path)  # output will be _processed.csv

            processed_name = os.path.basename(processed_path)
            if upload_file(service, processed_path, processed_name):
                try:
                    os.remove(original_path)
                    os.remove(processed_path)
                    print(f"🧹 Deleted local files: {filename}, {processed_name}")
                except Exception as e:
                    print(f"⚠️ Failed to delete local files: {e}")

if __name__ == '__main__':
    upload_all_csvs()
