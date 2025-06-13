import os
import sys
from dotenv import load_dotenv
from utils.google_process import GoogleProcess
import requests

load_dotenv()

API_URL = os.getenv("API_URL")

if len(sys.argv) != 2:
    print("Usage: python script.py (file_path)")
    print("Note: Use a quotation mark")
    sys.exit(1)

file_path = sys.argv[1]

google = GoogleProcess()

uploaded_csv_file_paths, uploaded_csv_id, folder_id = google.upload_all_csvs(file_path)

for fp in uploaded_csv_file_paths:
    os.remove(fp)

response = requests.post(
    f"{API_URL}/api/process_tickets",
    json={"uploaded_csv_id": str(uploaded_csv_id), "folder_id": folder_id},
)

if response.status_code == 200:
    print("SUCCESS")
else:
    print("PROCESSING FAILED")
