import os

import datetime

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

from utils.dataparse import clean_csv_description


class GoogleProcess:
    """
    A utility class to interact with Google Drive APIs.

    Provides methods to find, create, and organize folders on Google Drive.
    """

    def __init__(self):
        """
        Initializes the GoogleProcess instance by authenticating with Google APIs
        using service account credentials. Sets up the Drive clients.
        """
        creds = service_account.Credentials.from_service_account_file(
            "service_account.json",
            scopes=[
                os.getenv("GOOGLE_DRIVE_SCOPES"),
            ],
        )
        self.drive_service = build("drive", "v3", credentials=creds)

    def find_folder(self, folder_name: str):
        """
        Searches for a Google Drive folder by name.

        Returns:
            str or None: The ID of the first folder found with the given name,
                        or None if no such folder exists.
        """
        query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        results = (
            self.drive_service.files()
            .list(q=query, spaces="drive", fields="files(id, name)")
            .execute()
        )
        folders = results.get("files", [])
        if folders:
            return folders[0]["id"]
        else:
            return None

    def create_folder(self, folder_name: str):
        """
        Creates a new folder in Google Drive under a parent folder specified
        by the environment variable 'FOLDER_ID'.

        Returns:
            str: The ID of the newly created folder.
        """
        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [os.getenv("FOLDER_ID")],
        }
        folder = (
            self.drive_service.files().create(body=file_metadata, fields="id").execute()
        )
        return folder.get("id")

    def upload_file(self, file_path: str, file_name: str, folder_id: str):
        """
        Uploads a file to a specified folder on Google Drive.

        Returns:
            bool: True if the upload was successful.
        """
        file_metadata = {
            "name": file_name,
            "parents": [folder_id],
        }
        media = MediaFileUpload(file_path, mimetype="text/csv")
        file = (
            self.drive_service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )
        print(f"📤 Uploaded: {file_name} (ID: {file.get('id')})")
        return True, file.get("id")

    def upload_all_csvs(self, file_path: str):
        """
        Uploads all CSV files found in the given directory to a dated folder
        on Google Drive. If the folder for today's date does not exist, it is created.

        Returns:
            tuple: (list of str, str) where the list contains paths of uploaded CSV files,
                and the str is the ID of the folder they were uploaded to.
        """
        today = datetime.datetime.today()
        folder_name = today.strftime("%m/%d/%Y")

        folder_id = self.find_folder(folder_name)
        if not folder_id:
            folder_id = self.create_folder(folder_name)

        uploaded_csv_id = []
        uploaded_csv_file_paths = []
        for filename in os.listdir(file_path):
            if filename.endswith(".csv") and filename != os.path.basename(__file__):
                original_path = os.path.join(file_path, filename)
                cleaned_path = clean_csv_description(original_path)
                cleaned_name = os.path.basename(cleaned_path)

                is_success, file_id = self.upload_file(
                    cleaned_path, cleaned_name, folder_id
                )

                if is_success:
                    uploaded_csv_id.append(file_id)
                    uploaded_csv_file_paths.append(cleaned_path)

        return uploaded_csv_file_paths, uploaded_csv_id, folder_id
