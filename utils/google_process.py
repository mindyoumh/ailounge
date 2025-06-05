import os

import datetime

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

from utils.dataparse import clean_csv_description


class GoogleProcess:
    """
    A utility class to interact with Google Drive and Google Sheets APIs.

    Provides methods to find, create, and organize folders on Google Drive,
    upload CSV files, create spreadsheets, write data into spreadsheets,
    and move files between folders.
    """

    def __init__(self):
        """
        Initializes the GoogleProcess instance by authenticating with Google APIs
        using service account credentials. Sets up the Drive and Sheets service clients.
        """
        creds = service_account.Credentials.from_service_account_file(
            "service_account.json",
            scopes=[
                os.getenv("GOOGLE_DRIVE_SCOPES"),
                os.getenv("GOOGLE_SHEETS_SCOPES"),
            ],
        )
        self.drive_service = build("drive", "v3", credentials=creds)
        self.sheets_service = build("sheets", "v4", credentials=creds)

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
        return True

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

        uploaded_csv_file_path = []
        for filename in os.listdir(file_path):
            if filename.endswith(".csv") and filename != os.path.basename(__file__):
                original_path = os.path.join(file_path, filename)
                cleaned_path = clean_csv_description(original_path)

                cleaned_name = os.path.basename(cleaned_path)
                if self.upload_file(cleaned_path, cleaned_name, folder_id):
                    uploaded_csv_file_path.append(cleaned_path)
        return uploaded_csv_file_path, folder_id

    def create_spreadsheet(self, title: str) -> str:
        """
        Creates a new Google Sheets spreadsheet with the specified title.

        Returns:
            str: The ID of the newly created spreadsheet.
        """
        spreadsheet_metadata = {"properties": {"title": title}}
        spreadsheet = (
            self.sheets_service.spreadsheets()
            .create(body=spreadsheet_metadata, fields="spreadsheetId")
            .execute()
        )
        print(f"Spreadsheet ID: {(spreadsheet.get('spreadsheetId'))}")
        return spreadsheet.get("spreadsheetId")

    def write_value_on_spreadsheet(
        self,
        spreadsheet_id: str,
        range_name: str,
        _values,
        value_input_option: str = "RAW",
    ):
        """
        Appends values to a specified range in a Google Sheets spreadsheet.

        Returns:
            dict: The API response indicating the update result.
        """
        body = {"values": _values}
        result = (
            self.sheets_service.spreadsheets()
            .values()
            .append(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption=value_input_option,
                body=body,
            )
            .execute()
        )
        print(f"{(result.get('updates').get('updatedCells'))} cells appended.")
        return result

    def move_file_to_folder(self, file_id: str, folder_id: str):
        """
        Moves a file from its current parent folder(s) to a new folder in Google Drive.

        Returns:
            list: The list of parent folder IDs after the move (should include the new folder).
        """
        file = (
            self.drive_service.files().get(fileId=file_id, fields="parents").execute()
        )
        previous_parents = ",".join(file.get("parents"))
        file = (
            self.drive_service.files()
            .update(
                fileId=file_id,
                addParents=folder_id,
                removeParents=previous_parents,
                fields="id, parents",
            )
            .execute()
        )
        return file.get("parents")
