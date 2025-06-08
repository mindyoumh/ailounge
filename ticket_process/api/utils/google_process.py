import os


from googleapiclient.discovery import build
from google.oauth2 import service_account


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

    def get_file_name(self, file_id: str):
        file = self.drive_service.files().get(fileId=file_id, fields="name").execute()
        return file.get("name")

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
