from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
import ast
import io
import csv
from .utils.google_process import GoogleProcess
from .utils.ticket_processor import TicketClassifierUsingGemini
from .utils.dataparse import string_to_values
from googleapiclient.http import MediaIoBaseDownload


@api_view(["POST"])
def process_ticket(request):
    google = GoogleProcess()
    ticket = TicketClassifierUsingGemini()

    uploaded_csv_id = request.data.get("uploaded_csv_id")
    folder_id = request.data.get("folder_id")

    uploaded_csv_id = ast.literal_eval(uploaded_csv_id)

    for file_id in uploaded_csv_id:
        filename = google.get_file_name(file_id)
        spreadsheet_id = google.create_spreadsheet(filename)

        request = google.drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        fh.seek(0)
        csv_text = fh.read().decode("utf-8")
        reader = list(csv.reader(io.StringIO(csv_text)))

        header = reader[0]
        data_rows = reader[1:]

        try:
            category = header.index("Category")
            sub_category = header.index("Sub Category")
            tags = header.index("Tags")
            ticket_ID = header.index("Ticket Id")
            subject = header.index("Subject")
            description = header.index("Description")
            resolution = header.index("Resolution")
        except ValueError as e:
            print(f"{e}. Required column missing in {filename}. Skipping.")
            continue

        for row in data_rows:
            category_value, sub_category_value, tags_value = string_to_values(
                ticket.classify_each_ticket(
                    row[ticket_ID], row[subject], row[description], row[resolution]
                )
            )
            row[category] = category_value
            row[sub_category] = sub_category_value
            row[tags] = tags_value

        updated_data = [header] + data_rows

        google.write_value_on_spreadsheet(
            spreadsheet_id=spreadsheet_id, range_name="A1", _values=updated_data
        )

    google.move_file_to_folder(spreadsheet_id, folder_id)

    return Response("SUCCESS", status=status.HTTP_200_OK)
