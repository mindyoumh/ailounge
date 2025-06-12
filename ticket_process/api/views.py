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
from google.genai.errors import ServerError
import time


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
        google.move_file_to_folder(spreadsheet_id, folder_id)

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

        MAX_TOKEN_LIMIT = 100_000
        CHUNK_SIZE = 100

        def flush_chunk(chunk):
            if not chunk:
                return []
            result_text = ticket.classify_tickets(
                [
                    [row[ticket_ID], row[subject], row[description], row[resolution]]
                    for row in chunk
                ]
            )
            return string_to_values(result_text)

        google.write_value_on_spreadsheet(
            spreadsheet_id=spreadsheet_id, range_name="A1", _values=[header]
        )
        current_row = 2

        i = 0
        while i < len(data_rows):
            chunk = data_rows[i : i + CHUNK_SIZE]

            chunk_text = ""
            for row in chunk:
                chunk_text += f"""
                    Ticket ID: {row[ticket_ID]}
                    Subject: {row[subject]}
                    Description: {row[description]}
                    Resolution: {row[resolution]}
                """

            chunk_token_count = ticket.count_token(ticket.prompt + chunk_text)

            if chunk_token_count <= MAX_TOKEN_LIMIT:
                print(
                    f"✅ Processing chunk from row {i} to {i+len(chunk)} (Token count: {chunk_token_count})"
                )
                try:
                    parsed_results = flush_chunk(chunk)
                    for row, parsed in zip(chunk, parsed_results):
                        if isinstance(parsed, dict):
                            row[category] = parsed.get("Category", "")
                            row[sub_category] = parsed.get("Sub Category", "")
                            row[tags] = ", ".join(parsed.get("Tags", []))
                        else:
                            print(f"⚠️ Unexpected parsed format: {parsed}")

                    google.write_value_on_spreadsheet(
                        spreadsheet_id=spreadsheet_id,
                        range_name=f"A{current_row}",
                        _values=chunk,
                    )
                    current_row += len(chunk)

                    time.sleep(60)
                except ServerError as e:
                    print(f"⚠️ Gemini failed for this batch: {e}")
                i += len(chunk)
            else:
                CHUNK_SIZE = max(1, CHUNK_SIZE // 2)
                print(
                    f"❌ Skipping chunk from row {i} to {i+len(chunk)}: token count {chunk_token_count} exceeds {MAX_TOKEN_LIMIT}"
                )

        updated_data = [header] + data_rows

        google.write_value_on_spreadsheet(
            spreadsheet_id=spreadsheet_id, range_name="A1", _values=updated_data
        )

    return Response("SUCCESS", status=status.HTTP_200_OK)
