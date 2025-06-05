import os
import sys
import csv
from dotenv import load_dotenv
from utils.google_process import GoogleProcess
from utils.ticket_processor import TicketClassifierUsingGemini
from pathlib import Path
from utils.dataparse import string_to_values

load_dotenv()

if len(sys.argv) != 2:
    print("Usage: python script.py (file_path)")
    print("Note: Use a quotation mark")
    sys.exit(1)

file_path = sys.argv[1]

google = GoogleProcess()
ticket = TicketClassifierUsingGemini()

uploaded_csv_file_paths, folder_id = google.upload_all_csvs(file_path)

for path in uploaded_csv_file_paths:
    filename = Path(path).name
    spreadsheet_id = google.create_spreadsheet(filename)

    with open(path, newline="", encoding="utf-8") as csvfile:
        reader = list(csv.reader(csvfile))
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

for fp in uploaded_csv_file_paths:
    os.remove(fp)
