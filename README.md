# Ticket Categorization Automation

This repository provides an automated pipeline to clean CSV ticket files, upload them to Google Drive, categorize each ticket using the Gemini API, and write the categorized data to a new spreadsheet. The final spreadsheet is then moved to the same Google Drive folder as the uploaded CSV files.

---

📁 Folder Structure

├── main.py # Main script to run the entire automation
├── requirements.txt # Dependencies for the project
└── ticket_process/ # Django project directory

---

🚀 How to Run the Project (Locally)

0. Setup Virtual Environment and Install Requirements

Before running anything, create a virtual environment and install the required packages:

    python -m venv venv
    source venv/bin/activate   # On Windows use: venv\Scripts\activate
    pip install -r requirements.txt

---

1. Run the Django App First

The Django application hosts the necessary API endpoint that the `main.py` script calls after uploading the CSV files.
To run the Django app:

    cd ticket_process/
    python manage.py runserver

Make sure this server is running **before** you run `main.py`.

---

2. Run the Main Script Locally

After starting the Django server, you're ready to run the automation script.
Step-by-step:

1. Prepare your folder containing the raw CSV ticket files.
2. Copy the full path to the folder.
3. Run the script using:

    python main.py "/path/to/your/folder"

Replace `/path/to/your/folder` with your actual folder path.

---

🔐 Environment Variables Setup

Root `.env` file should include:

    FOLDER_ID=<Google Drive folder ID>
    GOOGLE_DRIVE_SCOPES="https://www.googleapis.com/auth/drive"
    API_URL=<The URL of the Django app>

To get `FOLDER_ID`, follow this instruction: [Google Drive Folder ID Guide](https://adventuresusingai.com/google-drive-folder-id)
Make sure the Django server is running to get the `API_URL`. By default, the API URL is http://127.0.0.1:8000

Django app `.env` file (inside `ticket_process/`) should include:

    FOLDER_ID=<Same as root .env>
    GOOGLE_DRIVE_SCOPES="https://www.googleapis.com/auth/drive"
    GOOGLE_SHEETS_SCOPES="https://www.googleapis.com/auth/spreadsheets"
    API_KEY=<Your Gemini API Key>
    MODEL_NAME="gemini-2.0-flash-lite"

To get `FOLDER_ID`, follow this instruction: [Google Drive Folder ID Guide](https://adventuresusingai.com/google-drive-folder-id)
To get `API_KEY`, follow steps from: [Gemini API Key Setup](https://ai.google.dev/gemini-api/docs/api-key)
For `MODEL_NAME` you can use the recommended model, or choose from: [Gemini Models](https://ai.google.dev/gemini-api/docs/models)
