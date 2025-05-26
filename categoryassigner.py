import requests
import json
import pandas as pd
from tqdm import tqdm  # Import the tqdm library for progress bars
from dotenv import load_dotenv
import os

load_dotenv() 
API_KEY = os.getenv("API_KEY")
API_URL = os.getenv("API_URL")
MODEL = os.getenv("MODEL")


def get_api_response(prompt, api_key):
    url = API_URL
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": MODEL,
        "messages": [{"role": "user", "content": prompt}]
    }
    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def categorize_ticket(subject, i):
    prompt = f"{subject}"
    response_data = get_api_response(prompt, API_KEY)
    
    if response_data is not None and "choices" in response_data and len(response_data["choices"]) > 0:
        category_with_subcategory = response_data["choices"][0]["message"]["content"].strip()
        if "|" in category_with_subcategory:
            categories = category_with_subcategory.split('|')
            if len(categories) == 2:
                return {
                    'Category': categories[0].strip(),
                    'Sub Category': categories[1].strip()
                }
            else:
                print(f"Failed to categorize: {subject}, due length of categories being more than 2. Row: {i}")
                return None
        else:
                print(f"Failed to categorize: {subject}, due to invalid response: {category_with_subcategory}. Row: {i}")
                return None
    else:
        print(f"Failed to categorize {subject}")
        return None

def process_csv(csv_file):
    df = pd.read_csv(csv_file)
    
    for i, subject in enumerate(df['Subject'], start=1):
        if not subject:  
            print(f"Skip ticket: {i}, due to null value.")
            continue
        elif subject.strip() == "(No Subject)":
            print(f"Skip ticket: {i}, due to (No Subject).")
            continue
        response_data = get_api_response(subject, API_KEY)
        
        if response_data is not None:
            category_info = categorize_ticket(subject, i)
            if category_info:
                df.loc[df['Subject'] == subject, 'Category'] = category_info['Category']
                df.loc[df['Subject'] == subject, 'Sub Category'] = category_info['Sub Category']
        else:
            print(f"Failed to categorize ticket: {i}, {subject}")
        
        print(f"Categorized {i} out of {len(df)} subjects", end='\r')
    
    df.to_csv(csv_file, index=False)
    print(f"\nCSV file '{csv_file}' updated with categories.")

csv_file = os.getenv("csvfile")

process_csv(csv_file)