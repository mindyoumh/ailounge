import os
from google import genai
from google.genai import types
import pandas as pd
import json
from collections import defaultdict
from dotenv import load_dotenv
from utils.classification_utils import normalize_name, merge_similar_items

# Load environment variables from .env file
load_dotenv()
API_KEY = os.getenv("API_KEY")
MODEL_NAME = os.getenv("MODEL_NAME", "gemini-2.0-flash")  # default if missing

if not API_KEY:
    raise ValueError("API_KEY not found in environment variables")

client = genai.Client(api_key=API_KEY)

# Step 1: Load the system prompt from file
prompt_path = os.path.join("prompts", "system_prompt_chunk.txt")
with open(prompt_path, "r", encoding="utf-8") as f:
    system_prompt_chunk = f.read()

# Step 2: Load the full CSV from data folder
csv_path = os.path.join("data", "Zoho Tickets 2021-2024_cleaned.csv")
try:
    df = pd.read_csv(csv_path)
except Exception as e:
    print(f"Error reading CSV file: {e}")
    exit()

# Step 3: Process each chunk
chunk_size = 100
all_categories = defaultdict(set)
all_tags = set()

for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i:i + chunk_size]
    csv_content = chunk.to_string(index=False)

    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            config=types.GenerateContentConfig(system_instruction=system_prompt_chunk),
            contents=[
                "Here is a chunk of the support ticket dataset:",
                csv_content
            ]
        )
        
        print(f"=== Gemini Response for chunk {i//chunk_size + 1} ===")
        print(response)

        raw_text = response.candidates[0].content.parts[0].text.strip()

        # Remove Markdown formatting if present
        if raw_text.startswith("```json"):
            raw_text = raw_text.replace("```json", "").replace("```", "").strip()

        # Parse the cleaned JSON string
        data = json.loads(raw_text)

        # Collect categories
        for cat in data.get("categories", []):
            main_cat = normalize_name(cat["name"])
            all_categories[main_cat].update(map(normalize_name, cat.get("subcategories", [])))

        # Collect tags
        all_tags.update(map(normalize_name, data.get("tags", [])))

    except Exception as e:
        print(f"Error processing chunk {i//chunk_size + 1}: {e}")

# Step 4: Aggregate and format the results
final_output = {
    "categories": [
        {
            "name": main_cat.title(),
            "subcategories": sorted(list(merge_similar_items(subcats)))
        }
        for main_cat, subcats in all_categories.items()
    ],
    "tags": sorted(list(merge_similar_items(all_tags)))
}

# Step 5: Print the final aggregated result
final_json_str = json.dumps(final_output, indent=2)
print(final_json_str)

# Step 6: Save the result to outputs folder
output_path = os.path.join("outputs", "final_categories_tags.json")
with open(output_path, "w", encoding="utf-8") as f:
    f.write(final_json_str)

print(f"\n✅ Final aggregated result saved to '{output_path}'")
