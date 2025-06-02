import os
import re
import json
import pandas as pd
from collections import defaultdict
from dotenv import load_dotenv
from google import genai
from google.genai import types
from utils.classification_utils import normalize_name, merge_similar_items

class TicketClassifier:
    def __init__(self, csv_path: str, chunk_size: int = 100):
        load_dotenv()
        self.api_key = os.getenv("API_KEY")
        self.model_name = os.getenv("MODEL_NAME", "gemini-2.0-flash")

        if not self.api_key:
            raise ValueError("API_KEY not found in environment variables")

        self.client = genai.Client(api_key=self.api_key)
        self.csv_path = csv_path
        self.chunk_size = chunk_size
        self.prompt = self._load_prompt()
        self.df = self._load_csv()
        self.categories = defaultdict(set)
        self.tags = set()

    def _load_prompt(self) -> str:
        prompt_path = os.path.join("prompts", "system_prompt_chunk.txt")
        with open(prompt_path, "r", encoding="utf-8") as f:
            return f.read()

    def _load_csv(self) -> pd.DataFrame:
        try:
            return pd.read_csv(self.csv_path)
        except Exception as e:
            raise ValueError(f"Failed to load CSV: {e}")

    def _extract_json_from_response(self, response) -> dict:
        raw_text = response.candidates[0].content.parts[0].text.strip()
        if raw_text.startswith("```json"):
            raw_text = raw_text.replace("```json", "").replace("```", "").strip()
        return json.loads(raw_text)

    def _process_chunk(self, chunk: pd.DataFrame):
        csv_content = chunk.to_string(index=False)
        response = self.client.models.generate_content(
            model=self.model_name,
            config=types.GenerateContentConfig(system_instruction=self.prompt),
            contents=[
                "Here is a chunk of the support ticket dataset:",
                csv_content
            ]
        )

        data = self._extract_json_from_response(response)

        for cat in data.get("categories", []):
            main_cat = normalize_name(cat["name"])
            self.categories[main_cat].update(map(normalize_name, cat.get("subcategories", [])))

        self.tags.update(map(normalize_name, data.get("tags", [])))

    def run(self):
        for i in range(0, len(self.df), self.chunk_size):
            chunk = self.df.iloc[i:i + self.chunk_size]
            try:
                print(f"\n📦 Processing chunk {i // self.chunk_size + 1}")
                self._process_chunk(chunk)
            except Exception as e:
                print(f"⚠️ Error processing chunk {i // self.chunk_size + 1}: {e}")

    def save_output(self):
        output = {
            "categories": [
                {
                    "name": main_cat.title(),
                    "subcategories": sorted(list(merge_similar_items(subcats)))
                }
                for main_cat, subcats in self.categories.items()
            ],
            "tags": sorted(list(merge_similar_items(self.tags)))
        }

        match = re.search(r"(\d{4}-\d{4})", os.path.basename(self.csv_path))
        date_range = match.group(1) if match else "unknown"
        output_filename = f"final_categories_tags({date_range}).json"
        output_path = os.path.join("outputs", output_filename)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)

        print(f"\n✅ Final aggregated result saved to '{output_path}'")

if __name__ == "__main__":
    classifier = TicketClassifier("data/Zoho Tickets 2021-2024_cleaned.csv")
    classifier.run()
    classifier.save_output()
