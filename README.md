# Ticket Classification with LLMs

## Step 1: Generate Categories, Subcategories, and Tags

We used the following tools:

-   [Open WebUI](https://github.com/open-webui/open-webui)
-   [Ollama](https://ollama.com/)
-   Model: [llama3.2:latest](https://ollama.com/library/llama3.2:latest)

### How it works:

1. Start Open WebUI connected to Ollama.
2. Use a [system prompt](system_prompt.txt) to tell the model to find all possible categories, subcategories, and tags from past tickets.
3. Paste your CSV data (e.g., `tickets.csv`) into the prompt.
4. The model returns a list of categories, subcategories, and tags based on the ticket content.

## Step 2: Classify Tickets Using Python

We used the Hugging Face model [meta-llama/Llama-3.2-3B-Instruct](https://huggingface.co/meta-llama/Llama-3.2-3B-Instruct) to classify each ticket.

### Setup Instructions

1. Clone this repository:
    ```bash
    git clone https://github.com/mindyoumh/ailounge.git
    git checkout classification
    cd ailounge
    ```
2. Create a virtual environment:

    ```bash
    python -m venv venv
    venv\Scripts\activate  # For Windows
    source venv/bin/activate  # For macOS/Linux

    ```

3. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
4. Run the Jupyter notebook [classify.ipynb](classify.ipynb)
5. Output will be saved in classified_tickets.csv.
