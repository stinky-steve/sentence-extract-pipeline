# Project: JSONL Sentence Extraction & MinIO Upload Pipeline

### Goal:
Set up a Python environment and scripts to:
1. Connect to a PostgreSQL database and fetch JSONL-formatted strings.
2. Parse the JSONL, segmenting text into clean, individual sentences using spaCy.
3. Save extracted sentences in a newline-separated plain text file (`sentences.txt`).
4. Upload this processed file to a MinIO bucket hosted on a private server.

### Workflow Steps:

**Step 1: Environment Setup**
- Create and activate a Python virtual environment.
- Install all dependencies listed in `requirements.txt`.

```shell
python -m venv env
source env/bin/activate  # On Windows: env\Scripts\activate
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

**Step 2: Load Environment Variables**
- Ensure your `.env` file is correctly populated and loaded into the Python environment.

**Step 3: Fetch JSONL Data from PostgreSQL**
- Use psycopg to connect and execute a SQL query fetching JSONL strings from the database.

**Step 4: Sentence Extraction**
- Use spaCy's `en_core_web_sm` model to accurately segment each text into sentences.
- Implement basic sentence filtering to exclude:
  - Sentences shorter than 10 characters.
  - Sentences longer than 500 characters.

**Step 5: Save and Upload Results**
- Save the processed sentences in `sentences.txt`.
- Use boto3 to upload the file to MinIO.

### File Outputs:
- `sentences.txt`: Final processed sentences ready for fine-tuning use.

### Notes:
- Write clean, well-commented, and modular Python code.
- Handle exceptions gracefully, particularly database or MinIO connection issues.
- Maintain a clear separation between data processing and uploading functionality. 