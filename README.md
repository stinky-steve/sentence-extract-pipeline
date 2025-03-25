# Sentence Extraction and MinIO Upload Pipeline

This project implements a data preprocessing pipeline that:
1. Fetches JSONL-formatted text data from a PostgreSQL database
2. Extracts individual sentences using spaCy's sentence segmentation
3. Saves the processed sentences to a text file
4. Uploads the results to a MinIO bucket

## Setup

1. Create and activate the Conda environment:
```bash
conda create -n sentence_extract python=3.10
conda activate sentence_extract
```

2. Install dependencies:
```bash
conda install -c conda-forge python-dotenv psycopg spacy boto3
python -m spacy download en_core_web_sm
```

3. Configure environment variables:
   - Copy `.env.example` to `.env`
   - Update the credentials in `.env` with your PostgreSQL and MinIO credentials

## Project Structure

```
.
├── data_prep/
│   ├── __init__.py
│   ├── parse_and_extract.py
│   └── upload_to_minio.py
├── .env
├── .gitignore
├── main.py
├── requirements.txt
├── test_db_connection.py
└── test_minio_connection.py
```

## Usage

1. Test database connections:
```bash
python test_db_connection.py
python test_minio_connection.py
```

2. Run the main pipeline:
```bash
python main.py
```

## Environment Variables

Required environment variables in `.env`:

### PostgreSQL
- `PG_HOST`: Database host
- `PG_PORT`: Database port
- `PG_DB`: Database name
- `PG_USER`: Database user
- `PG_PASSWORD`: Database password

### MinIO
- `MINIO_ENDPOINT`: MinIO server endpoint
- `MINIO_ACCESS_KEY`: MinIO access key
- `MINIO_SECRET_KEY`: MinIO secret key
- `MINIO_BUCKET_NAME`: Target bucket name 