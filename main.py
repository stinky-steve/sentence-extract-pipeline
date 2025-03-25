from data_prep.parse_and_extract import fetch_jsonl_strings, extract_sentences
from data_prep.upload_to_minio import upload_file_to_minio
import os
import logging
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)

def main():
    """Main pipeline function to process JSONL data and upload results."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Process documents and extract sentences
        logging.info("Starting sentence extraction pipeline...")
        output_file = 'sentences.txt'
        
        # Fetch and process documents
        jsonl_data = fetch_jsonl_strings()
        extract_sentences(jsonl_data, output_file)
        
        # Upload results to MinIO
        logging.info("Uploading results to MinIO...")
        bucket_name = os.getenv('MINIO_BUCKET_NAME')
        if not bucket_name:
            raise ValueError("MINIO_BUCKET_NAME not set in environment variables")
            
        upload_file_to_minio(output_file, bucket_name, output_file)
        logging.info("Pipeline completed successfully")
        
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main() 