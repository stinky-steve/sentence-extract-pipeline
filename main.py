from data_prep.parse_and_extract import fetch_jsonl_strings, extract_sentences, save_sentences
from data_prep.upload_to_minio import upload_file_to_minio
import os
from dotenv import load_dotenv

def main():
    """Main function to orchestrate the data processing and upload pipeline."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Step 1: Fetch JSONL data
        print("Fetching JSONL data from PostgreSQL...")
        jsonl_data = fetch_jsonl_strings()
        
        # Step 2: Extract sentences
        print("Extracting sentences from JSONL data...")
        sentences = extract_sentences(jsonl_data)
        
        # Step 3: Save sentences to file
        output_file = 'sentences.txt'
        print(f"Saving {len(sentences)} sentences to {output_file}...")
        save_sentences(sentences, output_file)
        
        # Step 4: Upload to MinIO
        bucket_name = os.getenv('MINIO_BUCKET_NAME')
        print(f"Uploading {output_file} to MinIO bucket {bucket_name}...")
        upload_file_to_minio(output_file, bucket_name, output_file)
        
        print("Pipeline completed successfully!")
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main() 