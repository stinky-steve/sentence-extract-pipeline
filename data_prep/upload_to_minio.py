import os
import boto3
from dotenv import load_dotenv

load_dotenv()

def upload_file_to_minio(filepath, bucket_name, object_name):
    """Upload a file to MinIO bucket."""
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('MINIO_ENDPOINT'),
            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY')
        )
        
        # Check if file exists
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File {filepath} not found")
            
        # Upload file
        s3_client.upload_file(filepath, bucket_name, object_name)
        print(f"Successfully uploaded {filepath} to {bucket_name}/{object_name}")
        
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")
        raise

if __name__ == "__main__":
    bucket_name = os.getenv('MINIO_BUCKET_NAME')
    upload_file_to_minio('sentences.txt', bucket_name, 'sentences.txt') 