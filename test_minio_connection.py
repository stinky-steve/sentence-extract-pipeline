import os
import boto3
from dotenv import load_dotenv

def test_minio_connection():
    """Test connection to MinIO server."""
    try:
        # Load environment variables
        load_dotenv()
        
        print("Attempting to connect to MinIO...")
        print(f"Endpoint: {os.getenv('MINIO_ENDPOINT')}")
        print(f"Bucket: {os.getenv('MINIO_BUCKET_NAME')}")
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('MINIO_ENDPOINT'),
            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY')
        )
        
        # Try to list buckets
        buckets = s3_client.list_buckets()
        print("\nSuccessfully connected to MinIO!")
        print("Available buckets:")
        for bucket in buckets['Buckets']:
            print(f"- {bucket['Name']}")
            
        # Try to access the specific bucket
        bucket_name = os.getenv('MINIO_BUCKET_NAME')
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"\nSuccessfully accessed bucket: {bucket_name}")
        
    except Exception as e:
        print(f"\nError connecting to MinIO: {e}")
        raise

if __name__ == "__main__":
    test_minio_connection() 