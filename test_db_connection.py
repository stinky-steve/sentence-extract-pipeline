import os
import psycopg
from dotenv import load_dotenv

def test_postgres_connection():
    """Test connection to PostgreSQL database."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Construct connection string
        conn_info = f"host={os.getenv('PG_HOST')} port={os.getenv('PG_PORT')} dbname={os.getenv('PG_DB')} user={os.getenv('PG_USER')} password={os.getenv('PG_PASSWORD')}"
        
        print("Attempting to connect to PostgreSQL...")
        print(f"Host: {os.getenv('PG_HOST')}")
        print(f"Database: {os.getenv('PG_DB')}")
        
        # Try to connect
        with psycopg.connect(conn_info) as conn:
            with conn.cursor() as cur:
                # Execute a simple query
                cur.execute("SELECT version();")
                version = cur.fetchone()[0]
                print("\nSuccessfully connected to PostgreSQL!")
                print(f"PostgreSQL version: {version}")
                
    except Exception as e:
        print(f"\nError connecting to PostgreSQL: {e}")
        raise

if __name__ == "__main__":
    test_postgres_connection() 