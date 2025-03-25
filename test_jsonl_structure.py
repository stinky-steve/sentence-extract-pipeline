import os
import json
import psycopg
from dotenv import load_dotenv
from pprint import pprint

def examine_single_jsonl():
    """Fetch and examine a single JSONL record from the database."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Construct connection string
        conn_info = f"host={os.getenv('PG_HOST')} port={os.getenv('PG_PORT')} dbname={os.getenv('PG_DB')} user={os.getenv('PG_USER')} password={os.getenv('PG_PASSWORD')}"
        
        print("Connecting to PostgreSQL database...")
        
        with psycopg.connect(conn_info) as conn:
            with conn.cursor() as cur:
                # Fetch one random record
                cur.execute("""
                    SELECT jsonl_cont 
                    FROM "43101pagesjsonl" 
                    ORDER BY RANDOM() 
                    LIMIT 1;
                """)
                
                record = cur.fetchone()
                if not record:
                    print("No records found in the table.")
                    return
                
                jsonl_content = record[0]
                
                print("\nJSONL Content Structure:")
                print("-" * 50)
                
                # Pretty print the JSON structure
                print("\nTop-level keys:")
                pprint(list(jsonl_content.keys()))
                
                print("\nFull JSON structure:")
                pprint(jsonl_content)
                
                # Save the JSON to a file for examination
                output_file = "sample_jsonl.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(jsonl_content, f, indent=2, ensure_ascii=False)
                print(f"\nSample JSON saved to {output_file}")
                
    except Exception as e:
        print(f"\nError examining JSONL structure: {e}")
        raise

if __name__ == "__main__":
    examine_single_jsonl() 