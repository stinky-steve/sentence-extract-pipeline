import os
import psycopg
from dotenv import load_dotenv
from tabulate import tabulate

def inspect_database_schema():
    """Inspect and display database schema information."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Construct connection string
        conn_info = f"host={os.getenv('PG_HOST')} port={os.getenv('PG_PORT')} dbname={os.getenv('PG_DB')} user={os.getenv('PG_USER')} password={os.getenv('PG_PASSWORD')}"
        
        print("Connecting to PostgreSQL database...")
        print(f"Database: {os.getenv('PG_DB')}")
        
        with psycopg.connect(conn_info) as conn:
            with conn.cursor() as cur:
                # Get all tables in the database
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name;
                """)
                tables = cur.fetchall()
                
                print("\nTables in database:")
                print("-" * 50)
                
                for table in tables:
                    table_name = table[0]
                    print(f"\nTable: {table_name}")
                    
                    # Get column information for each table
                    cur.execute("""
                        SELECT column_name, data_type, character_maximum_length
                        FROM information_schema.columns
                        WHERE table_name = %s
                        ORDER BY ordinal_position;
                    """, (table_name,))
                    
                    columns = cur.fetchall()
                    
                    # Prepare data for tabulate
                    table_data = []
                    for col in columns:
                        col_name, data_type, max_length = col
                        length_info = f" (max: {max_length})" if max_length else ""
                        table_data.append([col_name, data_type + length_info])
                    
                    # Print column information in a formatted table
                    print(tabulate(table_data, headers=['Column Name', 'Data Type'], tablefmt='grid'))
                    
                    # Get row count for the table (with proper quoting)
                    cur.execute(f'SELECT COUNT(*) FROM "{table_name}";')
                    row_count = cur.fetchone()[0]
                    print(f"\nTotal rows: {row_count:,}")
                    print("-" * 50)
                
    except Exception as e:
        print(f"\nError inspecting database schema: {e}")
        raise

if __name__ == "__main__":
    inspect_database_schema() 