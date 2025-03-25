import os
import json
import psycopg
import spacy
from dotenv import load_dotenv

load_dotenv()

def fetch_jsonl_strings():
    """Fetch JSONL strings from PostgreSQL database."""
    conn_info = f"host={os.getenv('PG_HOST')} port={os.getenv('PG_PORT')} dbname={os.getenv('PG_DB')} user={os.getenv('PG_USER')} password={os.getenv('PG_PASSWORD')}"
    try:
        with psycopg.connect(conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT jsonl_text FROM your_table;")
                return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        raise

def extract_sentences(jsonl_strings):
    """Extract and filter sentences from JSONL strings using spaCy."""
    nlp = spacy.load('en_core_web_sm')
    sentences = []
    
    for jsonl in jsonl_strings:
        try:
            data = json.loads(jsonl)
            doc = nlp(data['text'])
            # Filter sentences by length
            valid_sentences = [
                sent.text.strip() 
                for sent in doc.sents 
                if 10 < len(sent.text.strip()) < 500
            ]
            sentences.extend(valid_sentences)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSONL: {e}")
            continue
        except Exception as e:
            print(f"Error processing text: {e}")
            continue
    
    return sentences

def save_sentences(sentences, filepath='sentences.txt'):
    """Save extracted sentences to a text file."""
    try:
        with open(filepath, 'w', encoding='utf-8') as file:
            file.write('\n'.join(sentences))
        print(f"Successfully saved {len(sentences)} sentences to {filepath}")
    except Exception as e:
        print(f"Error saving sentences to file: {e}")
        raise

if __name__ == "__main__":
    jsonl_data = fetch_jsonl_strings()
    sentences = extract_sentences(jsonl_data)
    save_sentences(sentences) 