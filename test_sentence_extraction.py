import os
import json
import psycopg
import spacy
from dotenv import load_dotenv
import markdown
from bs4 import BeautifulSoup
import re
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sentence_extraction.log'),
        logging.StreamHandler()
    ]
)

def clean_text(text):
    """Clean extracted text with balanced cleaning rules."""
    # Remove multiple spaces
    text = ' '.join(text.split())
    
    # Remove multiple newlines
    text = '\n'.join(line.strip() for line in text.splitlines() if line.strip())
    
    # Remove URLs
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
    
    # Remove email addresses
    text = re.sub(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', '', text)
    
    # Remove special characters but keep more punctuation
    text = re.sub(r'[^\w\s.,!?;:()/-]', ' ', text)
    
    # Remove multiple punctuation marks
    text = re.sub(r'([.,!?;:])\1+', r'\1', text)
    
    # Remove spaces around punctuation
    text = re.sub(r'\s+([.,!?;:])', r'\1', text)
    
    # Remove multiple spaces again after all other cleaning
    text = ' '.join(text.split())
    
    return text.strip()

def markdown_to_text(markdown_string):
    """Convert markdown to plain text, excluding table content."""
    # Convert markdown to HTML
    html = markdown.markdown(markdown_string)
    # Parse HTML and extract text
    soup = BeautifulSoup(html, features='html.parser')
    
    # Remove all table elements
    for table in soup.find_all('table'):
        table.decompose()
    
    # Remove other unwanted elements
    for element in soup.find_all(['script', 'style']):
        element.decompose()
    
    # Get text content
    text = soup.get_text()
    return text

def get_processed_ids():
    """Get set of already processed document IDs from checkpoint file."""
    checkpoint_file = Path('processed_ids.json')
    if checkpoint_file.exists():
        with open(checkpoint_file, 'r') as f:
            return set(json.load(f))
    return set()

def save_processed_id(doc_id):
    """Save processed document ID to checkpoint file."""
    checkpoint_file = Path('processed_ids.json')
    processed_ids = get_processed_ids()
    processed_ids.add(doc_id)
    with open(checkpoint_file, 'w') as f:
        json.dump(list(processed_ids), f)

def verify_saved_sentences(sentences, output_file, start_position):
    """Verify that sentences were correctly saved to file."""
    try:
        if not os.path.exists(output_file):
            return False
        
        with open(output_file, 'r', encoding='utf-8') as f:
            f.seek(start_position)
            saved_content = f.read().strip()
            expected_content = '\n'.join(sentences)
            return saved_content.endswith(expected_content)
    except Exception as e:
        logging.error(f"Error verifying saved sentences: {e}")
        return False

def save_sentences(sentences, output_file):
    """Save sentences to file with verification."""
    if not sentences:
        return True
        
    max_retries = 3
    try:
        # Get current file size for verification
        start_position = os.path.getsize(output_file) if os.path.exists(output_file) else 0
    except Exception:
        start_position = 0
    
    for attempt in range(max_retries):
        try:
            # Append sentences to file
            with open(output_file, 'a', encoding='utf-8') as f:
                f.write('\n'.join(sentences) + '\n')
            
            if verify_saved_sentences(sentences, output_file, start_position):
                return True
            else:
                logging.warning(f"Save verification failed on attempt {attempt + 1}")
        except Exception as e:
            logging.error(f"Error saving sentences (attempt {attempt + 1}): {e}")
    
    return False

def process_documents():
    """Process documents from the database with error handling and checkpointing."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Initialize spaCy
        logging.info("Loading spaCy model...")
        nlp = spacy.load('en_core_web_sm')
        
        # Get already processed document IDs
        processed_ids = get_processed_ids()
        
        # Construct connection string
        conn_info = f"host={os.getenv('PG_HOST')} port={os.getenv('PG_PORT')} dbname={os.getenv('PG_DB')} user={os.getenv('PG_USER')} password={os.getenv('PG_PASSWORD')}"
        
        logging.info("Connecting to PostgreSQL database...")
        
        # Create output file if it doesn't exist
        output_file = "extracted_sentences.txt"
        if not os.path.exists(output_file):
            open(output_file, 'w', encoding='utf-8').close()
        
        with psycopg.connect(conn_info) as conn:
            with conn.cursor() as cur:
                # Get total number of documents
                cur.execute('SELECT COUNT(*) FROM "43101pagesjsonl"')
                total_docs = cur.fetchone()[0]
                logging.info(f"Total documents to process: {total_docs}")
                
                # For testing, only process 3 documents
                test_limit = 3
                logging.info(f"TEST MODE: Processing only {test_limit} documents")
                
                # Process test documents
                cur.execute("""
                    SELECT ctid::text, jsonl_cont 
                    FROM "43101pagesjsonl" 
                    ORDER BY ctid
                    LIMIT %s
                """, (test_limit,))
                
                for record in cur:
                    doc_id, jsonl_content = record
                    
                    # Skip if already processed
                    if doc_id in processed_ids:
                        logging.info(f"Skipping already processed document {doc_id}")
                        continue
                    
                    try:
                        # Extract markdown content
                        markdown_content = jsonl_content['response']['body']['pages'][0]['markdown']
                        
                        # Convert markdown to plain text
                        plain_text = markdown_to_text(markdown_content)
                        plain_text = clean_text(plain_text)
                        
                        # Process with spaCy for sentence segmentation
                        doc = nlp(plain_text)
                        
                        # Extract and filter sentences
                        sentences = []
                        for sent in doc.sents:
                            text = sent.text.strip()
                            if 10 <= len(text) <= 500:
                                sentences.append(text)
                        
                        # Save sentences if we found any
                        if sentences:
                            if save_sentences(sentences, output_file):
                                logging.info(f"Successfully processed and saved document {doc_id} with {len(sentences)} sentences")
                                save_processed_id(doc_id)
                            else:
                                logging.error(f"Failed to save sentences for document {doc_id}")
                        else:
                            logging.warning(f"No valid sentences found in document {doc_id}")
                            save_processed_id(doc_id)
                        
                    except KeyError as e:
                        logging.error(f"Error accessing JSON structure in document {doc_id}: {e}")
                        continue
                    except Exception as e:
                        logging.error(f"Error processing document {doc_id}: {e}")
                        continue
                
                logging.info("Test processing completed")
                
    except Exception as e:
        logging.error(f"Fatal error in main processing loop: {e}")
        raise

if __name__ == "__main__":
    process_documents() 