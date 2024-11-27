from transformers import BertTokenizer
from pymongo import MongoClient
from typing import List, Dict, Any
from datetime import datetime
import traceback


class ParagraphMerger:
    def __init__(self, mongodb_url: str = "mongodb://localhost:27017/"):
        try:
            # Initialize MongoDB connection
            print("Initializing MongoDB connections...")
            self.client = MongoClient(mongodb_url)
            self.db = self.client.ChomskyArxiv
            self.source_collection = self.db.ChomskyArchive
            self.target_collection = self.db.Paragraphs
            print("MongoDB connections successful")

            # Initialize BERT tokenizer
            print("Loading BERT tokenizer...")
            self.tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
            print("BERT tokenizer loaded successfully")

            # Initialize counter for new paragraph IDs
            self.current_paragraph_id = 0
            print(f"Starting with paragraph_id: {self.current_paragraph_id}")

        except Exception as e:
            print(f"Error during initialization: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            raise

    def get_all_titles(self) -> List[str]:
        """Get all unique titles from the source collection."""
        try:
            print("Fetching all unique titles...")
            titles = self.source_collection.distinct("title")
            print(f"Found {len(titles)} unique titles")
            return titles
        except Exception as e:
            print(f"Error fetching titles: {str(e)}")
            return []

    def get_documents_by_title(self, title: str) -> List[Dict]:
        """Get all documents for a given title, sorted by paragraph_int_id."""
        try:
            print(f"\nFetching documents for title: {title}")
            documents = list(self.source_collection.find(
                {"title": title},
                sort=[("paragraph_int_id", 1)]
            ))
            print(f"Found {len(documents)} documents")
            return documents
        except Exception as e:
            print(f"Error fetching documents for title {title}: {str(e)}")
            return []

    def merge_documents(self, documents: List[Dict]) -> List[Dict]:
        """Merge documents into larger paragraphs based on token count."""
        try:
            print("\nMerging documents...")
            merged_documents = []
            current_text = ""
            current_docs = []

            for doc in documents:
                # Add current document to the batch
                current_docs.append(doc)
                if current_text:
                    current_text += " "
                current_text += doc.get("text", "").strip()

                # Check token count
                tokens = self.tokenizer.encode(current_text)
                print(f"Current token count: {len(tokens)}")

                if len(tokens) > 500:
                    # Remove the last document if we exceeded the limit
                    current_docs.pop()
                    if len(current_docs) > 0:
                        # Create merged document
                        merged_doc = self._create_merged_document(current_docs)
                        merged_documents.append(merged_doc)
                        print(f"Created merged document with {len(self.tokenizer.encode(merged_doc['text']))} tokens")

                        # Start new batch with the last document
                        current_text = doc.get("text", "").strip()
                        current_docs = [doc]

            # Handle remaining documents
            if current_docs:
                merged_doc = self._create_merged_document(current_docs)
                merged_documents.append(merged_doc)
                print(f"Created final merged document with {len(self.tokenizer.encode(merged_doc['text']))} tokens")

            return merged_documents

        except Exception as e:
            print(f"Error merging documents: {str(e)}")
            return []

    def _create_merged_document(self, docs: List[Dict]) -> Dict:
        """Create a new document from merged documents."""
        try:
            merged_text = " ".join(doc.get("text", "").strip() for doc in docs)

            # Create base document
            merged_doc = {
                "paragraph_int_id": self.current_paragraph_id,
                "type": docs[0].get("type", ""),
                "source": docs[0].get("source", ""),
                "url": docs[0].get("url", ""),
                "title": docs[0].get("title", ""),
                "text": merged_text,
                "keyphrases": [],
                "keynames": [],
                "questions": [],
                "answers": [],
                "topic": None,
                "processed_date": datetime.now()
            }

            # Add timestamp if available
            if "timestamp" in docs[0] and "timestamp" in docs[-1]:
                merged_doc["timestamp"] = {
                    "start": docs[0]["timestamp"].get("start", ""),
                    "end": docs[-1]["timestamp"].get("end", "")
                }
                merged_doc[
                    "location"] = f"{docs[0]['timestamp'].get('start', '')} - {docs[-1]['timestamp'].get('end', '')}"

            self.current_paragraph_id += 1
            return merged_doc

        except Exception as e:
            print(f"Error creating merged document: {str(e)}")
            raise

    def process_all_documents(self):
        """Process all documents and create merged paragraphs."""
        try:
            print("\nStarting document processing...")

            # Get all unique titles
            titles = self.get_all_titles()

            total_merged_docs = 0
            for title in titles:
                print(f"\nProcessing title: {title}")

                # Get all documents for this title
                documents = self.get_documents_by_title(title)

                # Merge documents
                merged_documents = self.merge_documents(documents)

                # Insert merged documents
                if merged_documents:
                    print(f"Inserting {len(merged_documents)} merged documents...")
                    self.target_collection.insert_many(merged_documents)
                    total_merged_docs += len(merged_documents)
                    print("Insertion successful")

            print(f"\nProcessing completed. Total merged documents created: {total_merged_docs}")

        except Exception as e:
            print(f"Error processing documents: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")


def main():
    try:
        print("Starting paragraph merging process...")
        merger = ParagraphMerger()
        merger.process_all_documents()
        print("\nParagraph merging completed successfully")

    except Exception as e:
        print(f"Fatal error in main: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")


if __name__ == "__main__":
    main()