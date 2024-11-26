
from transformers import BertTokenizer
from pymongo import MongoClient
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
from typing import List, Dict, Any
import re
import traceback
from pathlib import Path


class ArticleProcessor:
    def __init__(self, mongodb_url: str = "mongodb://localhost:27017/"):
        try:
            # Initialize MongoDB connection
            print("Initializing MongoDB connection...")
            self.client = MongoClient(mongodb_url)
            self.db = self.client.ChomskyArxiv
            self.collection = self.db.ChomskyArchive
            print("MongoDB connection successful")

            # Initialize BERT tokenizer for long paragraphs
            print("Loading BERT tokenizer...")
            self.tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
            print("BERT tokenizer loaded successfully")

            # Initialize counter for paragraph IDs
            self.current_paragraph_id = self._get_max_paragraph_id()
            print(f"Current maximum paragraph ID: {self.current_paragraph_id}")
        except Exception as e:
            print(f"Error during initialization: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            raise

    def _get_max_paragraph_id(self) -> int:
        """Get the maximum existing paragraph_int_id from MongoDB."""
        try:
            max_doc = self.collection.find_one(sort=[("paragraph_int_id", -1)])
            return max_doc["paragraph_int_id"] if max_doc else 0
        except Exception as e:
            print(f"Error getting max paragraph ID: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            return 0

    def _split_long_paragraph(self, text: str) -> List[str]:
        """Split paragraphs longer than 500 characters using BERT tokenizer."""
        if len(text) <= 500:
            return [text]

        sentences = re.split(r'(?<=[.!?])\s+', text)
        current_segment = ""
        segments = []

        for sentence in sentences:
            potential_text = f"{current_segment} {sentence}".strip()
            tokens = self.tokenizer.encode(potential_text)

            if len(tokens) > 384:
                if current_segment:
                    segments.append(current_segment)
                current_segment = sentence
            else:
                current_segment = potential_text

        if current_segment:
            segments.append(current_segment)

        return segments

    def _extract_title_and_metadata(self, html_content: str) -> Dict[str, str]:
        """Extract title and other metadata from HTML content."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            main_container = soup.find('div', id='detail_main_container')

            if main_container:
                title = main_container.find('h1')
                title_text = title.text if title else "Untitled Article"

                # Try to get article details
                h2 = main_container.find('h2')
                h3 = main_container.find('h3')

                return {
                    "title": title_text,
                    "details": h2.text if h2 else "",
                    "source": h3.text if h3 else ""
                }
            return {"title": "Untitled Article", "details": "", "source": ""}

        except Exception as e:
            print(f"Error extracting title: {str(e)}")
            return {"title": "Untitled Article", "details": "", "source": ""}

    def _extract_paragraphs(self, html_content: str) -> List[str]:
        """Extract paragraphs from HTML content."""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            main_container = soup.find('div', id='detail_main_container')

            if main_container:
                # Get all paragraphs after the metadata
                paragraphs = main_container.find_all('p')
                return [p.text.strip() for p in paragraphs if p.text.strip()]
            return []

        except Exception as e:
            print(f"Error extracting paragraphs: {str(e)}")
            return []

    def _get_clean_url(self, file_path: str) -> str:
        """Extract clean URL from file path."""
        try:
            # Get just the filename without .html
            filename = os.path.splitext(os.path.basename(file_path))[0]
            return f"https://chomsky.info/articles/{filename}"
        except Exception as e:
            print(f"Error creating clean URL: {str(e)}")
            return "https://chomsky.info/articles/"

    def process_article(self, file_path: str) -> None:
        """Process a single article file and store in MongoDB."""
        try:
            print(f"\nProcessing article: {file_path}")

            # Read HTML file
            with open(file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()

            # Extract metadata
            metadata = self._extract_title_and_metadata(html_content)
            print(f"Processing article: {metadata['title']}")

            # Get clean URL
            clean_url = self._get_clean_url(file_path)

            # Extract paragraphs
            paragraphs = self._extract_paragraphs(html_content)
            print(f"Found {len(paragraphs)} paragraphs")

            # Process each paragraph
            for paragraph in paragraphs:
                try:
                    # Split long paragraphs
                    segments = self._split_long_paragraph(paragraph)

                    for segment in segments:
                        self.current_paragraph_id += 1

                        document = {
                            "paragraph_int_id": self.current_paragraph_id,
                            "type": "article",
                            "title": metadata["title"],
                            "source": metadata["source"],
                            "url": clean_url,
                            "text": segment.strip(),
                            "keyphrases": [],
                            "keynames": [],
                            "questions": [],
                            "answers": [],
                            "topic": None,
                            "processed_date": datetime.now()
                        }

                        print(f"Inserting paragraph {self.current_paragraph_id}")
                        print(f"Text length: {len(segment)} characters")

                        self.collection.insert_one(document)
                        print("Successfully inserted document")

                except Exception as e:
                    print(f"Error processing paragraph: {str(e)}")
                    print(f"Traceback: {traceback.format_exc()}")
                    continue

        except Exception as e:
            print(f"Error processing article {file_path}: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")

    def process_directory(self, directory_path: str) -> None:
        """Process all article files in a directory."""
        try:
            print(f"\nProcessing directory: {directory_path}")

            # Walk through all files in the articles directory
            for root, _, files in os.walk(directory_path):
                for file in files:
                    if file.endswith('.html'):
                        file_path = os.path.join(root, file)
                        self.process_article(file_path)

        except Exception as e:
            print(f"Error processing directory {directory_path}: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")


def main():
    try:
        print("Starting article processing...")
        processor = ArticleProcessor()

        # Process articles directory
        articles_dir = r"C:\Users\doren\PycharmProjects\ChomskyArchive\web_data\articles"
        processor.process_directory(articles_dir)

        print("\nArticle processing completed successfully")

    except Exception as e:
        print(f"Fatal error in main: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")


if __name__ == "__main__":
    main()
