
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

from web_data.transform_article_data_to_mongodb import ArticleProcessor


class TalkProcessor(ArticleProcessor):  # Inherit from ArticleProcessor to reuse common functionality
    def _get_clean_url(self, file_path: str) -> str:
        """Extract clean URL from file path."""
        try:
            # Get just the filename without .html
            filename = os.path.splitext(os.path.basename(file_path))[0]
            return f"https://chomsky.info/talks/{filename}"
        except Exception as e:
            print(f"Error creating clean URL: {str(e)}")
            return "https://chomsky.info/talks/"

    def process_talk(self, file_path: str) -> None:
        """Process a single talk file and store in MongoDB."""
        try:
            print(f"\nProcessing talk: {file_path}")

            # Read HTML file
            with open(file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()

            # Extract metadata
            metadata = self._extract_title_and_metadata(html_content)
            print(f"Processing talk: {metadata['title']}")

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
                            "type": "talk",  # Changed type to talk
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
            print(f"Error processing talk {file_path}: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")

    def process_directory(self, directory_path: str) -> None:
        """Process all talk files in a directory."""
        try:
            print(f"\nProcessing directory: {directory_path}")

            # Walk through all files in the talks directory
            for root, _, files in os.walk(directory_path):
                for file in files:
                    if file.endswith('.html'):
                        file_path = os.path.join(root, file)
                        self.process_talk(file_path)  # Changed to process_talk

        except Exception as e:
            print(f"Error processing directory {directory_path}: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")


def main():
    try:
        print("Starting talk processing...")
        processor = TalkProcessor()

        # Process talks directory
        talks_dir = r"C:\Users\doren\PycharmProjects\ChomskyArchive\web_data\talks"
        processor.process_directory(talks_dir)

        print("\nTalk processing completed successfully")

    except Exception as e:
        print(f"Fatal error in main: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")


if __name__ == "__main__":
    main()
