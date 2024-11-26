
from pymongo import MongoClient
import json
import os
import logging
from typing import Dict
import traceback


class MongoDBTitleUpdater:
    def __init__(self, mongodb_url: str = "mongodb://localhost:27017/"):
        try:
            # Initialize MongoDB connection
            print("Initializing MongoDB connection...")
            self.client = MongoClient(mongodb_url)
            self.db = self.client.ChomskyArxiv
            self.collection = self.db.ChomskyArchive
            print("MongoDB connection successful")

            # Load titles
            self.titles = self._load_titles()
            print(f"Loaded {len(self.titles)} video titles")

        except Exception as e:
            print(f"Error during initialization: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            raise

    def _load_titles(self) -> Dict[str, str]:
        """Load YouTube titles from JSON file."""
        try:
            titles_path = os.path.join(
                r"C:\Users\doren\PycharmProjects\ChomskyArchive\web_data",
                "youtube_urls_title.json"
            )
            with open(titles_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading titles: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            return {}

    def update_titles(self):
        """Update MongoDB documents with YouTube titles."""
        try:
            # Get all YouTube documents
            youtube_docs = self.collection.find({"type": "youtube"})
            updated_count = 0
            skipped_count = 0

            for doc in youtube_docs:
                try:
                    url = doc.get("url", "")
                    if url in self.titles:
                        # Update document with title
                        result = self.collection.update_one(
                            {"_id": doc["_id"]},
                            {"$set": {"title": self.titles[url]}}
                        )

                        if result.modified_count > 0:
                            updated_count += 1
                            print(f"Updated document {doc['paragraph_int_id']} with title: {self.titles[url]}")
                        else:
                            skipped_count += 1
                            print(f"No update needed for document {doc['paragraph_int_id']}")
                    else:
                        skipped_count += 1
                        print(f"No title found for URL: {url}")

                except Exception as e:
                    print(f"Error updating document {doc.get('paragraph_int_id')}: {str(e)}")
                    print(f"Traceback: {traceback.format_exc()}")
                    continue

            print(f"\nUpdate complete:")
            print(f"Documents updated: {updated_count}")
            print(f"Documents skipped: {skipped_count}")

        except Exception as e:
            print(f"Error updating titles: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")


def main():
    try:
        print("Starting title update process...")
        updater = MongoDBTitleUpdater()
        updater.update_titles()
        print("\nTitle update process completed successfully")

    except Exception as e:
        print(f"Fatal error in main: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")


if __name__ == "__main__":
    main()
