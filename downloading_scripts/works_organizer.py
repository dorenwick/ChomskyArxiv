from pymongo import MongoClient
import json
import os
import logging
from datetime import datetime
from typing import Dict, List
from tqdm import tqdm


class OpenAlexWorksProcessor:
    def __init__(self,
                 mongo_url: str = "mongodb://localhost:27017/",
                 database_name: str = "OpenAlex",
                 input_file: str = r"C:\Users\doren\PycharmProjects\ChomskyArchive\openalex\chomsky_all_works.json",
                 output_dir: str = r"C:\Users\doren\PycharmProjects\ChomskyArchive\openalex"):
        """Initialize the processor with MongoDB connection and file paths."""
        self.mongo_url = mongo_url
        self.database_name = database_name
        self.input_file = input_file
        self.output_dir = output_dir
        self.client = None
        self.db = None

        # Setup logging
        self.setup_logger()

        # Ensure output directory exists
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            self.logger.info(f"Created output directory: {output_dir}")

    def setup_logger(self):
        """Setup logging configuration."""
        log_file = os.path.join(self.output_dir, f"openalex_processor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        self.logger = logging.getLogger("OpenAlexProcessor")
        self.logger.setLevel(logging.INFO)

        if self.logger.handlers:
            self.logger.handlers.clear()

        file_handler = logging.FileHandler(log_file)
        console_handler = logging.StreamHandler()

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def connect_to_mongodb(self):
        """Establish connection to MongoDB."""
        try:
            self.client = MongoClient(self.mongo_url)
            self.db = self.client[self.database_name]
            self.logger.info("Successfully connected to MongoDB")
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise

    def get_download_url(self, work: Dict) -> str:
        """Extract download URL from work data."""
        if not work:
            return ""

        # Check primary_location first
        if work.get('primary_location'):
            if work['primary_location'].get('pdf_url'):
                return work['primary_location']['pdf_url']
            elif work['primary_location'].get('landing_page_url'):
                return work['primary_location']['landing_page_url']

        # Check locations
        if work.get('locations'):
            for location in work['locations']:
                if location.get('pdf_url'):
                    return location['pdf_url']
                elif location.get('landing_page_url'):
                    return location['landing_page_url']

        # Check open_access
        if work.get('open_access') and work['open_access'].get('oa_url'):
            return work['open_access']['oa_url']

        return ""

    def get_work_ids_from_author_profile(self) -> List[str]:
        """Extract work IDs from Chomsky's author profile."""
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                author_data = json.load(f)

            # Extract work IDs from works_published (get the values only)
            work_ids = []
            if 'works_published' in author_data:
                # We only want the values (URLs), not the numeric keys
                work_ids = list(author_data['works_published'].values())

                # Log some sample IDs for verification
                if work_ids:
                    self.logger.info(f"Sample work IDs: {work_ids[:5]}")

            self.logger.info(f"Found {len(work_ids)} works in author profile")

            if not work_ids:
                # Print the structure of author_data to help debug
                self.logger.error(f"Author data structure: {list(author_data.keys())}")
                self.logger.error("No works_published found in author profile")

            return work_ids

        except Exception as e:
            self.logger.error(f"Error reading author profile: {str(e)}")
            return []

    def process_works(self) -> List[Dict]:
        """Process works data and create simplified records."""
        simplified_works = []

        try:
            self.connect_to_mongodb()
            works_collection = self.db["Works"]

            # Get work IDs from author profile
            work_ids = self.get_work_ids_from_author_profile()
            if not work_ids:
                self.logger.error("No work IDs found in author profile")
                return []

            self.logger.info(f"Processing {len(work_ids)} works...")

            # Process each work
            for work_id in tqdm(work_ids, desc="Processing works"):
                try:
                    # Log the work_id we're looking for
                    self.logger.debug(f"Looking for work: {work_id}")

                    # Find the work in MongoDB
                    work = works_collection.find_one({"id": work_id})

                    if work:
                        simplified_work = {
                            "id": work.get('id', ''),
                            "display_name": work.get('display_name', ''),
                            "download_url": self.get_download_url(work),
                            "publication_year": work.get('publication_year', ''),
                            "type": work.get('type', ''),
                            "cited_by_count": work.get('cited_by_count', 0)
                        }
                        simplified_works.append(simplified_work)
                    else:
                        self.logger.warning(f"Work not found in MongoDB: {work_id}")

                except Exception as e:
                    self.logger.error(f"Error processing work {work_id}: {str(e)}")

            self.logger.info(f"Successfully processed {len(simplified_works)} works")
            return simplified_works

        except Exception as e:
            self.logger.error(f"Error processing works: {str(e)}")
            return []
        finally:
            if self.client:
                self.client.close()

    def save_results(self, simplified_works: List[Dict]):
        """Save simplified works data to JSON file."""
        output_file = os.path.join(self.output_dir,
                                   f"chomsky_works_simplified_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")

        try:
            # Sort works by publication year (newest first)
            simplified_works.sort(key=lambda x: x.get('publication_year', 0), reverse=True)

            output_data = {
                "metadata": {
                    "total_works": len(simplified_works),
                    "works_with_downloads": len([w for w in simplified_works if w['download_url']]),
                    "generated_at": datetime.now().isoformat(),
                    "year_range": {
                        "earliest": min([w['publication_year'] for w in simplified_works if w.get('publication_year')],
                                        default=None),
                        "latest": max([w['publication_year'] for w in simplified_works if w.get('publication_year')],
                                      default=None)
                    }
                },
                "works": simplified_works
            }

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)

            self.logger.info(f"Successfully saved results to {output_file}")

        except Exception as e:
            self.logger.error(f"Error saving results: {str(e)}")


    def verify_json_structure(self):
        """Verify the structure of the input JSON file."""
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Print the top-level keys
            self.logger.info("JSON file structure:")
            self.logger.info(f"Top-level keys: {list(data.keys())}")

            # Check for works_published
            if 'works_published' in data:
                sample_works = list(data['works_published'].items())[:5]
                self.logger.info(f"Sample works_published entries: {sample_works}")
                return True
            else:
                self.logger.error("works_published not found in JSON file")
                return False

        except Exception as e:
            self.logger.error(f"Error verifying JSON structure: {str(e)}")
            return False


def main():
    # Create processor instance
    processor = OpenAlexWorksProcessor()

    # Verify JSON structure first
    if processor.verify_json_structure():
        # Process works and save results
        simplified_works = processor.process_works()
        processor.save_results(simplified_works)
    else:
        print("Failed to verify JSON structure. Please check the log file for details.")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()