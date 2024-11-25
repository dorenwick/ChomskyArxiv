from scihub import SciHub
import json
import os
from tqdm import tqdm
import time
import logging
from urllib.parse import urlparse
import re
from datetime import datetime

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class SciHubDownloader:
    def __init__(self,
                 input_file: str = r"C:\Users\doren\PycharmProjects\ChomskyArchive\openalex\chomsky_works_metadata.json",
                 output_dir: str = r"C:\Users\doren\PycharmProjects\ChomskyArchive\papers"):
        """Initialize the downloader with input and output paths."""
        self.input_file = input_file
        self.output_dir = output_dir
        self.sh = SciHub()
        self.sh.verify = False  # Disable SSL verification
        self.sh.timeout = 30
        self.sh.retry_count = 5

        # Setup logging
        self.setup_logger()


        # Create output directory structure
        self.setup_directories()

    def setup_logger(self):
        """Setup logging configuration."""
        log_file = os.path.join(self.output_dir, f"download_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        self.logger = logging.getLogger("SciHubDownloader")
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

    def setup_directories(self):
        """Create necessary directories for organizing downloads."""
        os.makedirs(self.output_dir, exist_ok=True)

    def extract_doi(self, url: str) -> str:
        """Extract DOI from URL."""
        if not url:
            return ""

        # Try to find DOI in the URL
        doi_match = re.search(r'10\.\d{4,}\/[-._;()\/:A-Z0-9]+', url, re.IGNORECASE)
        if doi_match:
            return doi_match.group(0)
        return ""

    def sanitize_filename(self, title: str) -> str:
        """Create a safe filename from title."""
        # Remove invalid characters
        safe_title = re.sub(r'[<>:"/\\|?*]', '', title)
        # Remove any multiple spaces
        safe_title = re.sub(r'\s+', ' ', safe_title)
        # Limit length and remove trailing spaces/dots
        safe_title = safe_title[:150].strip('. ')
        return safe_title

    def download_paper(self, doi: str, filepath: str) -> dict:
        for attempt in range(3):  # Try 3 times
            try:
                result = self.sh.fetch(doi)
                if 'pdf' in result:
                    with open(filepath, 'wb') as f:
                        f.write(result['pdf'])
                    return {'success': True}
                time.sleep(5 * (attempt + 1))  # Increasing delay between attempts
            except Exception as e:
                if attempt == 2:  # Last attempt
                    return {'success': False, 'error': str(e)}
                time.sleep(5 * (attempt + 1))

    def download_papers(self):
        """Download papers using SciHub."""
        try:
            # Load the JSON file
            with open(self.input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            works = data.get('works', [])
            self.logger.info(f"Found {len(works)} works to process")

            # Track results
            results = {
                'successful': 0,
                'failed': 0,
                'skipped': 0,
                'errors': []
            }

            # Process each work
            for work in tqdm(works, desc="Downloading papers"):
                try:
                    # Extract information
                    doi = self.extract_doi(work.get('download_url', ''))
                    title = work.get('display_name', '')
                    year = work.get('publication_year', 'unknown')

                    if not doi:
                        self.logger.warning(f"No DOI found for: {title}")
                        results['skipped'] += 1
                        continue

                    # Create year directory
                    year_dir = os.path.join(self.output_dir, str(year))
                    os.makedirs(year_dir, exist_ok=True)

                    # Create filename
                    safe_title = self.sanitize_filename(title)
                    filename = f"{safe_title}.pdf"
                    filepath = os.path.join(year_dir, filename)

                    # Skip if file already exists
                    if os.path.exists(filepath):
                        self.logger.info(f"File already exists: {filepath}")
                        results['skipped'] += 1
                        continue

                    # Download the paper
                    self.logger.info(f"Downloading: {title} (DOI: {doi})")
                    result = self.download_paper(doi, filepath)

                    if result['success']:
                        results['successful'] += 1
                        self.logger.info(f"Successfully downloaded: {title}")
                    else:
                        results['failed'] += 1
                        error_msg = f"Failed to download {title}: {result.get('error', 'Unknown error')}"
                        results['errors'].append(error_msg)
                        self.logger.error(error_msg)

                    # Add delay to avoid overwhelming the server
                    time.sleep(5)

                except Exception as e:
                    results['failed'] += 1
                    error_msg = f"Error processing {title}: {str(e)}"
                    results['errors'].append(error_msg)
                    self.logger.error(error_msg)

            # Save summary
            self.save_summary(results)

        except Exception as e:
            self.logger.error(f"Fatal error: {str(e)}")

    def save_summary(self, results: dict):
        """Save download summary to file."""
        summary_file = os.path.join(self.output_dir, f"download_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")

        try:
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write("Download Summary\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Total successful downloads: {results['successful']}\n")
                f.write(f"Total failed downloads: {results['failed']}\n")
                f.write(f"Total skipped: {results['skipped']}\n\n")

                if results['errors']:
                    f.write("Errors:\n")
                    f.write("-" * 30 + "\n")
                    for error in results['errors']:
                        f.write(f"- {error}\n")

            self.logger.info(f"Summary saved to: {summary_file}")

        except Exception as e:
            self.logger.error(f"Error saving summary: {str(e)}")


def main():
    downloader = SciHubDownloader()
    downloader.download_papers()


if __name__ == "__main__":
    main()