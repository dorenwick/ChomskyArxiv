import requests
import os
from datetime import datetime
import logging
from typing import List, Dict
import time
from urllib.parse import urlparse


class ChomskyWebDownloader:
    def __init__(self, save_dir: str):
        """Initialize the downloader with save directory."""
        self.save_dir = save_dir
        self.setup_logger()

        # Create directory if it doesn't exist
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
            self.logger.info(f"Created directory: {save_dir}")

    def setup_logger(self):
        """Setup logging configuration"""
        log_file = os.path.join(self.save_dir, f"download_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

        # Create directory for log file if it doesn't exist
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        self.logger = logging.getLogger("ChomskyWebDownloader")
        self.logger.setLevel(logging.INFO)

        # Clear any existing handlers
        if self.logger.handlers:
            self.logger.handlers.clear()

        # Create handlers
        file_handler = logging.FileHandler(log_file)
        console_handler = logging.StreamHandler()

        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def get_section_name(self, url: str) -> str:
        """Extract section name from URL."""
        path = urlparse(url).path
        section = path.strip('/').split('/')[-1]
        return section if section else "home"

    def download_page(self, url: str) -> Dict:
        """Download HTML content from a URL and save it to a file."""
        try:
            # Add delay to be respectful to the server
            time.sleep(1)

            # Download the HTML content
            self.logger.info(f"Downloading {url}")
            response = requests.get(url)
            response.raise_for_status()

            # Generate filename with timestamp and section name
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            section = self.get_section_name(url)
            filename = f"chomsky_{section}_{timestamp}.html"
            filepath = os.path.join(self.save_dir, filename)

            # Save the HTML content
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(response.text)

            self.logger.info(f"Successfully downloaded {section} to: {filepath}")
            return {
                "url": url,
                "section": section,
                "success": True,
                "filepath": filepath
            }

        except Exception as e:
            self.logger.error(f"Error downloading {url}: {str(e)}")
            return {
                "url": url,
                "section": self.get_section_name(url),
                "success": False,
                "error": str(e)
            }

    def download_all_pages(self, urls: List[str]) -> List[Dict]:
        """Download all specified pages."""
        results = []
        for url in urls:
            result = self.download_page(url)
            results.append(result)
        return results


def main():
    # Set up base directory
    base_dir = r"/web_data"

    # Create directory if it doesn't exist
    os.makedirs(base_dir, exist_ok=True)

    # List of URLs to download
    urls = [
        "https://chomsky.info/articles/",
        "https://chomsky.info/books/",
        "https://chomsky.info/talks/",
        "https://chomsky.info/interviews/",
        "https://chomsky.info/debates/",
        "https://chomsky.info/letters/",
        "https://chomsky.info/about/",
        "https://chomsky.info/bios/",
        "https://chomsky.info/updates/",
        "https://chomsky.info/audionvideo/"
    ]

    # Initialize downloader
    downloader = ChomskyWebDownloader(save_dir=base_dir)

    # Download all pages
    results = downloader.download_all_pages(urls)

    # Print summary
    print("\nDownload Summary:")
    print("=" * 50)
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]

    print(f"Total pages attempted: {len(results)}")
    print(f"Successfully downloaded: {len(successful)}")
    print(f"Failed downloads: {len(failed)}")

    if failed:
        print("\nFailed Downloads:")
        for result in failed:
            print(f"- {result['url']}: {result.get('error')}")


if __name__ == "__main__":
    main()