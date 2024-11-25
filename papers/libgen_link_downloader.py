import requests
import json
import os
from datetime import datetime
import logging
import time
from typing import Dict, Optional, List, Tuple
from pathlib import Path
from tqdm import tqdm
from bs4 import BeautifulSoup
from urllib.parse import urljoin


class LibGenDownloadManager:
    def __init__(self,
                 matches_file: str,
                 output_dir: str = "downloads",
                 max_retries: int = 2,
                 delay_between_retries: float = 1.0):
        """
        Initialize the LibGen download manager.

        Args:
            matches_file: Path to the JSON file containing LibGen matches
            output_dir: Directory to save downloaded files
            max_retries: Maximum number of retry attempts per mirror
            delay_between_retries: Delay in seconds between retry attempts
        """
        self.matches_file = matches_file
        self.output_dir = Path(output_dir)
        self.max_retries = max_retries
        self.delay_between_retries = delay_between_retries
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.setup_directories()
        self.setup_logging()

    def setup_directories(self):
        """Create necessary directories."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir = self.output_dir / "logs"
        self.log_dir.mkdir(exist_ok=True)

    def setup_logging(self):
        """Configure logging."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = self.log_dir / f"download_log_{timestamp}.log"

        self.logger = logging.getLogger("LibGenDownloader")
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

    def extract_download_links(self, mirror_url: str) -> Tuple[Optional[str], List[str]]:
        """
        Extract direct download links from the library.lol page.

        Returns:
            Tuple containing the main GET link and list of IPFS links
        """
        try:
            response = self.session.get(mirror_url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find the main GET link
            get_link = soup.find('h2').find('a')['href'] if soup.find('h2') and soup.find('h2').find('a') else None

            # Find IPFS links
            ipfs_links = []
            download_div = soup.find('div', id='download')
            if download_div:
                ipfs_links = [a['href'] for a in download_div.find_all('a')
                              if any(gateway in a['href'] for gateway in
                                     ['cloudflare-ipfs.com', 'gateway.ipfs.io', 'gateway.pinata.cloud'])]

            return get_link, ipfs_links

        except Exception as e:
            self.logger.error(f"Error extracting download links: {str(e)}")
            return None, []

    def load_matches(self) -> List[Dict]:
        """Load matches from JSON file."""
        try:
            with open(self.matches_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('matches', [])
        except Exception as e:
            self.logger.error(f"Error loading matches file: {str(e)}")
            return []

    def sanitize_filename(self, filename: str) -> str:
        """Sanitize filename by removing invalid characters."""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename[:240]  # Limit filename length to avoid issues

    def download_file(self, url: str, filepath: Path, headers: Dict = None) -> bool:
        """
        Download a file from the given URL.

        Args:
            url: Download URL
            filepath: Path to save the file
            headers: Optional request headers
        """
        try:
            response = self.session.get(url, headers=headers, stream=True, timeout=30)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))

            with open(filepath, 'wb') as f, tqdm(
                    desc=filepath.name,
                    total=total_size,
                    unit='iB',
                    unit_scale=True,
                    unit_divisor=1024,
            ) as pbar:
                for data in response.iter_content(chunk_size=1024):
                    size = f.write(data)
                    pbar.update(size)
            return True

        except Exception as e:
            self.logger.error(f"Download failed: {str(e)}")
            if filepath.exists():
                filepath.unlink()
            return False

    def try_download_from_mirror(self, mirror_url: str, filepath: Path) -> bool:
        """
        Attempt to download from a single mirror with retries.
        """
        self.logger.info(f"Extracting download links from: {mirror_url}")
        direct_link, ipfs_links = self.extract_download_links(mirror_url)

        # Try the main GET link first
        if direct_link:
            self.logger.info(f"Attempting download from direct link: {direct_link}")
            for attempt in range(self.max_retries):
                if self.download_file(direct_link, filepath):
                    return True
                if attempt < self.max_retries - 1:
                    self.logger.warning(f"Retry {attempt + 1} for direct link")
                    time.sleep(self.delay_between_retries)

        # Try IPFS links if direct link fails
        for ipfs_link in ipfs_links:
            self.logger.info(f"Attempting download from IPFS: {ipfs_link}")
            for attempt in range(self.max_retries):
                if self.download_file(ipfs_link, filepath):
                    return True
                if attempt < self.max_retries - 1:
                    self.logger.warning(f"Retry {attempt + 1} for IPFS link")
                    time.sleep(self.delay_between_retries)

        return False

    def try_download_from_mirrors(self, item: Dict) -> Optional[Path]:
        """
        Attempt to download from each mirror with retries.
        """
        filename = self.sanitize_filename(f"{item['Author']} - {item['Title']}.{item['Extension']}")
        filepath = self.output_dir / filename

        mirrors = [
            item.get('Mirror_1'),
            item.get('Mirror_2'),
            item.get('Mirror_3')
        ]

        for mirror_num, mirror_url in enumerate(mirrors, 1):
            if not mirror_url:
                continue

            self.logger.info(f"Trying mirror {mirror_num} for: {filename}")

            if self.try_download_from_mirror(mirror_url, filepath):
                self.logger.info(f"Successfully downloaded: {filename}")
                return filepath

            self.logger.warning(f"Failed all attempts for mirror {mirror_num}")
            time.sleep(self.delay_between_retries)

        self.logger.error(f"Failed to download from all mirrors: {filename}")
        return None

    def process_downloads(self):
        """Process all matches and attempt downloads."""
        matches = self.load_matches()
        self.logger.info(f"Starting download of {len(matches)} files")

        results = {
            'successful': [],
            'failed': []
        }

        for item in tqdm(matches, desc="Processing downloads"):
            filepath = self.try_download_from_mirrors(item)
            if filepath:
                results['successful'].append({
                    'title': item['Title'],
                    'author': item['Author'],
                    'filepath': str(filepath)
                })
            else:
                results['failed'].append({
                    'title': item['Title'],
                    'author': item['Author']
                })

            # Add a delay between files to be respectful
            time.sleep(2)

        # Save results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_file = self.output_dir / f"download_results_{timestamp}.json"

        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump({
                'metadata': {
                    'total_files': len(matches),
                    'successful': len(results['successful']),
                    'failed': len(results['failed']),
                    'timestamp': datetime.now().isoformat()
                },
                'results': results
            }, f, indent=2)

        self.logger.info(f"\nDownload Summary:")
        self.logger.info(f"Total files processed: {len(matches)}")
        self.logger.info(f"Successfully downloaded: {len(results['successful'])}")
        self.logger.info(f"Failed downloads: {len(results['failed'])}")
        self.logger.info(f"Results saved to: {results_file}")

        return results


def main():
    # Example usage
    matches_file = r"C:\Users\doren\PycharmProjects\ChomskyArchive\papers\libgen_matches_20241125_093438.json"
    download_dir = r"C:\Users\doren\PycharmProjects\ChomskyArchive\downloads"

    downloader = LibGenDownloadManager(
        matches_file=matches_file,
        output_dir=download_dir,
        max_retries=2,
        delay_between_retries=1.0
    )

    results = downloader.process_downloads()


if __name__ == "__main__":
    main()