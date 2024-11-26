import os
import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import urljoin
import re
from datetime import datetime
from pathlib import Path
import random


class ChomskyScraper:
    def __init__(self, base_dir="C:\\Users\\doren\\PycharmProjects\\ChomskyArchive\\web_data"):
        self.base_dir = base_dir
        self.base_url = "https://chomsky.info"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.sections = {
            'interviews': '/interviews/',
            'talks': '/talks/',
            'articles': '/articles/',
            'debates': '/debates/',
            'letters': '/letters/'
        }
        self._setup_directories()

    def _setup_directories(self):
        """Create necessary directories if they don't exist."""
        for section in self.sections.keys():
            dir_path = os.path.join(self.base_dir, section)
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            print(f"Created directory: {dir_path}")

    def _safe_filename(self, url):
        """Generate a safe filename from URL."""
        # Extract the last part of the URL and remove trailing slash
        filename = url.rstrip('/').split('/')[-1]
        # If empty (like in case of root URL), use timestamp
        if not filename:
            filename = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Remove invalid characters and append .html
        filename = re.sub(r'[<>:"/\\|?*]', '', filename)
        if not filename.endswith('.html'):
            filename += '.html'
        return filename

    def _download_page(self, url, save_path):
        """Download a single page with error handling and retries."""
        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                print(f"Downloading: {url}")
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()

                # Save the HTML content
                with open(save_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                print(f"Saved to: {save_path}")

                # Random delay between requests (1-3 seconds)
                time.sleep(random.uniform(1, 3))
                return response.text

            except requests.RequestException as e:
                print(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    print(f"Failed to download {url} after {max_retries} attempts")
                    return None

    def _extract_links(self, html_content, section_url):
        """Extract relevant links from a section page."""
        soup = BeautifulSoup(html_content, 'html.parser')
        links = []

        # Find all links in the main container
        main_container = soup.find('div', id='main_container')
        if main_container:
            for a in main_container.find_all('a', href=True):
                href = a['href']
                if href.startswith(('http://chomsky.info', 'https://chomsky.info', '/')):
                    full_url = urljoin(self.base_url, href)
                    links.append(full_url)
                elif href.startswith('http') and 'chomsky' in href.lower():
                    links.append(href)

        return links

    def scrape_section(self, section):
        """Scrape an entire section of the website."""
        section_url = urljoin(self.base_url, self.sections[section])
        save_dir = os.path.join(self.base_dir, section)

        print(f"\nScraping section: {section}")
        print(f"Section URL: {section_url}")

        # Download the section index page
        index_content = self._download_page(
            section_url,
            os.path.join(save_dir, f"index_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
        )

        if not index_content:
            print(f"Failed to download section index: {section}")
            return

        # Extract and download all article links
        links = self._extract_links(index_content, section_url)
        print(f"Found {len(links)} links in {section}")

        for link in links:
            filename = self._safe_filename(link)
            save_path = os.path.join(save_dir, filename)

            if os.path.exists(save_path):
                print(f"File already exists: {save_path}")
                continue

            self._download_page(link, save_path)

    def scrape_all(self):
        """Scrape all sections of the website."""
        print("Starting full website scrape...")
        for section in self.sections:
            self.scrape_section(section)
        print("Scraping completed!")


def main():
    try:
        scraper = ChomskyScraper()
        scraper.scrape_all()
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        import traceback
        print(traceback.format_exc())


if __name__ == "__main__":
    main()