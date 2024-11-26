
from bs4 import BeautifulSoup
import json
import os
import re
import logging
from typing import List, Dict, Tuple
from googleapiclient.discovery import build
from urllib.parse import urlparse, parse_qs
import time


class YouTubeMetadataExtractor:
    def __init__(self, api_key: str):
        """Initialize with YouTube API key."""
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

    def _extract_video_id(self, url: str) -> str:
        """Extract video ID from YouTube URL."""
        try:
            if 'youtu.be' in url:
                return url.split('/')[-1]
            elif 'youtube.com' in url:
                parsed = urlparse(url)
                return parse_qs(parsed.query)['v'][0]
        except Exception as e:
            self.logger.error(f"Could not extract video ID from {url}: {str(e)}")
            return ""
        return ""

    def extract_youtube_links_and_titles(self, html_path: str) -> List[Tuple[str, str]]:
        """Extract YouTube links and their associated titles from the HTML."""
        youtube_links_and_titles = []

        try:
            with open(html_path, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f, 'html.parser')

            # Find all list items that contain YouTube links
            list_items = soup.find_all('li')

            for item in list_items:
                link = item.find('a')
                if link and link.get('href'):
                    href = link.get('href')
                    if 'youtube.com' in href or 'youtu.be' in href:
                        # Get the text content which includes the title
                        title = item.get_text().strip()
                        youtube_links_and_titles.append((href, title))

            self.logger.info(f"Found {len(youtube_links_and_titles)} YouTube links with titles")
            return youtube_links_and_titles

        except Exception as e:
            self.logger.error(f"Error processing HTML file: {str(e)}")
            return []

    def get_video_metadata(self, urls_and_titles: List[Tuple[str, str]]) -> tuple[Dict[str, str], Dict[str, str]]:
        """Fetch description for each video and use HTML titles."""
        titles = {}
        descriptions = {}

        for url, html_title in urls_and_titles:
            video_id = self._extract_video_id(url)
            if not video_id:
                continue

            try:
                # Store the HTML title
                titles[url] = html_title

                # Make API request for description
                request = self.youtube.videos().list(
                    part="snippet",
                    id=video_id
                )
                response = request.execute()

                # Extract description if video exists
                if response['items']:
                    snippet = response['items'][0]['snippet']
                    descriptions[url] = snippet['description']
                    self.logger.info(f"Retrieved metadata for video: {video_id}")
                else:
                    self.logger.warning(f"No metadata found for video: {video_id}")
                    descriptions[url] = ""

                # Respect API quotas with a small delay
                time.sleep(0.1)

            except Exception as e:
                self.logger.error(f"Error fetching metadata for {url}: {str(e)}")
                descriptions[url] = ""
                continue

        return titles, descriptions

    def save_metadata_to_json(self, data: Dict[str, str], output_path: str) -> None:
        """Save metadata to JSON file."""
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Successfully saved metadata to {output_path}")

        except Exception as e:
            self.logger.error(f"Error saving JSON file: {str(e)}")


def main():
    # YouTube API key - replace with your actual API key
    API_KEY = "YOUR_API_KEY_HERE"

    # Set up paths
    base_dir = r"C:\Users\doren\PycharmProjects\ChomskyArchive\web_data"
    html_path = os.path.join(base_dir, "chomsky_audio_video_20241124_222224.html")
    titles_path = os.path.join(base_dir, "youtube_urls_title.json")
    descriptions_path = os.path.join(base_dir, "youtube_urls_description.json")

    # Initialize extractor
    extractor = YouTubeMetadataExtractor(API_KEY)

    # Extract YouTube links and their HTML titles
    youtube_links_and_titles = extractor.extract_youtube_links_and_titles(html_path)

    # Get metadata (using HTML titles and fetching descriptions)
    titles, descriptions = extractor.get_video_metadata(youtube_links_and_titles)

    # Save metadata
    extractor.save_metadata_to_json(titles, titles_path)
    extractor.save_metadata_to_json(descriptions, descriptions_path)


if __name__ == "__main__":
    main()
