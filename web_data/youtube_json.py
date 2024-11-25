from bs4 import BeautifulSoup
import json
import os
import re
import logging
from typing import List


def extract_youtube_links(html_path: str) -> List[str]:
    """
    Extract YouTube links from HTML file.

    Args:
        html_path: Path to HTML file

    Returns:
        List of YouTube URLs
    """
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    youtube_links = []

    try:
        # Read the HTML file
        with open(html_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'html.parser')

        # Find all <a> tags
        links = soup.find_all('a')

        # Extract YouTube links
        for link in links:
            href = link.get('href', '')
            # Check if it's a YouTube link
            if 'youtube.com' in href or 'youtu.be' in href:
                youtube_links.append(href)

        logger.info(f"Found {len(youtube_links)} YouTube links")
        return youtube_links

    except Exception as e:
        logger.error(f"Error processing HTML file: {str(e)}")
        return []


def save_to_json(urls: List[str], output_path: str) -> None:
    """
    Save URLs to JSON file.

    Args:
        urls: List of URLs to save
        output_path: Path where to save JSON file
    """
    logger = logging.getLogger(__name__)

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump({"youtube_urls": urls}, f, indent=2)
        logger.info(f"Successfully saved URLs to {output_path}")

    except Exception as e:
        logger.error(f"Error saving JSON file: {str(e)}")


def main():
    # Set up paths
    base_dir = r"C:\Users\doren\PycharmProjects\ChomskyArchive\web_data"
    html_path = os.path.join(base_dir, "chomsky_audio_video_20241124_222224.html")
    json_path = os.path.join(base_dir, "youtube_urls.json")

    # Extract and save links
    youtube_links = extract_youtube_links(html_path)
    save_to_json(youtube_links, json_path)


if __name__ == "__main__":
    main()