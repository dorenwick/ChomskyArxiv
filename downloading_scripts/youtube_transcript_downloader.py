from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import TextFormatter
from urllib.parse import urlparse, parse_qs
import json
import os
from typing import List, Dict
from datetime import datetime
import logging
from tqdm import tqdm


class YouTubeTranscriptDownloader:
    def __init__(self, output_dir: str = "transcripts", json_path: str = None):
        """
        Initialize the downloader with an output directory and JSON file path.

        Args:
            output_dir: Directory to save transcripts
            json_path: Path to JSON file containing YouTube URLs
        """
        self.output_dir = output_dir
        self.json_path = json_path

        # Set up logging
        self.setup_logger()

        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            self.logger.info(f"Created output directory: {output_dir}")

    def setup_logger(self):
        """Setup logging configuration"""
        self.logger = logging.getLogger("TranscriptDownloader")
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            # Create handlers
            console_handler = logging.StreamHandler()
            file_handler = logging.FileHandler(
                os.path.join(self.output_dir, f"download_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
            )

            # Create formatters and add it to handlers
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            file_handler.setFormatter(formatter)

            # Add handlers to the logger
            self.logger.addHandler(console_handler)
            self.logger.addHandler(file_handler)

    def load_urls(self) -> List[str]:
        """Load YouTube URLs from JSON file"""
        try:
            with open(self.json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                urls = data.get('youtube_urls', [])

                # Filter out non-video URLs (like search results)
                filtered_urls = [url for url in urls if 'watch?' in url or 'youtu.be' in url]

                self.logger.info(f"Loaded {len(filtered_urls)} video URLs from JSON file")
                return filtered_urls
        except Exception as e:
            self.logger.error(f"Error loading URLs from JSON: {str(e)}")
            return []

    def extract_video_id(self, url: str) -> str:
        """Extract video ID from various forms of YouTube URLs."""
        try:
            parsed_url = urlparse(url)

            if parsed_url.hostname in ('youtu.be', 'www.youtu.be'):
                return parsed_url.path[1:]

            if parsed_url.hostname in ('youtube.com', 'www.youtube.com'):
                if parsed_url.path == '/watch':
                    return parse_qs(parsed_url.query)['v'][0]
                elif parsed_url.path.startswith('/embed/'):
                    return parsed_url.path.split('/')[2]
                elif parsed_url.path.startswith('/v/'):
                    return parsed_url.path.split('/')[2]

            raise ValueError(f"Could not extract video ID from URL: {url}")
        except Exception as e:
            raise ValueError(f"Error extracting video ID: {str(e)}")

    def download_transcript(self, url: str, languages: List[str] = ['en']) -> Dict:
        """Download transcript for a single video URL."""
        try:
            video_id = self.extract_video_id(url)
            transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=languages)

            # Format as plain text
            formatter = TextFormatter()
            text_transcript = formatter.format_transcript(transcript)

            # Save both JSON and text versions
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Create subdirectory for this video
            video_dir = os.path.join(self.output_dir, video_id)
            if not os.path.exists(video_dir):
                os.makedirs(video_dir)

            # Save JSON version (with timestamps)
            json_path = os.path.join(video_dir, f"transcript_{timestamp}.json")
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump({
                    "video_id": video_id,
                    "url": url,
                    "transcript": transcript
                }, f, ensure_ascii=False, indent=2)

            # Save text version
            text_path = os.path.join(video_dir, f"transcript_{timestamp}.txt")
            with open(text_path, 'w', encoding='utf-8') as f:
                f.write(f"Video URL: {url}\n\n")
                f.write(text_transcript)

            return {
                "video_id": video_id,
                "url": url,
                "success": True,
                "json_path": json_path,
                "text_path": text_path
            }

        except Exception as e:
            self.logger.error(f"Error downloading transcript for {url}: {str(e)}")
            return {
                "video_id": video_id if 'video_id' in locals() else None,
                "url": url,
                "success": False,
                "error": str(e)
            }

    def download_all_transcripts(self, languages: List[str] = ['en']) -> List[Dict]:
        """Download transcripts for all videos in the JSON file."""
        urls = self.load_urls()
        results = []

        self.logger.info(f"Starting download of {len(urls)} transcripts...")

        for url in tqdm(urls, desc="Downloading transcripts"):
            result = self.download_transcript(url, languages)
            results.append(result)

            # Log result
            if result['success']:
                self.logger.info(f"Successfully downloaded transcript for {result['video_id']}")
            else:
                self.logger.warning(f"Failed to download transcript for {url}: {result.get('error')}")

        # Save summary report
        self.save_summary_report(results)

        return results

    def save_summary_report(self, results: List[Dict]):
        """Save a summary report of the download results."""
        successful = [r for r in results if r['success']]
        failed = [r for r in results if not r['success']]

        report_path = os.path.join(self.output_dir, f"summary_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("YouTube Transcript Download Summary\n")
            f.write("=" * 40 + "\n\n")
            f.write(f"Total videos processed: {len(results)}\n")
            f.write(f"Successfully downloaded: {len(successful)}\n")
            f.write(f"Failed downloads: {len(failed)}\n\n")

            if failed:
                f.write("Failed Downloads:\n")
                f.write("-" * 20 + "\n")
                for result in failed:
                    f.write(f"URL: {result['url']}\n")
                    f.write(f"Error: {result.get('error')}\n\n")


def main():
    # Set up paths
    base_dir = r"/web_data"
    json_path = os.path.join(base_dir, "youtube_urls.json")
    output_dir = os.path.join(base_dir, "transcripts")

    # Create directories if they don't exist
    for directory in [base_dir, output_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Created directory: {directory}")

    # Verify JSON file exists
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"JSON file not found at: {json_path}")

    # Initialize downloader
    downloader = YouTubeTranscriptDownloader(
        output_dir=output_dir,
        json_path=json_path
    )

    # Download all transcripts
    results = downloader.download_all_transcripts(languages=['en'])


if __name__ == "__main__":
    main()