import json
import time
from pathlib import Path
import yt_dlp
from tqdm import tqdm


class VideoDownloader:
    def __init__(self):
        self.base_dir = Path("C:/Users/doren/PycharmProjects/ChomskyArchive/web_data")
        self.videos_dir = self.base_dir / "youtube_videos"  # Updated directory
        self.metadata_dir = self.base_dir / "metadata"

        # Create directories
        self.videos_dir.mkdir(parents=True, exist_ok=True)

        # Configure yt-dlp options
        self.ydl_opts = {
            'format': 'best',
            'paths': {'home': str(self.videos_dir)},
            'outtmpl': {'default': '%(title)s.%(ext)s'},
            'quiet': False,
            'no_warnings': False,
            'progress': True,
            'prefer_ffmpeg': False,
            'postprocessors': []
        }

    def load_video_urls(self) -> list:
        """Load video URLs from the collected data"""
        urls_file = self.metadata_dir / "chomsky_video_urls.json"

        if not urls_file.exists():
            raise FileNotFoundError(
                "Video URLs file not found. Please run the URL collection script first."
            )

        try:
            with open(urls_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Extract URLs and titles from the video data
                videos = [(video['url'], video['title']) for video in data['videos']]
                print(f"Loaded {len(videos)} video URLs")
                return videos
        except Exception as e:
            print(f"Error loading URLs: {str(e)}")
            return []

    def download_video(self, url: str, title: str) -> bool:
        try:
            print(f"\nDownloading: {title}")
            print(f"URL: {url}")

            with yt_dlp.YoutubeDL(self.ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                print(f"Successfully downloaded: {info.get('title', 'Unknown title')}")
                return True

        except Exception as e:
            print(f"Error downloading {title}: {str(e)}")
            return False
        finally:
            time.sleep(1)  # Rate limiting


def main():
    downloader = VideoDownloader()

    try:
        # Load URLs from collected data
        video_data = downloader.load_video_urls()

        if not video_data:
            print("No URLs found to download")
            return

        print("Starting downloads...")
        failed_downloads = []

        for url, title in tqdm(video_data, desc="Downloading videos"):
            success = downloader.download_video(url, title)
            if not success:
                failed_downloads.append((url, title))

        # Save failed downloads to file
        if failed_downloads:
            failed_file = downloader.base_dir / "failed_downloads.json"
            with open(failed_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'failed_downloads': [
                        {'url': url, 'title': title}
                        for url, title in failed_downloads
                    ]
                }, f, indent=2)

            print(f"\nFailed to download {len(failed_downloads)} videos.")
            print(f"Failed downloads saved to: {failed_file}")
        else:
            print("\nAll downloads completed successfully!")

    except Exception as e:
        print(f"Error in main process: {str(e)}")


if __name__ == "__main__":
    main()