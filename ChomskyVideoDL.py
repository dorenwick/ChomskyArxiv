
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
import json
import os
from datetime import datetime
from typing import Dict, List
import time
from tqdm import tqdm
from pathlib import Path


class ChomskyTranscriptFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.youtube = build('youtube', 'v3', developerKey=api_key)

        # Set up directories
        self.base_dir = Path("C:/Users/doren/PycharmProjects/ChomskyArchive/web_data")
        self.transcripts_dir = self.base_dir / "transcripts"
        self.metadata_dir = self.base_dir / "metadata"

        # Create directories
        self.transcripts_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)

    def search_videos(self, query: str, published_after: str, published_before: str, max_results: int = None) -> List[
        Dict]:
        """Search for videos matching query within date range."""
        videos = []
        next_page_token = None
        results_count = 0

        try:
            while True:
                search_response = self.youtube.search().list(
                    q=query,
                    part='id,snippet',
                    maxResults=250,
                    type='video',
                    pageToken=next_page_token,
                    relevanceLanguage='en',
                    publishedAfter=published_after,
                    publishedBefore=published_before
                ).execute()

                for item in search_response['items']:
                    video_data = {
                        'video_id': item['id']['videoId'],
                        'title': item['snippet']['title'],
                        'description': item['snippet']['description'],
                        'channel_title': item['snippet']['channelTitle'],
                        'published_at': item['snippet']['publishedAt'],
                        'url': f"https://www.youtube.com/watch?v={item['id']['videoId']}"
                    }
                    videos.append(video_data)
                    results_count += 1

                    print(f"Found: {video_data['title']} (Published: {video_data['published_at']})")

                next_page_token = search_response.get('nextPageToken')
                if not next_page_token or (max_results and results_count >= max_results):
                    break

                time.sleep(0.1)

        except Exception as e:
            print(f"Error searching videos: {str(e)}")

        return videos

    def collect_video_urls(self, search_queries: List[str], max_results_per_query: int = None) -> Dict:
        """Collect all video URLs and titles from search queries with date ranges"""
        all_videos = []

        # Define date ranges (5-year blocks from 2005 to present)
        date_ranges = [
            ("2005-01-01T00:00:00Z", "2009-12-31T23:59:59Z"),
            ("2010-01-01T00:00:00Z", "2014-12-31T23:59:59Z"),
            ("2015-01-01T00:00:00Z", "2019-12-31T23:59:59Z"),
            ("2020-01-01T00:00:00Z", "2024-12-31T23:59:59Z")
        ]

        # Search for videos in each date range
        for start_date, end_date in date_ranges:
            print(f"\nSearching period: {start_date[:4]} to {end_date[:4]}")

            for query in tqdm(search_queries, desc=f"Processing queries for {start_date[:4]}-{end_date[:4]}"):
                print(f"\nSearching for: {query}")
                videos = self.search_videos(query, start_date, end_date, max_results_per_query)
                all_videos.extend(videos)

                # Save intermediate results for this period and query
                period_results = {
                    'period': f"{start_date[:4]}-{end_date[:4]}",
                    'query': query,
                    'timestamp': datetime.now().strftime("%Y%m%d_%H%M%S"),
                    'total_results': len(videos),
                    'videos': videos
                }

                period_file = self.metadata_dir / f"search_results_{start_date[:4]}_{end_date[:4]}_{query.replace(' ', '_')}.json"
                with open(period_file, 'w', encoding='utf-8') as f:
                    json.dump(period_results, f, indent=4, ensure_ascii=False)

        # Remove duplicates while keeping all info
        unique_videos = {
            v['video_id']: {
                'video_id': v['video_id'],
                'title': v['title'],
                'url': v['url'],
                'channel_title': v['channel_title'],
                'published_at': v['published_at']
            } for v in all_videos
        }

        # Save combined results
        video_list = {
            'timestamp': datetime.now().strftime("%Y%m%d_%H%M%S"),
            'total_videos': len(unique_videos),
            'videos': list(unique_videos.values())
        }

        urls_file = self.metadata_dir / "chomsky_video_urls.json"
        with open(urls_file, 'w', encoding='utf-8') as f:
            json.dump(video_list, f, indent=4, ensure_ascii=False)

        print(f"\nSaved {len(unique_videos)} unique video URLs to {urls_file}")
        return video_list

    def get_video_details(self, video_id: str) -> Dict:
        """Get detailed information about a video."""
        try:
            response = self.youtube.videos().list(
                part='contentDetails,statistics,snippet',
                id=video_id
            ).execute()

            if response['items']:
                details = response['items'][0]
                return {
                    'duration': details['contentDetails']['duration'],
                    'view_count': details['statistics'].get('viewCount', 0),
                    'like_count': details['statistics'].get('likeCount', 0),
                    'title': details['snippet']['title'],
                    'description': details['snippet']['description'],
                    'tags': details['snippet'].get('tags', []),
                    'published_at': details['snippet']['publishedAt']
                }
        except Exception as e:
            print(f"Error getting video details for {video_id}: {str(e)}")
        return {}

    def get_transcript(self, video_id: str) -> Dict:
        """Fetch transcript for a video."""
        transcript_file = self.transcripts_dir / video_id / f"transcript_{datetime.now():%Y%m%d_%H%M%S}.json"
        transcript_file.parent.mkdir(exist_ok=True)

        try:
            transcript_list = YouTubeTranscriptApi.get_transcript(
                video_id,
                languages=['en']  # Prefer English transcripts
            )

            transcript_data = {
                'video_id': video_id,
                'url': f"https://www.youtube.com/watch?v={video_id}",
                'transcript': transcript_list
            }

            # Save transcript
            with open(transcript_file, 'w', encoding='utf-8') as f:
                json.dump(transcript_data, f, indent=4, ensure_ascii=False)

            return transcript_data

        except Exception as e:
            print(f"Error getting transcript for {video_id}: {str(e)}")
            return None
    #
    # def collect_video_urls(self, search_queries: List[str], max_results_per_query: int = None) -> Dict:
    #     """Collect all video URLs and titles from search queries"""
    #     all_videos = []
    #
    #     # Search for videos
    #     for query in tqdm(search_queries, desc="Processing search queries"):
    #         print(f"\nSearching for: {query}")
    #         videos = self.search_videos(query, max_results_per_query)
    #         all_videos.extend(videos)
    #
    #     # Remove duplicates while keeping all info
    #     unique_videos = {
    #         v['video_id']: {
    #             'video_id': v['video_id'],
    #             'title': v['title'],
    #             'url': v['url'],
    #             'channel_title': v['channel_title'],
    #             'published_at': v['published_at']
    #         } for v in all_videos
    #     }
    #
    #     # Save to JSON file
    #     video_list = {
    #         'timestamp': datetime.now().strftime("%Y%m%d_%H%M%S"),
    #         'total_videos': len(unique_videos),
    #         'videos': list(unique_videos.values())
    #     }
    #
    #     urls_file = self.metadata_dir / "chomsky_video_urls.json"
    #     with open(urls_file, 'w', encoding='utf-8') as f:
    #         json.dump(video_list, f, indent=2, ensure_ascii=False)
    #
    #     print(f"\nSaved {len(unique_videos)} unique video URLs to {urls_file}")
    #     return video_list

    def fetch_all_transcripts(self):
        """Fetch transcripts for all videos and track failures"""
        urls_file = self.metadata_dir / "chomsky_video_urls.json"
        failed_transcripts_file = self.metadata_dir / "failed_transcripts.json"

        if not urls_file.exists():
            raise FileNotFoundError("Video URLs file not found. Run collect_video_urls first.")

        with open(urls_file, 'r', encoding='utf-8') as f:
            video_data = json.load(f)

        print(f"\nFetching transcripts for {len(video_data['videos'])} videos...")

        failed_transcripts = []
        success_count = 0

        for video in tqdm(video_data['videos'], desc="Fetching transcripts"):
            video_id = video['video_id']

            # Check if transcript already exists
            if list(self.transcripts_dir.glob(f"{video_id}/transcript_*.json")):
                print(f"Transcript for '{video['title']}' already exists, skipping")
                success_count += 1
                continue

            # Get transcript
            transcript = self.get_transcript(video_id)
            if transcript:
                print(f"Successfully fetched transcript for: {video['title']}")
                success_count += 1
            else:
                print(f"No transcript available for: {video['title']}")
                failed_transcripts.append({
                    'video_id': video_id,
                    'title': video['title'],
                    'url': video['url'],
                    'channel_title': video.get('channel_title', ''),
                    'published_at': video.get('published_at', ''),
                    'failure_timestamp': datetime.now().strftime("%Y%m%d_%H%M%S")
                })

                # Save failed transcripts periodically
                if len(failed_transcripts) % 10 == 0:
                    self._save_failed_transcripts(failed_transcripts, failed_transcripts_file)

            time.sleep(0.5)  # Rate limiting

        # Save final failed transcripts
        if failed_transcripts:
            self._save_failed_transcripts(failed_transcripts, failed_transcripts_file)

        print(f"\nTranscript Fetching Summary:")
        print(f"Total videos processed: {len(video_data['videos'])}")
        print(f"Successfully fetched: {success_count}")
        print(f"Failed to fetch: {len(failed_transcripts)}")
        if failed_transcripts:
            print(f"Failed transcripts saved to: {failed_transcripts_file}")

    def _save_failed_transcripts(self, failed_transcripts: List[Dict], output_file: Path):
        """Save failed transcript attempts to JSON file"""
        failed_data = {
            'timestamp': datetime.now().strftime("%Y%m%d_%H%M%S"),
            'total_failed': len(failed_transcripts),
            'failed_videos': failed_transcripts
        }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(failed_data, f, indent=4, ensure_ascii=False)

    def process_all_videos(self, search_queries: List[str], max_results_per_query: int = None):
        """Two-step process: collect URLs, then fetch transcripts"""
        # Step 1: Collect URLs
        print("Step 1: Collecting video URLs...")
        self.collect_video_urls(search_queries, max_results_per_query)

        # Step 2: Fetch transcripts
        print("\nStep 2: Fetching transcripts...")
        self.fetch_all_transcripts()


def main():
    API_KEY = "AIzaSyCgeuH2GDEg4iHxe7CaIrLQdBZEn8CsJlM"

    fetcher = ChomskyTranscriptFetcher(API_KEY)


    search_queries = [
        "noam chomsky interview",
        "noam chomsky lecture",
        "noam chomsky talk",
        "noam chomsky debate",
        "chomsky linguistics",
        "chomsky politics",
        "chomsky philosophy",
        "noam chomsky",
    ]

    # To only collect URLs:
    fetcher.collect_video_urls(search_queries, max_results_per_query=2000)

    # To only fetch transcripts (if you already have URLs):
    fetcher.fetch_all_transcripts()

    # Or to do both:
    fetcher.process_all_videos(search_queries, max_results_per_query=2000)


if __name__ == "__main__":
    main()