
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

    def search_videos(self, query: str, max_results: int = None) -> List[Dict]:
        """Search for videos matching query."""
        videos = []
        next_page_token = None
        results_count = 0

        try:
            while True:
                search_response = self.youtube.search().list(
                    q=query,
                    part='id,snippet',
                    maxResults=50,
                    type='video',
                    pageToken=next_page_token,
                    relevanceLanguage='en'  # Prefer English content
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

                    print(f"Found: {video_data['title']}")

                next_page_token = search_response.get('nextPageToken')
                if not next_page_token or (max_results and results_count >= max_results):
                    break

                time.sleep(0.1)  # Respect API quota

        except Exception as e:
            print(f"Error searching videos: {str(e)}")

        # Save search results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = self.metadata_dir / f"search_results_{timestamp}.json"
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump({
                'query': query,
                'timestamp': timestamp,
                'total_results': len(videos),
                'videos': videos
            }, f, indent=2, ensure_ascii=False)

        return videos

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
                json.dump(transcript_data, f, indent=2, ensure_ascii=False)

            return transcript_data

        except Exception as e:
            print(f"Error getting transcript for {video_id}: {str(e)}")
            return None

    def process_all_videos(self, search_queries: List[str], max_results_per_query: int = None):
        """Search and fetch transcripts for all matching videos."""
        all_videos = []

        # Search for videos
        for query in search_queries:
            print(f"\nSearching for: {query}")
            videos = self.search_videos(query, max_results_per_query)
            all_videos.extend(videos)

        # Remove duplicates
        unique_videos = {v['video_id']: v for v in all_videos}.values()
        print(f"\nFound {len(unique_videos)} unique videos")

        # Process each video
        for video in tqdm(unique_videos, desc="Fetching transcripts"):
            video_id = video['video_id']

            # Check if transcript already exists
            if list(self.transcripts_dir.glob(f"{video_id}/transcript_*.json")):
                print(f"Transcript for {video_id} already exists, skipping")
                continue

            # Get detailed metadata
            metadata = self.get_video_details(video_id)
            if metadata:
                metadata_file = self.metadata_dir / f"{video_id}_metadata.json"
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)

            # Get transcript
            transcript = self.get_transcript(video_id)
            if transcript:
                print(f"Successfully fetched transcript for: {video['title']}")
            else:
                print(f"No transcript available for: {video['title']}")

            time.sleep(0.5)  # Rate limiting


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
        "chomsky philosophy"
    ]

    fetcher.process_all_videos(
        search_queries,
        max_results_per_query=1000  # Adjust based on your needs
    )


if __name__ == "__main__":
    main()
