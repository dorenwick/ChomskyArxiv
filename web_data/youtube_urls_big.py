
import json
from pathlib import Path
from typing import List
from tqdm import tqdm


def extract_youtube_urls(transcripts_dir: str) -> List[str]:
    """Extract YouTube URLs from transcript JSON files."""
    urls = []
    transcripts_path = Path(transcripts_dir)

    # Find all JSON files in directory and subdirectories
    json_files = list(transcripts_path.rglob("*.json"))
    print(f"Found {len(json_files)} JSON files")

    for json_file in tqdm(json_files, desc="Processing files"):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

                # Extract URL if it exists
                if 'url' in data:
                    url = data['url']
                    if url and url.strip():  # Check if URL is not empty
                        urls.append(url)
                        print(f"Found URL: {url}")

        except Exception as e:
            print(f"Error processing {json_file}: {str(e)}")
            continue

    # Remove duplicates while preserving order
    unique_urls = list(dict.fromkeys(urls))
    print(f"\nFound {len(unique_urls)} unique URLs")

    # Save to JSON file
    output_file = Path("youtube_urls_two.json")
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                "total_urls": len(unique_urls),
                "youtube_urls": unique_urls
            }, f, indent=2, ensure_ascii=False)
        print(f"Saved URLs to {output_file.absolute()}")
    except Exception as e:
        print(f"Error saving URLs: {str(e)}")

    return unique_urls


def main():
    transcripts_dir = r"C:\Users\doren\PycharmProjects\ChomskyArchive\web_data\transcripts"
    urls = extract_youtube_urls(transcripts_dir)

    # Print sample of found URLs
    if urls:
        print("\nSample of found URLs:")
        for url in urls[:5]:
            print(f"- {url}")


if __name__ == "__main__":
    main()
