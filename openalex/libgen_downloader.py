
from libgen_api import LibgenSearch
import os
import json
from datetime import datetime
import logging
from tqdm import tqdm
import time
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Set


class LibGenChomskyDownloader:
    def __init__(self,
                 output_dir: str = r"C:\Users\doren\PycharmProjects\ChomskyArchive\papers",
                 openalex_file: str = r"C:\Users\doren\PycharmProjects\ChomskyArchive\openalex\chomsky_works_simplified_20241124_230519.json"):
        """Initialize the LibGen downloader with n-gram matching capabilities."""
        self.output_dir = output_dir
        self.openalex_file = openalex_file
        self.libgen = LibgenSearch()
        self.author_identifiers = ['chomsky', 'noam']
        self.openalex_works = self.load_openalex_works()
        self.setup_logger()
        os.makedirs(self.output_dir, exist_ok=True)

    def setup_logger(self):
        """Setup logging configuration."""
        log_file = os.path.join(self.output_dir, f"libgen_download_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

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

    def load_openalex_works(self) -> List[Dict]:
        """Load works from OpenAlex JSON file."""
        try:
            with open(self.openalex_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('works', [])
        except Exception as e:
            print(f"Error loading OpenAlex works: {str(e)}")
            return []

    def is_chomsky_author(self, author_string: str) -> bool:
        """Check if author string contains Chomsky identifiers."""
        author_lower = author_string.lower()
        return any(identifier in author_lower for identifier in self.author_identifiers)

    def get_ngrams(self, text: str, n: int) -> List[str]:
        """Generate n-grams from text."""
        words = text.lower().split()
        return [' '.join(words[i:i + n]) for i in range(len(words) - n + 1)]

    def should_use_trigrams(self, text: str) -> bool:
        """Determine whether to use trigrams based on word count."""
        word_count = len(text.split())
        return word_count > 4

    def get_title_variants(self, title: str) -> Set[str]:
        """Generate different variants of the title for searching."""
        variants = set()

        # Add full title
        variants.add(title.strip())

        # Handle colon-split titles
        if ':' in title:
            parts = [p.strip() for p in title.split(':')]
            variants.add(parts[0])  # Before colon
            variants.add(parts[1])  # After colon

        return variants

    def get_search_terms(self, title: str) -> Set[str]:
        """Generate comprehensive search terms from title with stricter filtering."""
        search_terms = set()
        title = title.strip()

        # Get title variants (full phrases)
        title_variants = self.get_title_variants(title)

        # Process each variant
        for variant in title_variants:
            words = variant.split()

            # 1. Always add the full variant if it meets minimum length
            if len(variant) >= 3:
                search_terms.add(variant.lower())

            # 2. Generate and add n-grams based on length
            if len(words) >= 2:
                # Add bigrams for titles with 2-4 words
                bigrams = self.get_ngrams(variant, 2)
                search_terms.update(bigram for bigram in bigrams if len(bigram) >= 3)

            if len(words) >= 3:
                # Add trigrams for longer titles
                trigrams = self.get_ngrams(variant, 3)
                search_terms.update(trigram for trigram in trigrams if len(trigram) >= 3)

            # 3. Only add individual words if:
            # - The title is very short (2 words or less)
            # - AND the word is significant (longer than 5 characters)
            if len(words) <= 2:
                significant_words = {word.lower() for word in words if len(word) > 5}
                search_terms.update(significant_words)

        # Log the final search terms for debugging
        self.logger.debug(f"Generated search terms for '{title}': {search_terms}")

        return search_terms

    def search_work(self, work: Dict) -> List[Dict]:
        """Search for a specific work using multiple search strategies."""
        try:
            title = work['display_name']
            year = work.get('publication_year', '')

            self.logger.info(f"\nProcessing: {title}")

            # Get search terms
            search_terms = self.get_search_terms(title)
            all_results = []

            # Try exact title match first
            self.logger.info(f"Trying exact match: {title}")
            exact_results = self.libgen.search_title(title)
            if exact_results:
                all_results.extend(exact_results)
                self.logger.info(f"Found {len(exact_results)} exact matches")

            # Search using all generated terms
            self.logger.info(f"Searching with terms: {search_terms}")
            for term in search_terms:
                if len(term) >= 4:  # LibGen requires minimum 3 characters
                    try:
                        results = self.libgen.search_title(term)
                        all_results.extend(results)
                        self.logger.info(f"Term '{term}' found {len(results)} results")
                    except Exception as e:
                        self.logger.warning(f"Error searching term '{term}': {str(e)}")
                    time.sleep(0.02)  # Small delay between searches

            # Filter and deduplicate results
            filtered_results = []
            seen_ids = set()

            for result in all_results:
                result_id = result.get('ID')
                if result_id in seen_ids:
                    continue

                # Check author
                if not self.is_chomsky_author(result.get('Author', '')):
                    continue

                result_title = result.get('Title', '').lower()
                original_title = title.lower()

                # Various matching strategies
                match_found = False

                # 1. Exact match
                if result_title == original_title:
                    match_found = True
                    self.logger.info(f"Exact match found: {result_title}")

                # 2. Check if result contains any search term
                elif any(term.lower() in result_title for term in search_terms):
                    match_found = True
                    self.logger.info(f"Term match found: {result_title}")

                # 3. Check for colon variants if applicable
                if ':' in original_title and not match_found:
                    before_colon = original_title.split(':')[0].strip()
                    after_colon = original_title.split(':')[1].strip()
                    if before_colon in result_title or after_colon in result_title:
                        match_found = True
                        self.logger.info(f"Colon variant match found: {result_title}")

                if match_found:
                    seen_ids.add(result_id)
                    filtered_results.append(result)

            if filtered_results:
                self.logger.info(f"Total matches for '{title}': {len(filtered_results)}")
                for result in filtered_results:
                    self.logger.info(f"Matched: {result.get('Title')} by {result.get('Author')}")

            return filtered_results

        except Exception as e:
            self.logger.error(f"Error searching for work {title}: {str(e)}")
            return []

    def process_works(self):
        """Process all OpenAlex works."""
        try:
            all_results = []

            # Process each work
            for work in tqdm(self.openalex_works, desc="Searching works"):
                results = self.search_work(work)
                all_results.extend(results)
                time.sleep(0.2)  # Delay between works

            # Save results
            results_file = os.path.join(
                self.output_dir,
                f"libgen_matches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )

            with open(results_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "metadata": {
                        "total_openalex_works": len(self.openalex_works),
                        "total_matches_found": len(all_results),
                        "ngram_method": "bigrams/trigrams",
                        "generated_at": datetime.now().isoformat()
                    },
                    "matches": all_results
                }, f, indent=2)

            self.logger.info(f"\nResults Summary:")
            self.logger.info(f"Total OpenAlex works processed: {len(self.openalex_works)}")
            self.logger.info(f"Total LibGen matches found: {len(all_results)}")
            self.logger.info(f"Results saved to: {results_file}")

            return all_results

        except Exception as e:
            self.logger.error(f"Fatal error during processing: {str(e)}")
            return []


def main():
    downloader = LibGenChomskyDownloader()
    results = downloader.process_works()
    print(f"\nFound {len(results)} matching works in LibGen")


if __name__ == "__main__":
    main()
