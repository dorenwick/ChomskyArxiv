import logging
from datetime import datetime
from typing import Dict, List, Tuple
from pymongo import MongoClient
from tqdm import tqdm
from collections import defaultdict


class KnowledgeGraphFinalizer:
    """


    """

    def __init__(
            self,
            mongo_url: str = "mongodb://localhost:27017/",
            db_name: str = "ChomskyArxiv",
            log_dir: str = "logs"
    ):
        self.client = MongoClient(mongo_url)
        self.db = self.client[db_name]
        self.logger = self._setup_logger(log_dir)

    def _setup_logger(self, log_dir: str) -> logging.Logger:
        logger = logging.getLogger("KnowledgeGraphFinalizer")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def add_integer_ids(self):
        """Add integer IDs to keyphrases, keynames, and questions collections"""
        self.logger.info("Adding integer IDs to collections...")

        # Add keyphrase_int_id
        for idx, doc in enumerate(self.db.Keyphrases.find(), 1):
            self.db.Keyphrases.update_one(
                {"_id": doc["_id"]},
                {"$set": {"keyphrase_int_id": idx}}
            )

        # Add keyname_int_id
        for idx, doc in enumerate(self.db.Keynames.find(), 1):
            self.db.Keynames.update_one(
                {"_id": doc["_id"]},
                {"$set": {"keyname_int_id": idx}}
            )

        # Add question_int_id
        for idx, doc in enumerate(self.db.Questions.find(), 1):
            self.db.Questions.update_one(
                {"_id": doc["_id"]},
                {"$set": {"question_int_id": idx}}
            )

        self.logger.info("Integer IDs added successfully")

    def create_works_collection(self):
        """Create the Works collection with all associated data"""
        self.logger.info("Creating Works collection...")

        # Get all unique work_int_ids
        work_ids = self.db.Paragraphs.distinct("work_int_id")

        for work_id in tqdm(work_ids, desc="Processing works"):
            # Get all paragraphs for this work
            paragraphs = list(self.db.Paragraphs.find({"work_int_id": work_id}))
            if not paragraphs:
                continue

            # Get source and url from first paragraph
            source = paragraphs[0]["source"]
            url = paragraphs[0].get("url", "")

            # Get all paragraph IDs
            paragraph_ids = [p["paragraph_int_id"] for p in paragraphs]

            # Get all keyphrases
            keyphrases = list(self.db.Keyphrases.find(
                {"all_work_int_ids": work_id},
                {"keyphrase": 1, "keyphrase_int_id": 1}
            ))
            keyphrase_pairs = [(k["keyphrase"], k["keyphrase_int_id"]) for k in keyphrases]

            # Get all keynames
            keynames = list(self.db.Keynames.find(
                {"all_work_int_ids": work_id},
                {"keyname": 1, "keyname_int_id": 1}
            ))
            keyname_pairs = [(k["keyname"], k["keyname_int_id"]) for k in keynames]

            # Get all questions
            questions = list(self.db.Questions.find(
                {"work_int_id": work_id},
                {"question": 1, "question_int_id": 1}
            ))
            question_pairs = [(q["question"], q["question_int_id"]) for q in questions]

            # Create work document
            work_doc = {
                "work_int_id": work_id,
                "all_paragraph_init_ids": paragraph_ids,
                "source": source,
                "url": url,
                "all_keyphrases": keyphrase_pairs,
                "all_keynames": keyname_pairs,
                "all_questions": question_pairs,
                "all_topics": []  # Empty list as specified
            }

            # Insert or update work document
            self.db.Works.update_one(
                {"work_int_id": work_id},
                {"$set": work_doc},
                upsert=True
            )

        self.logger.info("Works collection created successfully")

    def generate_statistics(self) -> Dict[str, Dict[str, int]]:
        """Generate comprehensive statistics about the knowledge graph"""
        stats = {}

        # Work statistics by type
        work_types = defaultdict(int)
        for work in self.db.Works.find({}, {"source": 1}):
            # Assuming type is stored in the first part of the source or can be derived
            work_type = work.get("type", "unknown")
            work_types[work_type] += 1

        stats["works_by_type"] = dict(work_types)

        # Total counts
        stats["total_counts"] = {
            "works": self.db.Works.count_documents({}),
            "questions": self.db.Questions.count_documents({}),
            "keynames": self.db.Keynames.count_documents({}),
            "keyphrases": self.db.Keyphrases.count_documents({}),
            "paragraphs": self.db.Paragraphs.count_documents({})
        }

        return stats

    def print_statistics_report(self, stats: Dict[str, Dict[str, int]]):
        """Print a formatted statistics report"""
        self.logger.info("\n=== Knowledge Graph Statistics Report ===\n")

        self.logger.info("Works by Type:")
        for work_type, count in stats["works_by_type"].items():
            self.logger.info(f"  {work_type}: {count:,}")

        self.logger.info("\nTotal Counts:")
        for item, count in stats["total_counts"].items():
            self.logger.info(f"  Total {item}: {count:,}")

    def process(self):
        """Run the complete finalization process"""
        try:
            # Create necessary indexes
            self.db.Works.create_index("work_int_id", unique=True)

            # Add integer IDs
            self.add_integer_ids()

            # Create Works collection
            self.create_works_collection()

            # Generate and print statistics
            stats = self.generate_statistics()
            self.print_statistics_report(stats)

        except Exception as e:
            self.logger.error(f"Error in finalization process: {e}")
            raise
        finally:
            self.client.close()


class KnowledgeGraphStatistics:
    def __init__(
            self,
            mongo_url: str = "mongodb://localhost:27017/",
            db_name: str = "ChomskyArxiv",
            log_dir: str = "logs"
    ):
        self.client = MongoClient(mongo_url)
        self.db = self.client[db_name]
        self.logger = self._setup_logger(log_dir)

    def _setup_logger(self, log_dir: str) -> logging.Logger:
        logger = logging.getLogger("KnowledgeGraphStatistics")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def update_works_with_type(self):
        """Update Works collection to include type field from Paragraphs"""
        self.logger.info("Updating Works collection with type field...")

        # Get all works
        works = self.db.Works.find({})

        for work in tqdm(works, desc="Updating work types"):
            # Get type from the first paragraph of this work
            first_paragraph = self.db.Paragraphs.find_one(
                {"work_int_id": work["work_int_id"]},
                {"type": 1}
            )

            if first_paragraph and "type" in first_paragraph:
                self.db.Works.update_one(
                    {"_id": work["_id"]},
                    {"$set": {"type": first_paragraph["type"]}}
                )

    def generate_statistics(self) -> Dict[str, Dict[str, int]]:
        """Generate comprehensive statistics about the knowledge graph"""
        stats = {}

        # Work statistics by type
        work_types = defaultdict(int)
        for work in self.db.Works.find({}, {"type": 1}):
            work_type = work.get("type", "unknown")
            work_types[work_type] += 1

        stats["works_by_type"] = dict(work_types)

        # Total counts
        stats["total_counts"] = {
            "works": self.db.Works.count_documents({}),
            "questions": self.db.Questions.count_documents({}),
            "keynames": self.db.Keynames.count_documents({}),
            "keyphrases": self.db.Keyphrases.count_documents({}),
            "paragraphs": self.db.Paragraphs.count_documents({})
        }

        return stats

    def print_statistics_report(self, stats: Dict[str, Dict[str, int]]):
        """Print a formatted statistics report"""
        self.logger.info("\n=== Knowledge Graph Statistics Report ===\n")

        total_works = sum(stats["works_by_type"].values())

        self.logger.info("Works by Type:")
        for work_type, count in sorted(stats["works_by_type"].items()):
            percentage = (count / total_works * 100) if total_works > 0 else 0
            self.logger.info(f"  {work_type}: {count:,} ({percentage:.1f}%)")

        self.logger.info("\nTotal Counts:")
        for item, count in stats["total_counts"].items():
            self.logger.info(f"  Total {item}: {count:,}")

    def process(self):
        """Run the complete statistics process"""
        try:
            # Update Works with type field
            self.update_works_with_type()

            # Generate and print statistics
            stats = self.generate_statistics()
            self.print_statistics_report(stats)

        except Exception as e:
            self.logger.error(f"Error in statistics process: {e}")
            raise
        finally:
            self.client.close()


def main():
    stats_generator = KnowledgeGraphStatistics()
    stats_generator.process()


if __name__ == "__main__":
    main()