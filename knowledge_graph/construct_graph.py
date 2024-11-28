import os
import logging
from datetime import datetime
from typing import List, Dict, Any
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne
import torch
from span_marker import SpanMarkerModel
from transformers import AutoTokenizer, AutoModelForSequenceClassification


class KnowledgeGraphBuilder:
    def __init__(
            self,
            mongo_url: str = "mongodb://localhost:27017/",
            db_name: str = "ChomskyArxiv",
            batch_size: int = 100,
            question_threshold: float = 0.5,
            log_dir: str = "logs"
    ):
        self.mongo_url = mongo_url
        self.db_name = db_name
        self.batch_size = batch_size
        self.question_threshold = question_threshold

        # Initialize MongoDB connection
        self.client = MongoClient(mongo_url)
        self.db = self.client[db_name]

        # Set up logging
        self.logger = self._setup_logger(log_dir)

        # Initialize models
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._init_models()

        # Create indexes for collections
        self._create_indexes()

    def _setup_logger(self, log_dir: str) -> logging.Logger:
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"knowledge_graph_{timestamp}.log")

        logger = logging.getLogger("KnowledgeGraphBuilder")
        logger.setLevel(logging.INFO)

        file_handler = logging.FileHandler(log_file)
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    def _init_models(self):
        """Initialize NER and Question Detection models"""
        self.logger.info("Initializing models...")

        # Initialize NER model
        self.ner_model = SpanMarkerModel.from_pretrained(
            "tomaarsen/span-marker-mbert-base-fewnerd-fine-super"
        ).to(self.device)

        # Initialize Question Detection model
        self.question_tokenizer = AutoTokenizer.from_pretrained("huaen/question_detection")
        self.question_model = AutoModelForSequenceClassification.from_pretrained(
            "huaen/question_detection"
        ).to(self.device)

        self.logger.info("Models initialized successfully")

    def _create_indexes(self):
        """Create necessary indexes for all collections"""
        # Paragraphs collection
        # self.db.Paragraphs.create_index("paragraph_int_id", unique=True)
        self.db.Paragraphs.create_index("work_int_id")

        # Questions collection
        self.db.Questions.create_index([("question", 1), ("paragraph_int_id", 1)], unique=True)

        # Keyphrases collection
        self.db.Keyphrases.create_index("keyphrase", unique=True)

        # Keynames collection
        self.db.Keynames.create_index("keyname", unique=True)

    def add_work_ids(self):
        """Add work_int_id to paragraphs based on unique titles"""
        self.logger.info("Adding work_int_ids to paragraphs...")

        # Get unique sources
        sources = self.db.Paragraphs.distinct("source")

        # Create work_int_id mapping
        for work_id, source in enumerate(sources, 1):
            self.db.Paragraphs.update_many(
                {"source": source},
                {"$set": {"work_int_id": work_id}}
            )

        self.logger.info(f"Added work_int_ids for {len(sources)} works")

    def detect_question(self, text: str) -> tuple[bool, float]:
        """Detect if a text is a question and return score"""
        with torch.no_grad():
            inputs = self.question_tokenizer(
                text,
                padding=True,
                truncation=True,
                max_length=512,
                return_tensors="pt"
            ).to(self.device)

            outputs = self.question_model(**inputs)
            probs = torch.softmax(outputs.logits, dim=1)
            question_score = probs[0][1].item()

            return question_score >= self.question_threshold, question_score

    def extract_entities(self, text: str) -> List[Dict[str, Any]]:
        """Extract named entities from text"""
        return self.ner_model.predict(text)

    def process_paragraph(self, paragraph: Dict[str, Any]):
        """Process a single paragraph to extract questions and entities"""
        text = paragraph["text"]
        paragraph_id = paragraph["paragraph_int_id"]
        work_id = paragraph["work_int_id"]

        # Extract questions
        is_question, score = self.detect_question(text)
        if is_question:
            self.db.Questions.update_one(
                {
                    "question": text,
                    "paragraph_int_id": paragraph_id
                },
                {
                    "$set": {
                        "score": score,
                        "work_int_id": work_id
                    }
                },
                upsert=True
            )

        # Extract entities
        entities = self.extract_entities(text)
        for entity in entities:
            entity_data = {
                "label": entity["label"],
                "score": entity["score"],
                "all_paragraph_init_ids": [paragraph_id],
                "all_work_int_ids": [work_id]
            }

            # Check if entity is a person by checking if label starts with "person-"
            if entity["label"].startswith("person-"):
                # Add to Keynames
                self.db.Keynames.update_one(
                    {"keyname": entity["span"]},
                    {
                        "$set": {"label": entity["label"], "score": entity["score"]},
                        "$addToSet": {
                            "all_paragraph_init_ids": paragraph_id,
                            "all_work_int_ids": work_id
                        }
                    },
                    upsert=True
                )
            else:
                # Add to Keyphrases
                self.db.Keyphrases.update_one(
                    {"keyphrase": entity["span"]},
                    {
                        "$set": {"label": entity["label"], "score": entity["score"]},
                        "$addToSet": {
                            "all_paragraph_init_ids": paragraph_id,
                            "all_work_int_ids": work_id
                        }
                    },
                    upsert=True
                )

    def build_knowledge_graph(self, resume_from_id: int = 7462):
        """Build knowledge graph with batch processing, resuming from specified ID"""
        try:
            self.logger.info("Starting knowledge graph construction...")

            # First ensure all paragraphs have work_int_id
            self.add_work_ids()

            # Process all paragraphs
            total_paragraphs = self.db.Paragraphs.count_documents({})
            processed = 0
            batch_size = 1000

            # Query with paragraph_int_id filter
            query = {"paragraph_int_id": {"$gt": resume_from_id}}
            remaining_paragraphs = self.db.Paragraphs.count_documents(query)

            self.logger.info(f"Resuming from paragraph_int_id: {resume_from_id}")
            self.logger.info(f"Remaining paragraphs to process: {remaining_paragraphs:,}")

            while processed < remaining_paragraphs:
                cursor = self.db.Paragraphs.find(query) \
                    .skip(processed) \
                    .limit(batch_size) \
                    .hint("paragraph_int_id_1")

                batch_processed = 0
                for paragraph in tqdm(cursor,
                                      total=min(batch_size, remaining_paragraphs - processed),
                                      desc=f"Batch starting at {resume_from_id + processed}"):
                    self.process_paragraph(paragraph)
                    batch_processed += 1
                    processed += 1

                if processed % 100 == 0:
                    self.logger.info(
                        f"Processed {processed:,} additional paragraphs "
                        f"(Total progress: {(processed + resume_from_id):,}/{total_paragraphs:,})"
                    )

                cursor.close()

            self.logger.info("Knowledge graph construction completed successfully")

        except Exception as e:
            self.logger.error(f"Error building knowledge graph: {str(e)}")
            raise
        finally:
            self.client.close()

# Modified main function to include resume point
def main():
    builder = KnowledgeGraphBuilder()
    builder.build_knowledge_graph(resume_from_id=7462)

if __name__ == "__main__":
    main()