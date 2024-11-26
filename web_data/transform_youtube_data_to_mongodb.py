from transformers import BertTokenizer
from pymongo import MongoClient
import json
import os
from datetime import datetime
from typing import List, Dict, Any
import re
import traceback

class TranscriptProcessor:
    def __init__(self, mongodb_url: str = "mongodb://localhost:27017/"):
        try:
            # Initialize MongoDB connection
            print("Initializing MongoDB connection...")
            self.client = MongoClient(mongodb_url)
            self.db = self.client.ChomskyArxiv
            self.collection = self.db.ChomskyArchive
            print("MongoDB connection successful")

            # Initialize BERT tokenizer
            print("Loading BERT tokenizer...")
            self.tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
            print("BERT tokenizer loaded successfully")

            # Initialize counter for paragraph IDs
            self.current_paragraph_id = self._get_max_paragraph_id()
            print(f"Current maximum paragraph ID: {self.current_paragraph_id}")
        except Exception as e:
            print(f"Error during initialization: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            raise


    def _combine_transcript_segments(self, segments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Combine transcript segments into paragraphs within token limit."""
        try:
            print(f"Combining {len(segments)} transcript segments...")
            combined_segments = []
            current_segment = {
                "text": "",
                "start": None,
                "end": None
            }

            for i, segment in enumerate(segments):
                try:
                    # Start new segment if this is the first one
                    if current_segment["start"] is None:
                        current_segment["start"] = segment["start"]
                        current_segment["text"] = segment["text"]
                        print(f"Started new segment at timestamp {segment['start']}")
                        continue

                    # Try adding the next segment
                    potential_text = f"{current_segment['text']} {segment['text']}"
                    tokens = self.tokenizer.encode(potential_text)

                    # Check if adding would exceed token limit or if current segment ends with period
                    if len(tokens) > 384 or current_segment["text"].strip().endswith('.'):
                        current_segment["end"] = segment["start"]
                        combined_segments.append(current_segment)
                        print(f"Segment complete: {len(tokens)} tokens, "
                              f"from {self._format_timestamp(current_segment['start'])} "
                              f"to {self._format_timestamp(current_segment['end'])}")

                        current_segment = {
                            "text": segment["text"],
                            "start": segment["start"],
                            "end": None
                        }
                    else:
                        current_segment["text"] = potential_text

                except Exception as e:
                    print(f"Error processing segment {i}: {str(e)}")
                    print(f"Segment content: {segment}")
                    print(f"Traceback: {traceback.format_exc()}")
                    continue

            # Add the last segment if it exists
            if current_segment["text"]:
                current_segment["end"] = segments[-1]["start"] + segments[-1]["duration"]
                combined_segments.append(current_segment)
                print(f"Added final segment: {self._format_timestamp(current_segment['start'])} "
                      f"to {self._format_timestamp(current_segment['end'])}")

            print(f"Successfully combined into {len(combined_segments)} paragraphs")
            return combined_segments

        except Exception as e:
            print(f"Error in combine_transcript_segments: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            return []

    def _format_timestamp(self, seconds: float) -> str:
        """Convert seconds to MM:SS format."""
        try:
            minutes = int(seconds // 60)
            remaining_seconds = int(seconds % 60)
            return f"{minutes}:{remaining_seconds:02d}"
        except Exception as e:
            print(f"Error formatting timestamp {seconds}: {str(e)}")
            return "0:00"

    def _get_max_paragraph_id(self) -> int:
        """Get the maximum existing paragraph_int_id from MongoDB."""
        try:
            max_doc = self.collection.find_one(sort=[("paragraph_int_id", -1)])
            max_id = max_doc["paragraph_int_id"] if max_doc else 0
            print(f"Found current maximum paragraph_int_id in collection: {max_id}")
            print(f"Will start new documents at ID: {max_id + 1}")
            return max_id

        except Exception as e:
            print(f"Error getting max paragraph ID: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")
            return 0

    def process_transcript(self, transcript_path: str) -> None:
        """Process a single transcript file and store in MongoDB."""
        try:
            print(f"\nProcessing transcript: {transcript_path}")
            print(f"Starting with paragraph_int_id: {self.current_paragraph_id + 1}")

            # Read transcript file
            print("Reading transcript file...")
            with open(transcript_path, 'r', encoding='utf-8') as f:
                transcript_data = json.load(f)
            print(f"Successfully loaded transcript with {len(transcript_data['transcript'])} segments")

            # Combine segments into paragraphs
            print("Combining segments into paragraphs...")
            combined_segments = self._combine_transcript_segments(transcript_data["transcript"])
            print(f"Created {len(combined_segments)} combined segments")

            # Process each combined segment
            for i, segment in enumerate(combined_segments):
                try:
                    next_id = self.current_paragraph_id + 1
                    print(f"\nProcessing segment {i + 1}/{len(combined_segments)}")
                    print(f"Using paragraph_int_id: {next_id}")

                    # Create document
                    document = {
                        "paragraph_int_id": next_id,
                        "type": "youtube",
                        "source": transcript_data.get("url", ""),
                        "url": transcript_data.get("url", ""),
                        "timestamp": {
                            "start": self._format_timestamp(segment["start"]),
                            "end": self._format_timestamp(segment["end"])
                        },
                        "location": f"{self._format_timestamp(segment['start'])} - {self._format_timestamp(segment['end'])}",
                        "text": segment["text"].strip(),
                        "keyphrases": [],
                        "keynames": [],
                        "questions": [],
                        "answers": [],
                        "topic": None,
                        "processed_date": datetime.now()
                    }

                    print(f"Created document for timestamp: {document['location']}")
                    print(f"Text length: {len(document['text'])} characters")

                    # Insert into MongoDB
                    print(f"Inserting document with ID {next_id} into MongoDB...")
                    self.collection.insert_one(document)
                    self.current_paragraph_id = next_id  # Update only after successful insertion
                    print("Successfully inserted document")

                except Exception as e:
                    print(f"Error processing segment {i}: {str(e)}")
                    print(f"Segment content: {segment}")
                    print(f"Traceback: {traceback.format_exc()}")
                    continue

            print(f"\nSuccessfully completed processing of {transcript_path}")
            print(f"Final paragraph_int_id: {self.current_paragraph_id}")

        except Exception as e:
            print(f"Error processing transcript {transcript_path}: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")

    def process_directory(self, directory_path: str) -> None:
        """Process all transcript files in a directory and its subdirectories."""
        try:
            print(f"\nProcessing directory: {directory_path}")
            transcript_count = 0

            # Walk through all subdirectories
            for root, _, files in os.walk(directory_path):
                # Filter for transcript files (any JSON file in the transcripts directory)
                transcript_files = [f for f in files if f.startswith('transcript_') and f.endswith('.json')]

                for file in transcript_files:
                    transcript_path = os.path.join(root, file)
                    print(f"\nFound transcript file: {transcript_path}")

                    try:
                        self.process_transcript(transcript_path)
                        transcript_count += 1
                        print(f"Processed transcript {transcript_count}")
                    except Exception as e:
                        print(f"Failed to process transcript {transcript_path}: {str(e)}")
                        print(f"Traceback: {traceback.format_exc()}")
                        continue

            print(f"\nProcessing completed. Total transcripts processed: {transcript_count}")

        except Exception as e:
            print(f"Error processing directory {directory_path}: {str(e)}")
            print(f"Traceback: {traceback.format_exc()}")

def main():
    try:
        print("Starting transcript processing...")

        # Initialize processor
        processor = TranscriptProcessor()

        # Process transcripts directory
        transcripts_dir = r"C:\Users\doren\PycharmProjects\ChomskyArchive\web_data\transcripts"
        processor.process_directory(transcripts_dir)

        print("\nTranscript processing completed successfully")

    except Exception as e:
        print(f"Fatal error in main: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    main()