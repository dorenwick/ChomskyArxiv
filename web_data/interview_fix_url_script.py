
from pymongo import MongoClient
import traceback
import os

def update_interview_urls():
    try:
        # Connect to MongoDB
        print("Connecting to MongoDB...")
        client = MongoClient("mongodb://localhost:27017/")
        db = client.ChomskyArxiv
        collection = db.ChomskyArchive

        # Find all interview documents
        interview_docs = collection.find({"type": "interview"})
        updated_count = 0

        # Base path to remove
        base_path = r"C:\Users\doren\PycharmProjects\ChomskyArchive\web_data\interviews\\"

        # Update each document
        for doc in interview_docs:
            try:
                # Get current URL and extract just the filename
                current_url = doc.get("url", "")
                if base_path in current_url:
                    # Extract filename and remove .html
                    file_name = current_url.replace(base_path, "").replace(".html", "")
                else:
                    file_name = os.path.splitext(os.path.basename(current_url))[0]

                # Create new URL
                new_url = f"https://chomsky.info/interviews/{file_name}"

                # Update document
                result = collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"url": new_url}}
                )

                if result.modified_count > 0:
                    updated_count += 1
                    print(f"Updated document {doc['paragraph_int_id']} with URL: {new_url}")

            except Exception as e:
                print(f"Error updating document {doc.get('paragraph_int_id')}: {str(e)}")
                continue

        print(f"\nUpdate complete. Updated {updated_count} documents")

    except Exception as e:
        print(f"Error: {str(e)}")
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    update_interview_urls()