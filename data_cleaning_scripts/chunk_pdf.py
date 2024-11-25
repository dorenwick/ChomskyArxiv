import PyPDF2
import re
import json
from pathlib import Path
import uuid


def detect_chapter_boundaries(pdf_reader):
    chapter_markers = []
    for page_num in range(len(pdf_reader.pages)):
        text = pdf_reader.pages[page_num].extract_text()
        patterns = [
            r'^chapter \d+',
            r'^Chapter \d+',
            r'^CHAPTER \d+',
        ]
        if any(re.search(pattern, text, re.MULTILINE) for pattern in patterns):
            chapter_markers.append(page_num)

    if not chapter_markers:
        total_pages = len(pdf_reader.pages)
        chunk_size = min(2, total_pages // 3)
        chapter_markers = list(range(0, total_pages, chunk_size))
    return chapter_markers


def split_pdf(input_file, output_dir):
    with open(input_file, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        chapter_markers = detect_chapter_boundaries(pdf_reader)
        chunk_files = []

        for i in range(len(chapter_markers)):
            chunk_id = str(uuid.uuid4())
            start = chapter_markers[i]
            end = chapter_markers[i + 1] if i + 1 < len(chapter_markers) else len(pdf_reader.pages)

            pdf_writer = PyPDF2.PdfWriter()
            for page_num in range(start, end):
                pdf_writer.add_page(pdf_reader.pages[page_num])

            output_path = output_dir / f"{chunk_id}.pdf"
            with open(output_path, 'wb') as output_file:
                pdf_writer.write(output_file)

            chunk_files.append({
                'chunk_id': chunk_id,
                'start_page': start,
                'end_page': end - 1,
                'file_path': str(output_path)
            })

        return chunk_files


def process_directory():
    input_dir = Path(r"C:\Users\doren\PycharmProjects\ChomskyArchive\downloads\pdf")
    output_dir = Path(r"C:\Users\doren\PycharmProjects\ChomskyArchive\grobid_pdf_to_xml\chunked_pdf")
    output_dir.mkdir(exist_ok=True)

    chunk_mapping = {}

    for pdf_file in input_dir.glob("*.pdf"):
        try:
            chunk_files = split_pdf(pdf_file, output_dir)
            chunk_mapping[pdf_file.name] = chunk_files
            print(f"Successfully processed: {pdf_file.name}")
        except Exception as e:
            print(f"Error processing {pdf_file.name}: {e}")

    mapping_file = output_dir / 'chunk_mapping.json'
    with open(mapping_file, 'w') as f:
        json.dump(chunk_mapping, f, indent=2)


if __name__ == "__main__":
    process_directory()