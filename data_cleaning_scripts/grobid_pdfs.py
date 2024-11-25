import os
import subprocess
from pathlib import Path
from langdetect import detect
import shutil

"""
TODO before running:
1. Install Docker if not installed
2. Pull Grobid image: 
   docker pull lfoppiano/grobid:0.8.0
3. Run Grobid container:
   docker run --rm --init --ulimit core=0 -p 8070:8070 lfoppiano/grobid:0.8.0
4. Verify it's running:
   curl localhost:8070/api/version
"""


def process_pdfs():
    input_dir = Path(r"C:\Users\doren\PycharmProjects\ChomskyArchive\downloads\pdf")
    output_base = Path(r"C:\Users\doren\PycharmProjects\ChomskyArchive\grobid_pdf_to_xml")

    # Create language directories
    lang_dirs = {
        'en': output_base / 'english',
        'es': output_base / 'spanish',
        'fr': output_base / 'french',
        'de': output_base / 'german',
        'ru': output_base / 'russian',
        'misc': output_base / 'misc'
    }

    for dir_path in lang_dirs.values():
        dir_path.mkdir(parents=True, exist_ok=True)

    for pdf_file in input_dir.glob('*.pdf'):
        print(f"Processing: {pdf_file.name}")

        # Process with Grobid
        command = f'curl -s --form input=@"{pdf_file}" localhost:8070/api/processFulltextDocument'
        try:
            output = subprocess.check_output(command, shell=True)

            # Detect language from first 5000 characters
            text_content = output.decode('utf-8')[:10000]
            try:
                lang = detect(text_content)
            except:
                lang = 'misc'

            # Save to appropriate language directory
            output_dir = lang_dirs.get(lang, lang_dirs['misc'])
            output_path = output_dir / f"{pdf_file.stem}.xml"

            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(output.decode('utf-8'))

            print(f"Saved XML to: {output_path}")

        except Exception as e:
            print(f"Error processing {pdf_file.name}: {e}")


if __name__ == "__main__":
    process_pdfs()