import os
import shutil
from pathlib import Path
from typing import Dict, Set


def organize_downloads():
    # Set up paths
    downloads_dir = Path(r"C:\Users\doren\PycharmProjects\ChomskyArchive\downloads")

    # Get all files and their extensions
    extensions: Set[str] = set()
    files = []

    for file_path in downloads_dir.glob('*'):
        if file_path.is_file():
            ext = file_path.suffix.lower().lstrip('.')
            if ext:
                extensions.add(ext)
                files.append(file_path)

    # Create subdirectories
    for ext in extensions:
        ext_dir = downloads_dir / ext
        ext_dir.mkdir(exist_ok=True)

    # Move files to appropriate directories
    moved_files: Dict[str, int] = {ext: 0 for ext in extensions}

    for file_path in files:
        ext = file_path.suffix.lower().lstrip('.')
        if ext:
            dest_dir = downloads_dir / ext
            dest_path = dest_dir / file_path.name
            try:
                shutil.move(str(file_path), str(dest_path))
                moved_files[ext] += 1
            except Exception as e:
                print(f"Error moving {file_path.name}: {str(e)}")

    # Print summary
    print("\nFile Organization Summary:")
    for ext, count in moved_files.items():
        print(f"{ext}: {count} files")


if __name__ == "__main__":
    organize_downloads()