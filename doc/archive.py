import os
import subprocess

with open("/Users/kevin/ray/files.txt", "w") as f:
    list_files = subprocess.check_output(["git", "ls-files", "--others"]).decode("utf-8")
    f.write(list_files)

with open("/Users/kevin/ray/pending_files.txt", "w") as f:
    list_files = subprocess.check_output(["git", "diff", "HEAD", "--name-only"]).decode("utf-8")
    f.write(list_files)

import zipfile
import os

# Path to your text file containing file paths
file_list_path = '/Users/kevin/ray/files.txt'
pending_files_path = '/Users/kevin/ray/pending_files.txt'

latest_origin_main_commit = subprocess.check_output(["git", "log", "--pretty=format:%H", "--first-parent", "origin/master", "-n", "1"]).decode("utf-8")

# Name of the zip file you want to create
os.makedirs(f"/Users/kevin/ray/{latest_origin_main_commit}", exist_ok=True)
build_archive_name = f"/Users/kevin/ray/{latest_origin_main_commit}/doc.zip"

# Create a ZipFile object
with zipfile.ZipFile(build_archive_name, 'w') as zipf:
    # Open the file containing the list of file paths
    with open(pending_files_path, 'r') as pending_files:
        pending_file_list = pending_files.read().splitlines()
        with open(file_list_path, 'r') as file_list:
            # Read each line (file path) from the text file
            for file_path in file_list:
                # Strip any whitespace/newlines from the file path
                file_path = file_path.strip()
                
                # Check if the file exists
                if os.path.isfile(file_path) and file_path.startswith("doc/") and file_path not in pending_file_list:
                    # Add file to the zip archive
                    zipf.write(file_path)
                    print("writing file: ", file_path)
                else:
                    print(f"Warning: File not found - {file_path}")

print(f"Archive created!")