import pickle
from sphinx.project import Project
import os
import time
from datetime import datetime, timezone

RAY_DOC_DIR = "/Users/kevin/ray/doc"
files = []
with open("/Users/kevin/ray/pending_files.txt", "r") as f:
    files = f.readlines()
    files = [file.strip() for file in files]
with open(os.path.join(RAY_DOC_DIR, "_build/doctrees/environment.pickle"), "rb") as f:
    env = pickle.load(f)
    env.srcdir = os.path.join(RAY_DOC_DIR, "source")
    env.doctreedir = os.path.join(RAY_DOC_DIR, "_build/doctrees")
    env.project.srcdir = os.path.join(RAY_DOC_DIR, "source")
    p = Project(os.path.join(RAY_DOC_DIR, "source"), {'.rst': 'restructuredtext', '.md': 'myst-nb', '.ipynb': 'myst-nb'})
    p.discover()
    env.project = p

    for doc, val in env.all_docs.items():
        if doc not in files:
            env.all_docs[doc] = int(time.time()) * 1000000

with open(os.path.join(RAY_DOC_DIR, "_build/doctrees/environment.pickle"), "wb+") as f:
    pickle.dump(env, f, pickle.HIGHEST_PROTOCOL)

new_time = datetime(2000, 7, 22, 0, 0, tzinfo=timezone.utc)
new_timestamp = new_time.timestamp()
os.utime(f"{RAY_DOC_DIR}/Makefile", (new_timestamp, new_timestamp))

directory = f"{RAY_DOC_DIR}/source/_templates"
# Get the current year
current_year = datetime.now().year

# Create a datetime object for July 22 12:00 AM UTC of the current year
new_time = datetime(2000, 7, 22, 0, 0, tzinfo=timezone.utc)

# Convert to timestamp
new_timestamp = new_time.timestamp()

# Walk through the directory
for root, dirs, files in os.walk(directory):
    for file in files:
        file_path = os.path.join(root, file)
        try:
            # Change the access and modification times
            os.utime(file_path, (new_timestamp, new_timestamp))
            print(f"Changed timestamp for: {file_path}")
        except Exception as e:
            print(f"Failed to change timestamp for {file_path}: {str(e)}")

print("Timestamp change operation completed.")