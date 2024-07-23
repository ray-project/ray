import os
import time
from datetime import datetime, timezone

# Specify the directory
# directory = '/Users/kevin/miniconda3/envs/docs/lib/python3.12/site-packages/sphinx'
#directory = '/Users/kevin/miniconda3/envs/docs/lib/python3.12/site-packages/pydata_sphinx_theme'
directory = "/Users/kevin/test/ray/doc/source/_templates"
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

# new_time = datetime(2024, 7, 22, 0, 0, tzinfo=timezone.utc)
# new_timestamp = new_time.timestamp()
# os.utime("/Users/kevin/test/ray/doc/source/_templates/main-sidebar.html", (new_timestamp, new_timestamp))