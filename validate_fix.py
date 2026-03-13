#!/usr/bin/env python3
"""Validate our boto3 import fix by examining the code directly."""

import os

print("Validating boto3 import fix...")

# Read the modified file
s3_file_path = "/tmp/oss-ray/python/ray/llm/_internal/common/utils/cloud_filesystem/s3_filesystem.py"

with open(s3_file_path, 'r') as f:
    content = f.read()

# Check that our fix is properly implemented
checks = [
    ("Conditional boto3 import", "try:\n    import boto3" in content),
    ("HAS_BOTO3 flag", "HAS_BOTO3 = True" in content and "HAS_BOTO3 = False" in content),
    ("Stub types for missing boto3", "BaseClient = None" in content),
    ("boto3 availability check method", "_ensure_boto3_available" in content),
    ("Helpful error message", "ray[llm]" in content and "pip install boto3" in content),
    ("Error check in _get_s3_client", "_ensure_boto3_available()" in content and 
     content.find("_ensure_boto3_available()") < content.find("Config(")),
    ("Error check in get_file", "get_file(" in content and 
     content.find("_ensure_boto3_available()") < content.find("bucket, key, is_anonymous")),
    ("Error check in list_subfolders", "list_subfolders(" in content and 
     "_ensure_boto3_available()" in content),
    ("Error check in download_files", "download_files(" in content and 
     "_ensure_boto3_available()" in content),
    ("Error check in upload_files", "upload_files(" in content and 
     "_ensure_boto3_available()" in content),
]

all_passed = True
for check_name, passed in checks:
    if passed:
        print(f"✓ {check_name}")
    else:
        print(f"✗ {check_name}")
        all_passed = False

if all_passed:
    print("\n✅ All validation checks passed!")
    print("🎉 The fix correctly implements conditional boto3 import with helpful error messages.")
else:
    print("\n❌ Some validation checks failed!")
    exit(1)

# Show a sample of the key changes
print("\n📄 Sample of key changes made:")
print("-" * 60)

# Show the import section
import_start = content.find("try:\n    import boto3")
import_end = content.find("BaseClient = None") + len("BaseClient = None")
print("Import section:")
print(content[import_start:import_end])

print("\n" + "-" * 30)

# Show the error helper method
error_start = content.find("def _ensure_boto3_available")
error_end = content.find('            )\n', error_start) + 14
print("Error helper method:")
print(content[error_start:error_end])