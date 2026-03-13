#!/usr/bin/env python3
"""Test our fix for issue #61269 - missing boto3 dependency."""

import sys
import os

# Add the python directory to sys.path so we can import ray modules
python_dir = "/tmp/oss-ray/python"
sys.path.insert(0, python_dir)

print("Testing our boto3 import fix...")

try:
    print("✓ Testing S3FileSystem import without boto3...")
    from ray.llm._internal.common.utils.cloud_filesystem.s3_filesystem import S3FileSystem
    print("  S3FileSystem imported successfully")
except ImportError as e:
    if "boto3" in str(e):
        print(f"✗ Still getting boto3 import error: {e}")
        sys.exit(1)
    else:
        print(f"✗ Got unexpected import error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

print("\nTesting that S3 operations fail gracefully...")

try:
    print("✗ Attempting S3 get_file operation (should fail gracefully)...")
    result = S3FileSystem.get_file("s3://test-bucket/test-file.txt")
    print(f"  Unexpected success: {result}")
    sys.exit(1)
except ImportError as e:
    if "boto3" in str(e) and "ray[llm]" in str(e):
        print(f"✓ Got expected boto3 import error with helpful message: {e}")
    else:
        print(f"✗ Got unexpected import error: {e}")
        sys.exit(1)
except Exception as e:
    print(f"✗ Got unexpected error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n✅ All tests passed! The fix correctly handles missing boto3 dependency.")
print("🎉 S3FileSystem can now be imported without requiring boto3")