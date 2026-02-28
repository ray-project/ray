#!/usr/bin/env python3
"""Reproduce issue #61269 - missing boto3 dependency for ray[serve] installation."""

import sys
import traceback

print("Python version:", sys.version)

print("\nTesting imports that should work without boto3...")

try:
    print("✓ Importing ray...")
    import ray
    print(f"  Ray version: {ray.__version__}")
except ImportError as e:
    print(f"✗ Failed to import ray: {e}")
    sys.exit(1)

try:
    print("✓ Importing ray.serve...")
    import ray.serve
    print("  ray.serve imported successfully")
except ImportError as e:
    print(f"✗ Failed to import ray.serve: {e}")
    sys.exit(1)

try:
    print("✓ Testing import ray.serve.llm (this should now work)...")
    from ray.serve.llm import LLMConfig, build_openai_app
    print("  ray.serve.llm imported successfully")
except ImportError as e:
    print(f"✗ Failed to import ray.serve.llm: {e}")
    print("Traceback:")
    traceback.print_exc()
    sys.exit(1)

print("\nTesting S3 functionality without boto3...")

try:
    print("✓ Importing S3FileSystem...")
    from ray.llm._internal.common.utils.cloud_filesystem.s3_filesystem import S3FileSystem
    print("  S3FileSystem imported successfully")
except ImportError as e:
    print(f"✗ Failed to import S3FileSystem: {e}")
    traceback.print_exc()
    sys.exit(1)

print("\nTesting S3 operations that should fail gracefully without boto3...")

try:
    print("✗ Attempting S3 get_file operation (should fail gracefully)...")
    result = S3FileSystem.get_file("s3://test-bucket/test-file.txt")
    print(f"  Unexpected success: {result}")
    sys.exit(1)
except ImportError as e:
    if "boto3" in str(e) and "ray[llm]" in str(e):
        print(f"✓ Got expected boto3 import error: {e}")
    else:
        print(f"✗ Got unexpected import error: {e}")
        sys.exit(1)
except Exception as e:
    print(f"✗ Got unexpected error: {e}")
    sys.exit(1)

print("\n✅ All tests passed! The fix correctly handles missing boto3 dependency.")
print("🎉 ray[serve] can now be imported without requiring boto3/ray[llm]")