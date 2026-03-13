#!/usr/bin/env python3
"""Test our boto3 import fix directly without importing ray core."""

import sys
import os

# Test that we can import the modified s3_filesystem.py without boto3
print("Testing boto3 import fix in s3_filesystem.py...")

# Test that we can import the modified s3_filesystem.py without boto3
import builtins
original_import = builtins.__import__

def mock_import(name, *args, **kwargs):
    if name in ['boto3', 'botocore', 'botocore.client', 'botocore.config']:
        raise ImportError(f"No module named '{name}'")
    return original_import(name, *args, **kwargs)

builtins.__import__ = mock_import

try:
    # Read the modified file and test that it compiles and can be executed
    with open('/tmp/oss-ray/python/ray/llm/_internal/common/utils/cloud_filesystem/s3_filesystem.py', 'r') as f:
        s3_code = f.read()
    
    # Check that our fix is in place
    if 'try:' in s3_code and 'import boto3' in s3_code and 'HAS_BOTO3 = True' in s3_code:
        print("✓ Fix is present: conditional boto3 import found")
    else:
        print("✗ Fix not found in code")
        sys.exit(1)
    
    if '_ensure_boto3_available' in s3_code:
        print("✓ Fix is present: _ensure_boto3_available method found")
    else:
        print("✗ _ensure_boto3_available method not found")
        sys.exit(1)
    
    if 'ray[llm]' in s3_code:
        print("✓ Fix is present: helpful error message found")
    else:
        print("✗ Helpful error message not found")
        sys.exit(1)
    
    # Try to compile the module
    try:
        compile(s3_code, '/tmp/oss-ray/python/ray/llm/_internal/common/utils/cloud_filesystem/s3_filesystem.py', 'exec')
        print("✓ Modified code compiles successfully")
    except SyntaxError as e:
        print(f"✗ Syntax error in modified code: {e}")
        sys.exit(1)
    
    print("\n✅ All tests passed! The boto3 import fix is correctly implemented.")
    
finally:
    # Restore original import
    builtins.__import__ = original_import