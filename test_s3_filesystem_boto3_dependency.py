#!/usr/bin/env python3
"""
Test suite for S3FileSystem boto3 dependency handling.

Tests that S3FileSystem can be imported without boto3 but fails gracefully
when S3 operations are attempted without the dependency.

This addresses issue #61269: ray[serve] imports fail due to missing boto3.
"""

import sys
import unittest
from unittest.mock import patch


class TestS3FileSystemBoto3Dependency(unittest.TestCase):
    """Test S3FileSystem boto3 dependency handling."""

    def setUp(self):
        """Clear any cached imports before each test."""
        modules_to_remove = [m for m in sys.modules.keys() 
                           if 's3_filesystem' in m]
        for mod in modules_to_remove:
            del sys.modules[mod]

    def test_import_without_boto3(self):
        """Test that S3FileSystem can be imported when boto3 is unavailable."""
        # Mock boto3/botocore as unavailable
        with patch.dict('sys.modules', {
            'boto3': None, 
            'botocore': None,
            'botocore.client': None,
            'botocore.config': None
        }):
            try:
                from ray.llm._internal.common.utils.cloud_filesystem.s3_filesystem import S3FileSystem
                # Should succeed without throwing ImportError for boto3
                self.assertTrue(True, "S3FileSystem imported successfully without boto3")
            except ImportError as e:
                if "boto3" in str(e) or "botocore" in str(e):
                    self.fail(f"S3FileSystem import failed due to boto3: {e}")
                else:
                    # Other import errors (like ray core) are acceptable for this test
                    self.skipTest(f"Skipping due to other import dependency: {e}")

    def test_s3_operations_fail_gracefully(self):
        """Test that S3 operations fail with helpful error when boto3 is missing."""
        with patch.dict('sys.modules', {
            'boto3': None, 
            'botocore': None,
            'botocore.client': None,
            'botocore.config': None
        }):
            try:
                from ray.llm._internal.common.utils.cloud_filesystem.s3_filesystem import S3FileSystem
            except ImportError as e:
                if "boto3" not in str(e) and "botocore" not in str(e):
                    self.skipTest(f"Skipping due to other import dependency: {e}")
                else:
                    self.fail(f"Import should succeed without boto3: {e}")
                    
            # Test each public method fails gracefully
            test_cases = [
                ('get_file', lambda: S3FileSystem.get_file("s3://test/file.txt")),
                ('list_subfolders', lambda: S3FileSystem.list_subfolders("s3://test/")),
                ('download_files', lambda: S3FileSystem.download_files("/tmp", "s3://test/")),
                ('upload_files', lambda: S3FileSystem.upload_files("/tmp", "s3://test/")),
            ]
            
            for method_name, method_call in test_cases:
                with self.subTest(method=method_name):
                    with self.assertRaises(ImportError) as cm:
                        method_call()
                    
                    error_msg = str(cm.exception)
                    self.assertIn("boto3", error_msg, 
                                f"{method_name} should mention boto3 in error")
                    self.assertIn("ray[llm]", error_msg, 
                                f"{method_name} should suggest ray[llm] installation")

    def test_has_boto3_flag_behavior(self):
        """Test that HAS_BOTO3 flag correctly reflects boto3 availability."""
        # Test when boto3 is unavailable
        with patch.dict('sys.modules', {
            'boto3': None, 
            'botocore': None,
            'botocore.client': None,
            'botocore.config': None
        }):
            try:
                from ray.llm._internal.common.utils.cloud_filesystem import s3_filesystem
                self.assertEqual(s3_filesystem.HAS_BOTO3, False, 
                               "HAS_BOTO3 should be False when boto3 is unavailable")
            except ImportError as e:
                if "boto3" not in str(e):
                    self.skipTest(f"Skipping due to other dependency: {e}")

    def test_error_message_content(self):
        """Test that error messages contain helpful installation instructions."""
        with patch.dict('sys.modules', {
            'boto3': None, 
            'botocore': None,
            'botocore.client': None,
            'botocore.config': None
        }):
            try:
                from ray.llm._internal.common.utils.cloud_filesystem.s3_filesystem import S3FileSystem
            except ImportError:
                self.skipTest("Cannot import S3FileSystem due to other dependencies")
                
            try:
                S3FileSystem.get_file("s3://test/file.txt")
                self.fail("get_file should have raised ImportError")
            except ImportError as e:
                error_msg = str(e)
                # Check for helpful installation instructions
                self.assertIn("boto3 is required", error_msg)
                self.assertIn("pip install ray[llm]", error_msg)
                self.assertIn("pip install boto3", error_msg)


if __name__ == '__main__':
    # Add the ray python directory to path for testing
    ray_python_path = "/tmp/oss-ray/python"
    if ray_python_path not in sys.path:
        sys.path.insert(0, ray_python_path)
    
    unittest.main(verbosity=2)