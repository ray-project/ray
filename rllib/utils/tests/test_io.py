import unittest

import ray
from ray.rllib.utils.io import is_remote_path


class TestIO(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_is_remote_path(self):
        # Test S3 remote path
        s3_path = "s3://my-bucket/my-object"
        self.assertTrue(is_remote_path(s3_path))

        # Test GCS remote path
        gcs_path = "gs://my-bucket/my-object"
        self.assertTrue(is_remote_path(gcs_path))

        # Test local path
        local_path = "/home/user/my-file.txt"
        self.assertFalse(is_remote_path(local_path))

        # Test local path with no leading slash
        local_path_no_slash = "home/user/my-file.txt"
        self.assertFalse(is_remote_path(local_path_no_slash))

        # Test local path with special characters
        local_path_special_chars = "/home/user/my-folder/my file (1).txt"
        self.assertFalse(is_remote_path(local_path_special_chars))

if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
