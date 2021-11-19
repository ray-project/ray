import tempfile
import unittest

import shutil
import sys
from unittest.mock import patch

from ray.tune.cloud import TrialCheckpoint


class TrialCheckpointTest(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp()
        self.cloud_dir = "s3://invalid"

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir)

    def testDownloadNoDefaults(self):
        state = {}

        def check_call(cmd, *args, **kwargs):
            state["cmd"] = cmd

        # Case: Nothing is passed
        checkpoint = TrialCheckpoint()
        with self.assertRaises(RuntimeError):
            checkpoint.download()

        # Case: Local dir is passed
        checkpoint = TrialCheckpoint()
        with self.assertRaisesRegex(RuntimeError, "No cloud path"):
            checkpoint.download(local_path=self.test_dir)

        # Case: Cloud dir is passed
        checkpoint = TrialCheckpoint()
        with self.assertRaisesRegex(RuntimeError, "No local path"):
            checkpoint.download(cloud_path=self.cloud_dir)

        # Case: Both are passed
        checkpoint = TrialCheckpoint()
        with patch("subprocess.check_call", check_call):
            path = checkpoint.download(
                local_path=self.test_dir, cloud_path=self.cloud_dir)

        self.assertEquals(self.test_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(self.test_dir, state["cmd"])

    def testDownloadDefaultLocal(self):
        state = {}

        def check_call(cmd, *args, **kwargs):
            state["cmd"] = cmd

        other_local_dir = "/tmp/invalid"

        # Case: Nothing is passed
        checkpoint = TrialCheckpoint(local_path=self.test_dir)
        with self.assertRaisesRegex(RuntimeError, "No cloud path"):
            checkpoint.download()

        # Case: Local dir is passed
        checkpoint = TrialCheckpoint(local_path=self.test_dir)
        with self.assertRaisesRegex(RuntimeError, "No cloud path"):
            checkpoint.download(local_path=other_local_dir)

        # Case: Cloud dir is passed
        checkpoint = TrialCheckpoint(local_path=self.test_dir)
        with patch("subprocess.check_call", check_call):
            path = checkpoint.download(cloud_path=self.cloud_dir)

        self.assertEquals(self.test_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(self.test_dir, state["cmd"])

        # Case: Both are passed
        checkpoint = TrialCheckpoint(local_path=self.test_dir)
        with patch("subprocess.check_call", check_call):
            path = checkpoint.download(
                local_path=other_local_dir, cloud_path=self.cloud_dir)

        self.assertEquals(other_local_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(other_local_dir, state["cmd"])
        self.assertNotIn(self.test_dir, state["cmd"])

    def testDownloadDefaultCloud(self):
        state = {}

        def check_call(cmd, *args, **kwargs):
            state["cmd"] = cmd

        other_cloud_dir = "s3://other"

        # Case: Nothing is passed
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)
        with self.assertRaisesRegex(RuntimeError, "No local path"):
            checkpoint.download()

        # Case: Local dir is passed
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)
        with patch("subprocess.check_call", check_call):
            path = checkpoint.download(local_path=self.test_dir)

        self.assertEquals(self.test_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(self.test_dir, state["cmd"])

        # Case: Cloud dir is passed
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)
        with self.assertRaisesRegex(RuntimeError, "No local path"):
            checkpoint.download(cloud_path=other_cloud_dir)

        # Case: Both are passed
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)
        with patch("subprocess.check_call", check_call):
            path = checkpoint.download(
                local_path=self.test_dir, cloud_path=other_cloud_dir)

        self.assertEquals(self.test_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(other_cloud_dir, state["cmd"])
        self.assertNotIn(self.cloud_dir, state["cmd"])

    def testDownloadDefaultBoth(self):
        state = {}

        def check_call(cmd, *args, **kwargs):
            state["cmd"] = cmd

        other_local_dir = "/tmp/other"
        other_cloud_dir = "s3://other"

        # Case: Nothing is passed
        checkpoint = TrialCheckpoint(
            local_path=self.test_dir, cloud_path=self.cloud_dir)

        with patch("subprocess.check_call", check_call):
            path = checkpoint.download()

        self.assertEquals(self.test_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(self.test_dir, state["cmd"])

        # Case: Local dir is passed
        checkpoint = TrialCheckpoint(
            local_path=self.test_dir, cloud_path=self.cloud_dir)

        with patch("subprocess.check_call", check_call):
            path = checkpoint.download(local_path=other_local_dir)

        self.assertEquals(other_local_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(other_local_dir, state["cmd"])
        self.assertNotIn(self.test_dir, state["cmd"])

        # Case: Both are passed
        checkpoint = TrialCheckpoint(
            local_path=self.test_dir, cloud_path=self.cloud_dir)

        with patch("subprocess.check_call", check_call):

            path = checkpoint.download(
                local_path=other_local_dir, cloud_path=other_cloud_dir)

        self.assertEquals(other_local_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(other_local_dir, state["cmd"])
        self.assertNotIn(self.test_dir, state["cmd"])
        self.assertIn(other_cloud_dir, state["cmd"])
        self.assertNotIn(self.cloud_dir, state["cmd"])


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
