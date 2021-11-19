import os
import tempfile
import unittest

import shutil
import sys
from unittest.mock import patch

from ray.tune.cloud import TrialCheckpoint


class TrialCheckpointTest(unittest.TestCase):
    def setUp(self) -> None:
        self.local_dir = tempfile.mkdtemp()
        self.cloud_dir = "s3://invalid"

    def tearDown(self) -> None:
        shutil.rmtree(self.local_dir)

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
            checkpoint.download(local_path=self.local_dir)

        # Case: Cloud dir is passed
        checkpoint = TrialCheckpoint()
        with self.assertRaisesRegex(RuntimeError, "No local path"):
            checkpoint.download(cloud_path=self.cloud_dir)

        # Case: Both are passed
        checkpoint = TrialCheckpoint()
        with patch("subprocess.check_call", check_call):
            path = checkpoint.download(
                local_path=self.local_dir, cloud_path=self.cloud_dir)

        self.assertEquals(self.local_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(self.local_dir, state["cmd"])

    def testDownloadDefaultLocal(self):
        state = {}

        def check_call(cmd, *args, **kwargs):
            state["cmd"] = cmd

        other_local_dir = "/tmp/invalid"

        # Case: Nothing is passed
        checkpoint = TrialCheckpoint(local_path=self.local_dir)
        with self.assertRaisesRegex(RuntimeError, "No cloud path"):
            checkpoint.download()

        # Case: Local dir is passed
        checkpoint = TrialCheckpoint(local_path=self.local_dir)
        with self.assertRaisesRegex(RuntimeError, "No cloud path"):
            checkpoint.download(local_path=other_local_dir)

        # Case: Cloud dir is passed
        checkpoint = TrialCheckpoint(local_path=self.local_dir)
        with patch("subprocess.check_call", check_call):
            path = checkpoint.download(cloud_path=self.cloud_dir)

        self.assertEquals(self.local_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(self.local_dir, state["cmd"])

        # Case: Both are passed
        checkpoint = TrialCheckpoint(local_path=self.local_dir)
        with patch("subprocess.check_call", check_call):
            path = checkpoint.download(
                local_path=other_local_dir, cloud_path=self.cloud_dir)

        self.assertEquals(other_local_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(other_local_dir, state["cmd"])
        self.assertNotIn(self.local_dir, state["cmd"])

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
            path = checkpoint.download(local_path=self.local_dir)

        self.assertEquals(self.local_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(self.local_dir, state["cmd"])

        # Case: Cloud dir is passed
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)
        with self.assertRaisesRegex(RuntimeError, "No local path"):
            checkpoint.download(cloud_path=other_cloud_dir)

        # Case: Both are passed
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)
        with patch("subprocess.check_call", check_call):
            path = checkpoint.download(
                local_path=self.local_dir, cloud_path=other_cloud_dir)

        self.assertEquals(self.local_dir, path)
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
            local_path=self.local_dir, cloud_path=self.cloud_dir)

        with patch("subprocess.check_call", check_call):
            path = checkpoint.download()

        self.assertEquals(self.local_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(self.local_dir, state["cmd"])

        # Case: Local dir is passed
        checkpoint = TrialCheckpoint(
            local_path=self.local_dir, cloud_path=self.cloud_dir)

        with patch("subprocess.check_call", check_call):
            path = checkpoint.download(local_path=other_local_dir)

        self.assertEquals(other_local_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(other_local_dir, state["cmd"])
        self.assertNotIn(self.local_dir, state["cmd"])

        # Case: Both are passed
        checkpoint = TrialCheckpoint(
            local_path=self.local_dir, cloud_path=self.cloud_dir)

        with patch("subprocess.check_call", check_call):

            path = checkpoint.download(
                local_path=other_local_dir, cloud_path=other_cloud_dir)

        self.assertEquals(other_local_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(other_local_dir, state["cmd"])
        self.assertNotIn(self.local_dir, state["cmd"])
        self.assertIn(other_cloud_dir, state["cmd"])
        self.assertNotIn(self.cloud_dir, state["cmd"])

    def testUploadNoDefaults(self):
        state = {}

        def check_call(cmd, *args, **kwargs):
            state["cmd"] = cmd

        # Case: Nothing is passed
        checkpoint = TrialCheckpoint()
        with self.assertRaises(RuntimeError):
            checkpoint.upload()

        # Case: Local dir is passed
        checkpoint = TrialCheckpoint()
        with self.assertRaisesRegex(RuntimeError, "No cloud path"):
            checkpoint.upload(local_path=self.local_dir)

        # Case: Cloud dir is passed
        checkpoint = TrialCheckpoint()
        with self.assertRaisesRegex(RuntimeError, "No local path"):
            checkpoint.upload(cloud_path=self.cloud_dir)

        # Case: Both are passed
        checkpoint = TrialCheckpoint()
        with patch("subprocess.check_call", check_call):
            path = checkpoint.upload(
                local_path=self.local_dir, cloud_path=self.cloud_dir)

        self.assertEquals(self.cloud_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(self.cloud_dir, state["cmd"])

    def testUploadDefaultLocal(self):
        state = {}

        def check_call(cmd, *args, **kwargs):
            state["cmd"] = cmd

        other_local_dir = "/tmp/invalid"

        # Case: Nothing is passed
        checkpoint = TrialCheckpoint(local_path=self.local_dir)
        with self.assertRaisesRegex(RuntimeError, "No cloud path"):
            checkpoint.upload()

        # Case: Local dir is passed
        checkpoint = TrialCheckpoint(local_path=self.local_dir)
        with self.assertRaisesRegex(RuntimeError, "No cloud path"):
            checkpoint.upload(local_path=other_local_dir)

        # Case: Cloud dir is passed
        checkpoint = TrialCheckpoint(local_path=self.local_dir)
        with patch("subprocess.check_call", check_call):
            path = checkpoint.upload(cloud_path=self.cloud_dir)

        self.assertEquals(self.cloud_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(self.cloud_dir, state["cmd"])

        # Case: Both are passed
        checkpoint = TrialCheckpoint(local_path=self.local_dir)
        with patch("subprocess.check_call", check_call):
            path = checkpoint.upload(
                local_path=other_local_dir, cloud_path=self.cloud_dir)

        self.assertEquals(self.cloud_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(other_local_dir, state["cmd"])
        self.assertNotIn(self.local_dir, state["cmd"])

    def testUploadDefaultCloud(self):
        state = {}

        def check_call(cmd, *args, **kwargs):
            state["cmd"] = cmd

        other_cloud_dir = "s3://other"

        # Case: Nothing is passed
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)
        with self.assertRaisesRegex(RuntimeError, "No local path"):
            checkpoint.upload()

        # Case: Local dir is passed
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)
        with patch("subprocess.check_call", check_call):
            path = checkpoint.upload(local_path=self.local_dir)

        self.assertEquals(self.cloud_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(self.cloud_dir, state["cmd"])

        # Case: Cloud dir is passed
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)
        with self.assertRaisesRegex(RuntimeError, "No local path"):
            checkpoint.upload(cloud_path=other_cloud_dir)

        # Case: Both are passed
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)
        with patch("subprocess.check_call", check_call):
            path = checkpoint.upload(
                local_path=self.local_dir, cloud_path=other_cloud_dir)

        self.assertEquals(other_cloud_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(other_cloud_dir, state["cmd"])
        self.assertNotIn(self.cloud_dir, state["cmd"])

    def testUploadDefaultBoth(self):
        state = {}

        def check_call(cmd, *args, **kwargs):
            state["cmd"] = cmd

        other_local_dir = "/tmp/other"
        other_cloud_dir = "s3://other"

        # Case: Nothing is passed
        checkpoint = TrialCheckpoint(
            local_path=self.local_dir, cloud_path=self.cloud_dir)

        with patch("subprocess.check_call", check_call):
            path = checkpoint.upload()

        self.assertEquals(self.cloud_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(self.cloud_dir, state["cmd"])

        # Case: Local dir is passed
        checkpoint = TrialCheckpoint(
            local_path=self.local_dir, cloud_path=self.cloud_dir)

        with patch("subprocess.check_call", check_call):
            path = checkpoint.upload(local_path=other_local_dir)

        self.assertEquals(self.cloud_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(other_local_dir, state["cmd"])
        self.assertNotIn(self.local_dir, state["cmd"])

        # Case: Both are passed
        checkpoint = TrialCheckpoint(
            local_path=self.local_dir, cloud_path=self.cloud_dir)

        with patch("subprocess.check_call", check_call):

            path = checkpoint.upload(
                local_path=other_local_dir, cloud_path=other_cloud_dir)

        self.assertEquals(other_cloud_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(other_local_dir, state["cmd"])
        self.assertNotIn(self.local_dir, state["cmd"])
        self.assertIn(other_cloud_dir, state["cmd"])
        self.assertNotIn(self.cloud_dir, state["cmd"])

    def testSaveLocalTarget(self):
        state = {}

        def check_call(cmd, *args, **kwargs):
            state["cmd"] = cmd

        def copytree(source, dest):
            state["copy_source"] = source
            state["copy_dest"] = dest

        other_local_dir = "/tmp/other"

        # Case: No defaults
        checkpoint = TrialCheckpoint()
        with self.assertRaisesRegex(RuntimeError, "No cloud path"):
            checkpoint.save()

        # Case: Default local dir
        checkpoint = TrialCheckpoint(local_path=self.local_dir)

        with self.assertRaisesRegex(RuntimeError, "No cloud path"):
            checkpoint.save()

        # Case: Default cloud dir, no local dir passed
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)

        with self.assertRaisesRegex(RuntimeError, "No target path"):
            checkpoint.save()

        # Case: Default cloud dir, pass local dir
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)

        with patch("subprocess.check_call", check_call):
            path = checkpoint.save(self.local_dir)

        self.assertEquals(self.local_dir, path)
        self.assertEquals(state["cmd"][0], "aws")
        self.assertIn(self.cloud_dir, state["cmd"])
        self.assertIn(self.local_dir, state["cmd"])

        # Case: Default local dir, pass local dir
        checkpoint = TrialCheckpoint(local_path=self.local_dir)

        with patch("shutil.copytree", copytree):
            path = checkpoint.save(other_local_dir)

        self.assertEquals(other_local_dir, path)
        self.assertEquals(state["copy_source"], self.local_dir)
        self.assertEquals(state["copy_dest"], other_local_dir)

        # Case: Both default, no pass
        checkpoint = TrialCheckpoint(
            local_path=self.local_dir, cloud_path=self.cloud_dir)

        with patch("subprocess.check_call", check_call):
            path = checkpoint.save()

        self.assertEquals(self.local_dir, path)
        self.assertIn(self.cloud_dir, state["cmd"])
        self.assertIn(self.local_dir, state["cmd"])

        # Case: Both default, pass other local dir
        checkpoint = TrialCheckpoint(
            local_path=self.local_dir, cloud_path=self.cloud_dir)

        with patch("shutil.copytree", copytree):
            path = checkpoint.save(other_local_dir)

        self.assertEquals(other_local_dir, path)
        self.assertEquals(state["copy_source"], self.local_dir)
        self.assertEquals(state["copy_dest"], other_local_dir)
        self.assertEquals(checkpoint.local_path, self.local_dir)

    def testSaveCloudTarget(self):
        state = {}

        def check_call(cmd, *args, **kwargs):
            state["cmd"] = cmd

            # Fake AWS-specific checkpoint download
            local_dir = cmd[6]
            if not local_dir.startswith("s3"):
                with open(os.path.join(local_dir, "checkpoint.txt"),
                          "wt") as f:
                    f.write("Checkpoint\n")

        other_cloud_dir = "s3://other"

        # Case: No defaults
        checkpoint = TrialCheckpoint()
        with self.assertRaisesRegex(RuntimeError, "No existing local"):
            checkpoint.save(self.cloud_dir)

        # Case: Default local dir
        # Write a checkpoint here as we assume existing local dir
        with open(os.path.join(self.local_dir, "checkpoint.txt"), "wt") as f:
            f.write("Checkpoint\n")

        checkpoint = TrialCheckpoint(local_path=self.local_dir)
        with patch("subprocess.check_call", check_call):
            path = checkpoint.save(self.cloud_dir)

        self.assertEquals(self.cloud_dir, path)
        self.assertIn(self.cloud_dir, state["cmd"])
        self.assertIn(self.local_dir, state["cmd"])

        # Clean up checkpoint
        os.remove(os.path.join(self.local_dir, "checkpoint.txt"))

        # Case: Default cloud dir, copy to other cloud
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)

        with patch("subprocess.check_call", check_call):
            path = checkpoint.save(other_cloud_dir)

        self.assertEquals(other_cloud_dir, path)
        self.assertIn(other_cloud_dir, state["cmd"])
        self.assertNotIn(self.local_dir, state["cmd"])  # Temp dir

        # Case: Default both, copy to other cloud
        checkpoint = TrialCheckpoint(
            local_path=self.local_dir, cloud_path=self.cloud_dir)

        with patch("subprocess.check_call", check_call):
            path = checkpoint.save(other_cloud_dir)

        self.assertEquals(other_cloud_dir, path)
        self.assertIn(other_cloud_dir, state["cmd"])
        self.assertIn(self.local_dir, state["cmd"])


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
