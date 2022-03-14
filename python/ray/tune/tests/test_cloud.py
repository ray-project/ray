import json
import os
import tempfile
import unittest

import shutil
import sys
from unittest.mock import patch

from ray import tune
from ray.tune.cloud import TrialCheckpoint


class TrialCheckpointApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.local_dir = tempfile.mkdtemp()
        self.cloud_dir = "s3://invalid"

    def tearDown(self) -> None:
        shutil.rmtree(self.local_dir)

    def testConstructTrialCheckpoint(self):
        # All these constructions should work
        TrialCheckpoint(None, None)
        TrialCheckpoint("/tmp", None)
        TrialCheckpoint(None, "s3://invalid")
        TrialCheckpoint("/remote/node/dir", None)

    def ensureCheckpointFile(self):
        with open(os.path.join(self.local_dir, "checkpoint.txt"), "wt") as f:
            f.write("checkpoint\n")

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
                local_path=self.local_dir, cloud_path=self.cloud_dir
            )

        self.assertEqual(self.local_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
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

        self.assertEqual(self.local_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
        self.assertIn(self.local_dir, state["cmd"])

        # Case: Both are passed
        checkpoint = TrialCheckpoint(local_path=self.local_dir)
        with patch("subprocess.check_call", check_call):
            path = checkpoint.download(
                local_path=other_local_dir, cloud_path=self.cloud_dir
            )

        self.assertEqual(other_local_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
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

        self.assertEqual(self.local_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
        self.assertIn(self.local_dir, state["cmd"])

        # Case: Cloud dir is passed
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)
        with self.assertRaisesRegex(RuntimeError, "No local path"):
            checkpoint.download(cloud_path=other_cloud_dir)

        # Case: Both are passed
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)
        with patch("subprocess.check_call", check_call):
            path = checkpoint.download(
                local_path=self.local_dir, cloud_path=other_cloud_dir
            )

        self.assertEqual(self.local_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
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
            local_path=self.local_dir, cloud_path=self.cloud_dir
        )

        with patch("subprocess.check_call", check_call):
            path = checkpoint.download()

        self.assertEqual(self.local_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
        self.assertIn(self.local_dir, state["cmd"])

        # Case: Local dir is passed
        checkpoint = TrialCheckpoint(
            local_path=self.local_dir, cloud_path=self.cloud_dir
        )

        with patch("subprocess.check_call", check_call):
            path = checkpoint.download(local_path=other_local_dir)

        self.assertEqual(other_local_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
        self.assertIn(other_local_dir, state["cmd"])
        self.assertNotIn(self.local_dir, state["cmd"])

        # Case: Both are passed
        checkpoint = TrialCheckpoint(
            local_path=self.local_dir, cloud_path=self.cloud_dir
        )

        with patch("subprocess.check_call", check_call):

            path = checkpoint.download(
                local_path=other_local_dir, cloud_path=other_cloud_dir
            )

        self.assertEqual(other_local_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
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
                local_path=self.local_dir, cloud_path=self.cloud_dir
            )

        self.assertEqual(self.cloud_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
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

        self.assertEqual(self.cloud_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
        self.assertIn(self.cloud_dir, state["cmd"])

        # Case: Both are passed
        checkpoint = TrialCheckpoint(local_path=self.local_dir)
        with patch("subprocess.check_call", check_call):
            path = checkpoint.upload(
                local_path=other_local_dir, cloud_path=self.cloud_dir
            )

        self.assertEqual(self.cloud_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
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

        self.assertEqual(self.cloud_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
        self.assertIn(self.cloud_dir, state["cmd"])

        # Case: Cloud dir is passed
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)
        with self.assertRaisesRegex(RuntimeError, "No local path"):
            checkpoint.upload(cloud_path=other_cloud_dir)

        # Case: Both are passed
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)
        with patch("subprocess.check_call", check_call):
            path = checkpoint.upload(
                local_path=self.local_dir, cloud_path=other_cloud_dir
            )

        self.assertEqual(other_cloud_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
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
            local_path=self.local_dir, cloud_path=self.cloud_dir
        )

        with patch("subprocess.check_call", check_call):
            path = checkpoint.upload()

        self.assertEqual(self.cloud_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
        self.assertIn(self.cloud_dir, state["cmd"])

        # Case: Local dir is passed
        checkpoint = TrialCheckpoint(
            local_path=self.local_dir, cloud_path=self.cloud_dir
        )

        with patch("subprocess.check_call", check_call):
            path = checkpoint.upload(local_path=other_local_dir)

        self.assertEqual(self.cloud_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
        self.assertIn(other_local_dir, state["cmd"])
        self.assertNotIn(self.local_dir, state["cmd"])

        # Case: Both are passed
        checkpoint = TrialCheckpoint(
            local_path=self.local_dir, cloud_path=self.cloud_dir
        )

        with patch("subprocess.check_call", check_call):

            path = checkpoint.upload(
                local_path=other_local_dir, cloud_path=other_cloud_dir
            )

        self.assertEqual(other_cloud_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
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
            path = checkpoint.save(self.local_dir, force_download=True)

        self.assertEqual(self.local_dir, path)
        self.assertEqual(state["cmd"][0], "aws")
        self.assertIn(self.cloud_dir, state["cmd"])
        self.assertIn(self.local_dir, state["cmd"])

        # Case: Default local dir, pass local dir
        checkpoint = TrialCheckpoint(local_path=self.local_dir)
        self.ensureCheckpointFile()

        with patch("shutil.copytree", copytree):
            path = checkpoint.save(other_local_dir)

        self.assertEqual(other_local_dir, path)
        self.assertEqual(state["copy_source"], self.local_dir)
        self.assertEqual(state["copy_dest"], other_local_dir)

        # Case: Both default, no pass
        checkpoint = TrialCheckpoint(
            local_path=self.local_dir, cloud_path=self.cloud_dir
        )

        with patch("subprocess.check_call", check_call):
            path = checkpoint.save()

        self.assertEqual(self.local_dir, path)
        self.assertIn(self.cloud_dir, state["cmd"])
        self.assertIn(self.local_dir, state["cmd"])

        # Case: Both default, pass other local dir
        checkpoint = TrialCheckpoint(
            local_path=self.local_dir, cloud_path=self.cloud_dir
        )

        with patch("shutil.copytree", copytree):
            path = checkpoint.save(other_local_dir)

        self.assertEqual(other_local_dir, path)
        self.assertEqual(state["copy_source"], self.local_dir)
        self.assertEqual(state["copy_dest"], other_local_dir)
        self.assertEqual(checkpoint.local_path, self.local_dir)

    def testSaveCloudTarget(self):
        state = {}

        def check_call(cmd, *args, **kwargs):
            state["cmd"] = cmd

            # Fake AWS-specific checkpoint download
            local_dir = cmd[6]
            if not local_dir.startswith("s3"):
                with open(os.path.join(local_dir, "checkpoint.txt"), "wt") as f:
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

        self.assertEqual(self.cloud_dir, path)
        self.assertIn(self.cloud_dir, state["cmd"])
        self.assertIn(self.local_dir, state["cmd"])

        # Clean up checkpoint
        os.remove(os.path.join(self.local_dir, "checkpoint.txt"))

        # Case: Default cloud dir, copy to other cloud
        checkpoint = TrialCheckpoint(cloud_path=self.cloud_dir)

        with patch("subprocess.check_call", check_call):
            path = checkpoint.save(other_cloud_dir)

        self.assertEqual(other_cloud_dir, path)
        self.assertIn(other_cloud_dir, state["cmd"])
        self.assertNotIn(self.local_dir, state["cmd"])  # Temp dir

        # Case: Default both, copy to other cloud
        checkpoint = TrialCheckpoint(
            local_path=self.local_dir, cloud_path=self.cloud_dir
        )

        with patch("subprocess.check_call", check_call):
            path = checkpoint.save(other_cloud_dir)

        self.assertEqual(other_cloud_dir, path)
        self.assertIn(other_cloud_dir, state["cmd"])
        self.assertIn(self.local_dir, state["cmd"])


def train(config, checkpoint_dir=None):
    for i in range(10):
        with tune.checkpoint_dir(step=0) as cd:
            with open(os.path.join(cd, "checkpoint.json"), "wt") as f:
                json.dump({"score": i, "train_id": config["train_id"]}, f)
        tune.report(score=i)


class TrialCheckpointEndToEndTest(unittest.TestCase):
    def setUp(self) -> None:
        self.local_experiment_dir = tempfile.mkdtemp()

        self.fake_cloud_dir = tempfile.mkdtemp()
        self.cloud_target = "s3://invalid/sub/path"

        self.second_fake_cloud_dir = tempfile.mkdtemp()
        self.second_cloud_target = "gs://other/cloud"

    def tearDown(self) -> None:
        shutil.rmtree(self.local_experiment_dir)
        shutil.rmtree(self.fake_cloud_dir)
        shutil.rmtree(self.second_fake_cloud_dir)

    def _clear_bucket(self, bucket: str):
        cloud_local_dir = bucket.replace(self.cloud_target, self.fake_cloud_dir)
        cloud_local_dir = cloud_local_dir.replace(
            self.second_cloud_target, self.second_fake_cloud_dir
        )
        shutil.rmtree(cloud_local_dir)

    def _fake_download_from_bucket(self, bucket: str, local_path: str):
        cloud_local_dir = bucket.replace(self.cloud_target, self.fake_cloud_dir)
        cloud_local_dir = cloud_local_dir.replace(
            self.second_cloud_target, self.second_fake_cloud_dir
        )

        shutil.rmtree(local_path, ignore_errors=True)
        shutil.copytree(cloud_local_dir, local_path)

    def _fake_upload_to_bucket(self, bucket: str, local_path: str):
        cloud_local_dir = bucket.replace(self.cloud_target, self.fake_cloud_dir)
        cloud_local_dir = cloud_local_dir.replace(
            self.second_cloud_target, self.second_fake_cloud_dir
        )
        shutil.rmtree(cloud_local_dir, ignore_errors=True)
        shutil.copytree(local_path, cloud_local_dir)

    def testCheckpointDownload(self):
        analysis = tune.run(
            train,
            config={"train_id": tune.grid_search([0, 1, 2, 3, 4])},
            local_dir=self.local_experiment_dir,
            verbose=2,
        )

        # Inject the sync config (this is usually done by `tune.run()`)
        analysis._sync_config = tune.SyncConfig(upload_dir=self.cloud_target)

        # Pretend we have all checkpoints on cloud storage (durable)
        shutil.rmtree(self.fake_cloud_dir, ignore_errors=True)
        shutil.copytree(self.local_experiment_dir, self.fake_cloud_dir)

        # Pretend we don't have these on local storage
        shutil.rmtree(analysis.trials[1].logdir)
        shutil.rmtree(analysis.trials[2].logdir)
        shutil.rmtree(analysis.trials[3].logdir)
        shutil.rmtree(analysis.trials[4].logdir)

        cp0 = analysis.get_best_checkpoint(analysis.trials[0], "score", "max")
        cp1 = analysis.get_best_checkpoint(analysis.trials[1], "score", "max")
        cp2 = analysis.get_best_checkpoint(analysis.trials[2], "score", "max")
        cp3 = analysis.get_best_checkpoint(analysis.trials[3], "score", "max")
        cp4 = analysis.get_best_checkpoint(analysis.trials[4], "score", "max")

        def _load_cp(cd):
            with open(os.path.join(cd, "checkpoint.json"), "rt") as f:
                return json.load(f)

        with patch("ray.tune.cloud.clear_bucket", self._clear_bucket), patch(
            "ray.tune.cloud.download_from_bucket", self._fake_download_from_bucket
        ), patch(
            "ray.ml.checkpoint.download_from_bucket", self._fake_download_from_bucket
        ), patch(
            "ray.tune.cloud.upload_to_bucket", self._fake_upload_to_bucket
        ):
            #######
            # Case: Checkpoint exists on local dir. Copy to other local dir.
            other_local_dir = tempfile.mkdtemp()

            cp0.save(other_local_dir)

            self.assertTrue(os.path.exists(cp0.local_path))

            cp_content = _load_cp(other_local_dir)
            self.assertEqual(cp_content["train_id"], 0)
            self.assertEqual(cp_content["score"], 9)

            cp_content_2 = _load_cp(cp0.local_path)
            self.assertEqual(cp_content, cp_content_2)

            # Clean up
            shutil.rmtree(other_local_dir)

            #######
            # Case: Checkpoint does not exist on local dir, download from cloud
            # store in experiment dir.

            # Directory is empty / does not exist before
            self.assertFalse(os.path.exists(cp1.local_path))

            # Save!
            cp1.save()

            # Directory is not empty anymore
            self.assertTrue(os.listdir(cp1.local_path))
            cp_content = _load_cp(cp1.local_path)
            self.assertEqual(cp_content["train_id"], 1)
            self.assertEqual(cp_content["score"], 9)

            #######
            # Case: Checkpoint does not exist on local dir, download from cloud
            # store into other local dir.

            # Directory is empty / does not exist before
            self.assertFalse(os.path.exists(cp2.local_path))

            other_local_dir = tempfile.mkdtemp()
            # Save!
            cp2.save(other_local_dir)

            # Directory still does not exist (as we save to other dir)
            self.assertFalse(os.path.exists(cp2.local_path))
            cp_content = _load_cp(other_local_dir)
            self.assertEqual(cp_content["train_id"], 2)
            self.assertEqual(cp_content["score"], 9)

            # Clean up
            shutil.rmtree(other_local_dir)

            #######
            # Case: Checkpoint does not exist on local dir, download from cloud
            # and store onto other cloud.

            # Local dir does not exist
            self.assertFalse(os.path.exists(cp3.local_path))
            # First cloud exists
            self.assertTrue(os.listdir(self.fake_cloud_dir))
            # Second cloud does not exist
            self.assertFalse(os.listdir(self.second_fake_cloud_dir))

            # Trigger save
            cp3.save(self.second_cloud_target)

            # Local dir now exists
            self.assertTrue(os.path.exists(cp3.local_path))
            # First cloud exists
            self.assertTrue(os.listdir(self.fake_cloud_dir))
            # Second cloud now exists!
            self.assertTrue(os.listdir(self.second_fake_cloud_dir))

            cp_content = _load_cp(self.second_fake_cloud_dir)
            self.assertEqual(cp_content["train_id"], 3)
            self.assertEqual(cp_content["score"], 9)

            #######
            # Case: Checkpoint does not exist on local dir, download from cloud
            # store into local dir. Use new checkpoint abstractions for this.

            temp_dir = cp4.to_directory(tempfile.mkdtemp())
            cp_content = _load_cp(temp_dir)
            self.assertEqual(cp_content["train_id"], 4)
            self.assertEqual(cp_content["score"], 9)

            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
