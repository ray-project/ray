import os
import pickle
import shutil
import tempfile
import unittest
from unittest.mock import patch

from ray.ml.checkpoint import Checkpoint


def mock_s3_sync(local_path):
    def mocked(cmd, *args, **kwargs):
        # The called command is e.g.
        # ["aws", "s3", "cp", "--recursive", "--quiet", local_path, bucket]
        source = cmd[5]
        target = cmd[6]

        checkpoint_path = os.path.join(local_path, "checkpoint")

        if source.startswith("s3://"):
            if os.path.exists(target):
                shutil.rmtree(target)
            shutil.copytree(checkpoint_path, target)
        else:
            if os.path.exists(checkpoint_path):
                shutil.rmtree(checkpoint_path)
            shutil.copytree(source, checkpoint_path)

    return mocked


class CheckpointsTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = os.path.realpath(tempfile.mkdtemp())

        self.checkpoint_dict_data = {"metric": 5, "step": 4}
        self.checkpoint_dir_data = {"metric": 2, "step": 6}

        self.cloud_uri = "s3://invalid"
        self.local_mock_cloud_path = os.path.realpath(tempfile.mkdtemp())
        self.mock_s3 = mock_s3_sync(self.local_mock_cloud_path)

        self.checkpoint_dir = os.path.join(self.tmpdir, "existing_checkpoint")
        os.mkdir(self.checkpoint_dir, 0o755)
        with open(os.path.join(self.checkpoint_dir, "test_data.pkl"), "wb") as fp:
            pickle.dump(self.checkpoint_dir_data, fp)

        os.chdir(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        shutil.rmtree(self.local_mock_cloud_path)

    def _prepare_dict_checkpoint(self) -> Checkpoint:
        # Create checkpoint from dict
        checkpoint = Checkpoint.from_dict(self.checkpoint_dict_data)
        self.assertIsInstance(checkpoint, Checkpoint)
        self.assertTrue(checkpoint._data_dict)
        self.assertEqual(
            checkpoint._data_dict["metric"], self.checkpoint_dict_data["metric"]
        )
        return checkpoint

    def _assert_dict_checkpoint(self, checkpoint):
        # Convert into dict
        checkpoint_data = checkpoint.to_dict()
        self.assertDictEqual(checkpoint_data, self.checkpoint_dict_data)

    def test_dict_checkpoint_bytes(self):
        """Test conversion from dict to bytes checkpoint and back."""
        checkpoint = self._prepare_dict_checkpoint()

        # Convert into bytes checkpoint
        blob = checkpoint.to_bytes()
        self.assertIsInstance(blob, bytes)

        # Create from bytes
        checkpoint = Checkpoint.from_bytes(blob)
        self.assertTrue(checkpoint._data_dict)

        self._assert_dict_checkpoint(checkpoint)

    def test_dict_checkpoint_dict(self):
        """Test conversion from dict to dict checkpoint and back."""
        checkpoint = self._prepare_dict_checkpoint()

        # Convert into dict checkpoint
        data_dict = checkpoint.to_dict()
        self.assertIsInstance(data_dict, dict)

        # Create from dict
        checkpoint = Checkpoint.from_dict(data_dict)
        self.assertTrue(checkpoint._data_dict)

        self._assert_dict_checkpoint(checkpoint)

    def test_dict_checkpoint_fs(self):
        """Test conversion from dict to FS checkpoint and back."""
        checkpoint = self._prepare_dict_checkpoint()

        # Convert into fs checkpoint
        path = checkpoint.to_directory()
        self.assertIsInstance(path, str)

        # Create from path
        checkpoint = Checkpoint.from_directory(path)
        self.assertTrue(checkpoint._local_path)

        self._assert_dict_checkpoint(checkpoint)

    def test_dict_checkpoint_obj_store(self):
        """Test conversion from fs to obj store checkpoint and back."""
        import ray

        if not ray.is_initialized():
            ray.init()

        checkpoint = self._prepare_dict_checkpoint()

        # Convert into dict checkpoint
        obj_ref = checkpoint.to_object_ref()
        self.assertIsInstance(obj_ref, ray.ObjectRef)

        # Create from dict
        checkpoint = Checkpoint.from_object_ref(obj_ref)
        self.assertTrue(checkpoint._obj_ref)

        self._assert_dict_checkpoint(checkpoint)

    def test_dict_checkpoint_uri(self):
        """Test conversion from dict to cloud checkpoint and back."""
        checkpoint = self._prepare_dict_checkpoint()

        with patch("subprocess.check_call", self.mock_s3):
            # Convert into dict checkpoint
            location = checkpoint.to_uri(self.cloud_uri)
            self.assertIsInstance(location, str)
            self.assertIn("s3://", location)

            # Create from dict
            checkpoint = Checkpoint.from_uri(location)
            self.assertTrue(checkpoint._uri)

            self._assert_dict_checkpoint(checkpoint)

    def _prepare_fs_checkpoint(self) -> Checkpoint:
        # Create checkpoint from fs
        checkpoint = Checkpoint.from_directory(self.checkpoint_dir)

        self.assertIsInstance(checkpoint, Checkpoint)
        self.assertTrue(checkpoint._local_path, str)
        self.assertEqual(checkpoint._local_path, self.checkpoint_dir)

        return checkpoint

    def _assert_fs_checkpoint(self, checkpoint):
        # Convert back to directory
        local_dir = checkpoint.to_directory()

        with open(os.path.join(local_dir, "test_data.pkl"), "rb") as fp:
            local_data = pickle.load(fp)

        self.assertDictEqual(local_data, self.checkpoint_dir_data)

        # Checkpoint should lie within current directory
        self.assertTrue(
            local_dir.startswith(self.tmpdir),
            msg=f"Checkpoint dir {local_dir} is not contained in {self.tmpdir}",
        )

    def test_fs_checkpoint_bytes(self):
        """Test conversion from fs to bytes checkpoint and back."""
        checkpoint = self._prepare_fs_checkpoint()

        # Convert into bytest checkpoint
        blob = checkpoint.to_bytes()
        self.assertIsInstance(blob, bytes)

        # Create from bytes
        checkpoint = Checkpoint.from_bytes(blob)
        self.assertTrue(checkpoint._data_dict)

        self._assert_fs_checkpoint(checkpoint)

    def test_fs_checkpoint_dict(self):
        """Test conversion from fs to dict checkpoint and back."""
        checkpoint = self._prepare_fs_checkpoint()

        # Convert into dict checkpoint
        data_dict = checkpoint.to_dict()
        self.assertIsInstance(data_dict, dict)

        # Create from dict
        checkpoint = Checkpoint.from_dict(data_dict)
        self.assertTrue(checkpoint._data_dict)

        self._assert_fs_checkpoint(checkpoint)

    def test_fs_checkpoint_fs(self):
        """Test conversion from fs to fs checkpoint and back."""
        checkpoint = self._prepare_fs_checkpoint()

        # Convert into fs checkpoint
        path = checkpoint.to_directory()
        self.assertIsInstance(path, str)

        # Create from fs
        checkpoint = Checkpoint.from_directory(path)
        self.assertTrue(checkpoint._local_path)

        self._assert_fs_checkpoint(checkpoint)

    def test_fs_checkpoint_obj_store(self):
        """Test conversion from fs to obj store checkpoint and back."""
        import ray

        if not ray.is_initialized():
            ray.init()

        checkpoint = self._prepare_fs_checkpoint()

        # Convert into obj ref checkpoint
        obj_ref = checkpoint.to_object_ref()

        # Create from object ref
        checkpoint = Checkpoint.from_object_ref(obj_ref)
        self.assertIsInstance(checkpoint._obj_ref, ray.ObjectRef)

        self._assert_fs_checkpoint(checkpoint)

    def test_fs_checkpoint_uri(self):
        """Test conversion from fs to cloud checkpoint and back."""
        checkpoint = self._prepare_fs_checkpoint()

        with patch("subprocess.check_call", self.mock_s3):
            # Convert into dict checkpoint
            location = checkpoint.to_uri(self.cloud_uri)
            self.assertIsInstance(location, str)
            self.assertIn("s3://", location)

            # Create from dict
            checkpoint = Checkpoint.from_uri(location)
            self.assertTrue(checkpoint._uri)

            self._assert_fs_checkpoint(checkpoint)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
