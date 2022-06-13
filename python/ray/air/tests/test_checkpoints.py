import os
import pickle
import shutil
import tempfile
import unittest
from typing import Any

import ray
from ray.air.checkpoint import Checkpoint
from ray.air._internal.remote_storage import delete_at_uri, _ensure_directory


class CheckpointsConversionTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = os.path.realpath(tempfile.mkdtemp())
        self.tmpdir_pa = os.path.realpath(tempfile.mkdtemp())

        self.checkpoint_dict_data = {"metric": 5, "step": 4}
        self.checkpoint_dir_data = {"metric": 2, "step": 6}

        # We test two different in-memory filesystems as "cloud" providers,
        # one for fsspec and one for pyarrow.fs

        # fsspec URI
        self.cloud_uri = "memory:///cloud/bucket"
        # pyarrow URI
        self.cloud_uri_pa = "mock://cloud/bucket/"

        self.checkpoint_dir = os.path.join(self.tmpdir, "existing_checkpoint")
        os.mkdir(self.checkpoint_dir, 0o755)
        with open(os.path.join(self.checkpoint_dir, "test_data.pkl"), "wb") as fp:
            pickle.dump(self.checkpoint_dir_data, fp)

        self.old_dir = os.getcwd()
        os.chdir(self.tmpdir)

    def tearDown(self):
        os.chdir(self.old_dir)
        shutil.rmtree(self.tmpdir)
        shutil.rmtree(self.tmpdir_pa)

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

        # Convert into dict checkpoint
        location = checkpoint.to_uri(self.cloud_uri)
        self.assertIsInstance(location, str)
        self.assertIn("memory://", location)

        # Create from dict
        checkpoint = Checkpoint.from_uri(location)
        self.assertTrue(checkpoint._uri)

        self._assert_dict_checkpoint(checkpoint)

    def test_dict_checkpoint_uri_pa(self):
        """Test conversion from dict to cloud checkpoint and back."""
        checkpoint = self._prepare_dict_checkpoint()

        # Clean up mock bucket
        delete_at_uri(self.cloud_uri_pa)
        _ensure_directory(self.cloud_uri_pa)

        # Convert into dict checkpoint
        location = checkpoint.to_uri(self.cloud_uri_pa)
        self.assertIsInstance(location, str)
        self.assertIn("mock://", location)

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

        # Convert into dict checkpoint
        location = checkpoint.to_uri(self.cloud_uri)
        self.assertIsInstance(location, str)
        self.assertIn("memory://", location)

        # Create from dict
        checkpoint = Checkpoint.from_uri(location)
        self.assertTrue(checkpoint._uri)

        self._assert_fs_checkpoint(checkpoint)

    def test_fs_checkpoint_uri_pa(self):
        """Test conversion from fs to cloud checkpoint and back."""
        checkpoint = self._prepare_fs_checkpoint()

        # Clean up mock bucket
        delete_at_uri(self.cloud_uri_pa)
        _ensure_directory(self.cloud_uri_pa)

        # Convert into dict checkpoint
        location = checkpoint.to_uri(self.cloud_uri_pa)
        self.assertIsInstance(location, str)
        self.assertIn("mock://", location)

        # Create from dict
        checkpoint = Checkpoint.from_uri(location)
        self.assertTrue(checkpoint._uri)

        self._assert_fs_checkpoint(checkpoint)

    def test_fs_delete_at_uri(self):
        """Test that clear bucket utility works"""
        checkpoint = self._prepare_fs_checkpoint()

        # Convert into dict checkpoint
        location = checkpoint.to_uri(self.cloud_uri)
        delete_at_uri(location)

        checkpoint = Checkpoint.from_uri(location)
        with self.assertRaises(FileNotFoundError):
            checkpoint.to_directory()

    def test_fs_cp_as_directory(self):
        checkpoint = self._prepare_fs_checkpoint()

        with checkpoint.as_directory() as checkpoint_dir:
            assert checkpoint._local_path == checkpoint_dir

        assert os.path.exists(checkpoint_dir)

    def test_dict_cp_as_directory(self):
        checkpoint = self._prepare_dict_checkpoint()

        with checkpoint.as_directory() as checkpoint_dir:
            assert os.path.exists(checkpoint_dir)

        assert not os.path.exists(checkpoint_dir)

    def test_obj_store_cp_as_directory(self):
        checkpoint = self._prepare_dict_checkpoint()

        # Convert into obj ref checkpoint
        obj_ref = checkpoint.to_object_ref()

        # Create from object ref
        checkpoint = Checkpoint.from_object_ref(obj_ref)

        with checkpoint.as_directory() as checkpoint_dir:
            assert os.path.exists(checkpoint_dir)
            assert checkpoint_dir.endswith(obj_ref.hex())

        assert not os.path.exists(checkpoint_dir)


class CheckpointsSerdeTest(unittest.TestCase):
    def setUp(self) -> None:
        if not ray.is_initialized():
            ray.init()

    def _testCheckpointSerde(
        self, checkpoint: Checkpoint, expected_type: str, expected_data: Any
    ):
        @ray.remote
        def assert_checkpoint_content(cp: Checkpoint):
            type_, data = cp.get_internal_representation()
            assert type_ == expected_type
            assert data == expected_data

            return True

        self.assertTrue(ray.get(assert_checkpoint_content.remote(checkpoint)))

    def testUriCheckpointSerde(self):
        # URI checkpoints keep the same internal representation, pointing to
        # a remote location

        checkpoint = Checkpoint.from_uri("s3://some/bucket")

        self._testCheckpointSerde(checkpoint, *checkpoint.get_internal_representation())

    def testDataCheckpointSerde(self):
        # Data checkpoints keep the same internal representation, including
        # their data.

        checkpoint = Checkpoint.from_dict({"checkpoint_data": 5})

        self._testCheckpointSerde(checkpoint, *checkpoint.get_internal_representation())

    def testLocalCheckpointSerde(self):
        # Local checkpoints are converted to bytes on serialization. Currently
        # this is a pickled dict, so we compare with a dict checkpoint.
        source_checkpoint = Checkpoint.from_dict({"checkpoint_data": 5})
        with source_checkpoint.as_directory() as tmpdir:
            checkpoint = Checkpoint.from_directory(tmpdir)
            self._testCheckpointSerde(
                checkpoint, *source_checkpoint.get_internal_representation()
            )

    def testBytesCheckpointSerde(self):
        # Bytes checkpoints are just dict checkpoints constructed
        # from pickled data, so we compare with the source dict checkpoint.
        source_checkpoint = Checkpoint.from_dict({"checkpoint_data": 5})
        blob = source_checkpoint.to_bytes()
        checkpoint = Checkpoint.from_bytes(blob)

        self._testCheckpointSerde(
            checkpoint, *source_checkpoint.get_internal_representation()
        )

    def testObjRefCheckpointSerde(self):
        # Obj ref checkpoints are dict checkpoints put into the Ray object
        # store, but they have their own data representation (the obj ref).
        # We thus compare with the actual obj ref checkpoint.
        source_checkpoint = Checkpoint.from_dict({"checkpoint_data": 5})
        obj_ref = source_checkpoint.to_object_ref()
        checkpoint = Checkpoint.from_object_ref(obj_ref)

        self._testCheckpointSerde(checkpoint, *checkpoint.get_internal_representation())


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
