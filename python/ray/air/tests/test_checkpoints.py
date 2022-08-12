import os
import pickle
import re
import shutil
import tempfile
import unittest
from typing import Any

import ray
from ray.air._internal.remote_storage import delete_at_uri, _ensure_directory
from ray.air.checkpoint import Checkpoint, _DICT_CHECKPOINT_ADDITIONAL_FILE_KEY
from ray.air.constants import MAX_REPR_LENGTH, PREPROCESSOR_KEY
from ray.data import Preprocessor


class DummyPreprocessor(Preprocessor):
    def __init__(self, multiplier):
        self.multiplier = multiplier

    def transform_batch(self, df):
        return df * self.multiplier


def test_repr():
    checkpoint = Checkpoint(data_dict={"foo": "bar"})

    representation = repr(checkpoint)

    assert len(representation) < MAX_REPR_LENGTH
    pattern = re.compile("^Checkpoint\\((.*)\\)$")
    assert pattern.match(representation)


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

    def test_metadata(self):
        """Test conversion with metadata involved.

        a. from fs to dict checkpoint;
        b. drop some marker to dict checkpoint;
        c. convert back to fs checkpoint;
        d. convert back to dict checkpoint.

        Assert that the marker should still be there."""
        checkpoint = self._prepare_fs_checkpoint()

        # Convert into dict checkpoint
        data_dict = checkpoint.to_dict()
        self.assertIsInstance(data_dict, dict)

        data_dict["my_marker"] = "marked"

        # Create from dict
        checkpoint = Checkpoint.from_dict(data_dict)
        self.assertTrue(checkpoint._data_dict)

        self._assert_fs_checkpoint(checkpoint)

        # Convert back to dict
        data_dict_2 = Checkpoint.from_directory(checkpoint.to_directory()).to_dict()
        assert data_dict_2["my_marker"] == "marked"

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

    def test_dict_checkpoint_additional_files(self):
        checkpoint = self._prepare_dict_checkpoint()

        # Convert to directory
        checkpoint_dir = checkpoint.to_directory()

        # Add file into checkpoint directory
        with open(os.path.join(checkpoint_dir, "additional_file.txt"), "w") as f:
            f.write("Additional data\n")
        os.mkdir(os.path.join(checkpoint_dir, "subdir"))
        with open(os.path.join(checkpoint_dir, "subdir", "another.txt"), "w") as f:
            f.write("Another additional file\n")

        # Create new checkpoint object
        checkpoint = Checkpoint.from_directory(checkpoint_dir)

        new_dir = checkpoint.to_directory()

        assert os.path.exists(os.path.join(new_dir, "additional_file.txt"))
        with open(os.path.join(new_dir, "additional_file.txt"), "r") as f:
            assert f.read() == "Additional data\n"

        assert os.path.exists(os.path.join(new_dir, "subdir", "another.txt"))
        with open(os.path.join(new_dir, "subdir", "another.txt"), "r") as f:
            assert f.read() == "Another additional file\n"

        checkpoint_dict = checkpoint.to_dict()
        for k, v in self.checkpoint_dict_data.items():
            assert checkpoint_dict[k] == v

        assert _DICT_CHECKPOINT_ADDITIONAL_FILE_KEY in checkpoint_dict

        # Add another field
        checkpoint_dict["new_field"] = "Data"

        another_dict = Checkpoint.from_directory(
            Checkpoint.from_dict(checkpoint_dict).to_directory()
        ).to_dict()
        assert _DICT_CHECKPOINT_ADDITIONAL_FILE_KEY in another_dict
        assert another_dict["new_field"] == "Data"

    def test_fs_checkpoint_additional_fields(self):
        checkpoint = self._prepare_fs_checkpoint()

        # Convert to dict
        checkpoint_dict = checkpoint.to_dict()

        # Add field to dict
        checkpoint_dict["additional_field"] = "data"

        # Create new checkpoint object
        checkpoint = Checkpoint.from_dict(checkpoint_dict)

        # Turn into FS
        checkpoint_dir = checkpoint.to_directory()

        assert os.path.exists(os.path.join(checkpoint_dir, "test_data.pkl"))
        assert os.path.exists(os.path.join(checkpoint_dir, "additional_field.meta.pkl"))

        # Add new file
        with open(os.path.join(checkpoint_dir, "even_more.txt"), "w") as f:
            f.write("More\n")

        # Turn into dict
        new_dict = Checkpoint.from_directory(checkpoint_dir).to_dict()

        assert new_dict["additional_field"] == "data"

        # Turn into fs
        new_dir = Checkpoint.from_dict(new_dict).to_directory()

        assert os.path.exists(os.path.join(new_dir, "test_data.pkl"))
        assert os.path.exists(os.path.join(new_dir, "additional_field.meta.pkl"))
        assert os.path.exists(os.path.join(new_dir, "even_more.txt"))


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


class PreprocessorCheckpointTest(unittest.TestCase):
    def testDictCheckpointWithoutPreprocessor(self):
        data = {"metric": 5}
        checkpoint = Checkpoint.from_dict(data)
        preprocessor = checkpoint.get_preprocessor()
        assert preprocessor is None

    def testDictCheckpointWithPreprocessor(self):
        preprocessor = DummyPreprocessor(1)
        data = {"metric": 5, PREPROCESSOR_KEY: preprocessor}
        checkpoint = Checkpoint.from_dict(data)
        preprocessor = checkpoint.get_preprocessor()
        assert preprocessor.multiplier == 1

    def testDictCheckpointWithPreprocessorAsDir(self):
        preprocessor = DummyPreprocessor(1)
        data = {"metric": 5, PREPROCESSOR_KEY: preprocessor}
        checkpoint = Checkpoint.from_dict(data)
        checkpoint_path = checkpoint.to_directory()
        checkpoint = Checkpoint.from_directory(checkpoint_path)
        preprocessor = checkpoint.get_preprocessor()
        assert preprocessor.multiplier == 1

    def testDirCheckpointWithoutPreprocessor(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data = {"metric": 5}
            checkpoint_dir = os.path.join(tmpdir, "existing_checkpoint")
            os.mkdir(checkpoint_dir, 0o755)
            with open(os.path.join(checkpoint_dir, "test_data.pkl"), "wb") as fp:
                pickle.dump(data, fp)
            checkpoint = Checkpoint.from_directory(checkpoint_dir)
            preprocessor = checkpoint.get_preprocessor()
            assert preprocessor is None

    def testDirCheckpointWithPreprocessor(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            preprocessor = DummyPreprocessor(1)
            data = {"metric": 5}
            checkpoint_dir = os.path.join(tmpdir, "existing_checkpoint")
            os.mkdir(checkpoint_dir, 0o755)
            with open(os.path.join(checkpoint_dir, "test_data.pkl"), "wb") as fp:
                pickle.dump(data, fp)
            with open(os.path.join(checkpoint_dir, PREPROCESSOR_KEY), "wb") as fp:
                pickle.dump(preprocessor, fp)
            checkpoint = Checkpoint.from_directory(checkpoint_dir)
            preprocessor = checkpoint.get_preprocessor()
            assert preprocessor.multiplier == 1

    def testDirCheckpointWithPreprocessorAsDict(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            preprocessor = DummyPreprocessor(1)
            data = {"metric": 5}
            checkpoint_dir = os.path.join(tmpdir, "existing_checkpoint")
            os.mkdir(checkpoint_dir, 0o755)
            with open(os.path.join(checkpoint_dir, "test_data.pkl"), "wb") as fp:
                pickle.dump(data, fp)
            with open(os.path.join(checkpoint_dir, PREPROCESSOR_KEY), "wb") as fp:
                pickle.dump(preprocessor, fp)
            checkpoint = Checkpoint.from_directory(checkpoint_dir)
            checkpoint_dict = checkpoint.to_dict()
            checkpoint = checkpoint.from_dict(checkpoint_dict)
            preprocessor = checkpoint.get_preprocessor()
            assert preprocessor.multiplier == 1

    def testAttrPath(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint = Checkpoint.from_directory(tmpdir)
            with self.assertRaises(TypeError):
                os.path.exists(checkpoint)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
