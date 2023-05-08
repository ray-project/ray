import logging
import os
import pickle
import re
import shutil
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
import boto3

import ray
from ray.air._internal.remote_storage import _ensure_directory, delete_at_uri
from ray.air._internal.uri_utils import URI
from ray.air.checkpoint import _DICT_CHECKPOINT_ADDITIONAL_FILE_KEY, Checkpoint
from ray.air.constants import MAX_REPR_LENGTH, PREPROCESSOR_KEY
from ray.data import Preprocessor
from ray._private.test_utils import simulate_storage


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


class StubCheckpoint(Checkpoint):

    _SERIALIZED_ATTRS = ("foo",)

    def __init__(self, *args, **kwargs):
        self.foo = None
        self.baz = None
        super().__init__(*args, **kwargs)


class OtherStubCheckpoint(Checkpoint):
    pass


class OtherStubCheckpointWithAttrs(Checkpoint):
    _SERIALIZED_ATTRS = StubCheckpoint._SERIALIZED_ATTRS


def test_from_checkpoint():
    checkpoint = Checkpoint.from_dict({"spam": "ham"})
    assert type(StubCheckpoint.from_checkpoint(checkpoint)) is StubCheckpoint

    # Check that attributes persist if same checkpoint type.
    checkpoint = StubCheckpoint.from_dict({"spam": "ham"})
    checkpoint.foo = "bar"
    assert StubCheckpoint.from_checkpoint(checkpoint).foo == "bar"

    # Check that attributes persist if the new checkpoint
    # has them as well.
    # Check that attributes persist if same checkpoint type.
    checkpoint = StubCheckpoint.from_dict({"spam": "ham"})
    checkpoint.foo = "bar"
    assert OtherStubCheckpointWithAttrs.from_checkpoint(checkpoint).foo == "bar"


class TestCheckpointTypeCasting:
    def test_dict(self):
        data = StubCheckpoint.from_dict({"foo": "bar"}).to_dict()
        assert isinstance(Checkpoint.from_dict(data), StubCheckpoint)

        data = Checkpoint.from_dict({"foo": "bar"}).to_dict()
        assert isinstance(StubCheckpoint.from_dict(data), StubCheckpoint)

        with pytest.raises(ValueError):
            data = OtherStubCheckpoint.from_dict({"foo": "bar"}).to_dict()
            StubCheckpoint.from_dict(data)

    def test_directory(self):
        path = StubCheckpoint.from_dict({"foo": "bar"}).to_directory()
        assert isinstance(Checkpoint.from_directory(path), StubCheckpoint)

        path = Checkpoint.from_dict({"foo": "bar"}).to_directory()
        assert isinstance(StubCheckpoint.from_directory(path), StubCheckpoint)

        with pytest.raises(ValueError):
            path = OtherStubCheckpoint.from_dict({"foo": "bar"}).to_directory()
            StubCheckpoint.from_directory(path)

    def test_uri(self):
        uri = StubCheckpoint.from_dict({"foo": "bar"}).to_uri("memory://1/")
        assert isinstance(Checkpoint.from_uri(uri), StubCheckpoint)

        uri = Checkpoint.from_dict({"foo": "bar"}).to_uri("memory://2/")
        assert isinstance(StubCheckpoint.from_uri(uri), StubCheckpoint)

        with pytest.raises(ValueError):
            uri = OtherStubCheckpoint.from_dict({"foo": "bar"}).to_uri("memory://3/")
            StubCheckpoint.from_uri(uri)

    def test_e2e(self):
        from ray.air import session
        from ray.air.config import ScalingConfig
        from ray.train.torch import TorchTrainer

        def train_loop_per_worker():
            checkpoint = StubCheckpoint.from_dict({"spam": "ham"})
            session.report({}, checkpoint=checkpoint)

        trainer = TorchTrainer(
            train_loop_per_worker=train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=1),
        )
        results = trainer.fit()
        assert isinstance(results.checkpoint, StubCheckpoint)


class TestCheckpointSerializedAttrs:
    def test_dict(self):
        checkpoint = StubCheckpoint.from_dict({"spam": "ham"})
        assert "foo" in checkpoint._SERIALIZED_ATTRS
        checkpoint.foo = "bar"

        recovered_checkpoint = StubCheckpoint.from_dict(checkpoint.to_dict())

        assert recovered_checkpoint.foo == "bar"

    def test_directory(self):
        checkpoint = StubCheckpoint.from_dict({"spam": "ham"})
        assert "foo" in checkpoint._SERIALIZED_ATTRS
        checkpoint.foo = "bar"

        recovered_checkpoint = StubCheckpoint.from_directory(checkpoint.to_directory())

        assert recovered_checkpoint.foo == "bar"

    def test_directory_move_instead_of_copy(self):
        checkpoint = StubCheckpoint.from_dict({"spam": "ham"})
        assert "foo" in checkpoint._SERIALIZED_ATTRS
        checkpoint.foo = "bar"

        path = checkpoint.to_directory()
        recovered_checkpoint = StubCheckpoint.from_directory(path)
        tmpdir = tempfile.mkdtemp()
        new_path = recovered_checkpoint._move_directory(tmpdir)
        new_recovered_checkpoint = StubCheckpoint.from_directory(new_path)

        assert recovered_checkpoint._local_path == tmpdir
        assert new_recovered_checkpoint.foo == "bar"
        assert not list(Path(path).glob("*"))

    def test_uri(self):
        checkpoint = StubCheckpoint.from_dict({"spam": "ham"})
        assert "foo" in checkpoint._SERIALIZED_ATTRS
        checkpoint.foo = "bar"

        uri = checkpoint.to_uri("memory://bucket")
        recovered_checkpoint = StubCheckpoint.from_uri(uri)

        assert recovered_checkpoint.foo == "bar"

    def test_e2e(self):
        from ray.air import session
        from ray.air.config import ScalingConfig
        from ray.train.torch import TorchTrainer

        def train_loop_per_worker():
            checkpoint = StubCheckpoint.from_dict({"spam": "ham"})
            assert "foo" in checkpoint._SERIALIZED_ATTRS
            checkpoint.foo = "bar"
            session.report({}, checkpoint=checkpoint)

        trainer = TorchTrainer(
            train_loop_per_worker=train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=1),
        )
        results = trainer.fit()
        assert results.checkpoint.foo == "bar"


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

    def _prepare_dict_checkpoint(self) -> StubCheckpoint:
        # Create checkpoint from dict
        checkpoint = StubCheckpoint.from_dict(dict(self.checkpoint_dict_data))
        # The `foo` attribute should be serialized.
        self.assertTrue("foo" in StubCheckpoint._SERIALIZED_ATTRS)
        checkpoint.foo = "bar"
        # The `baz` attribute shouldn't be serialized.
        self.assertFalse("baz" in StubCheckpoint._SERIALIZED_ATTRS)
        checkpoint.baz = "qux"

        self.assertIsInstance(checkpoint, StubCheckpoint)
        self.assertTrue(checkpoint._data_dict)
        self.assertEqual(
            checkpoint._data_dict["metric"], self.checkpoint_dict_data["metric"]
        )

        return checkpoint

    def _assert_dict_checkpoint(self, checkpoint, check_state=True):
        # Convert into dict
        checkpoint_data = checkpoint.to_dict()
        self.assertIsInstance(checkpoint, StubCheckpoint)
        if check_state:
            self.assertEqual(checkpoint.foo, "bar")
            self.assertEqual(checkpoint.baz, None)
        checkpoint_data = {
            key: value
            for key, value in checkpoint_data.items()
            if not key.startswith("_")
        }
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
        obj_ref = ray.put(checkpoint)
        self.assertIsInstance(obj_ref, ray.ObjectRef)

        # Create from dict
        checkpoint = ray.get(obj_ref)
        self._assert_dict_checkpoint(checkpoint, check_state=False)

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

    def _prepare_fs_checkpoint(self) -> StubCheckpoint:
        # Create checkpoint from fs
        checkpoint = StubCheckpoint.from_directory(self.checkpoint_dir)
        # The `foo` attribute should be serialized.
        self.assertIn("foo", StubCheckpoint._SERIALIZED_ATTRS)
        checkpoint.foo = "bar"
        # The `baz` attribute shouldn't be serialized.
        self.assertNotIn("baz", StubCheckpoint._SERIALIZED_ATTRS)
        checkpoint.baz = "qux"

        self.assertTrue(checkpoint._local_path, str)
        self.assertEqual(checkpoint._local_path, self.checkpoint_dir)

        return checkpoint

    def _assert_fs_checkpoint(self, checkpoint):
        # Convert back to directory
        local_dir = checkpoint.to_directory()

        with open(os.path.join(local_dir, "test_data.pkl"), "rb") as fp:
            local_data = pickle.load(fp)

        self.assertEqual(checkpoint.foo, "bar")
        self.assertEqual(checkpoint.baz, None)
        self.assertIsInstance(checkpoint, StubCheckpoint)
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
        obj_ref = ray.put(checkpoint)

        # Create from object ref
        checkpoint = ray.get(obj_ref)
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
        obj_ref = ray.put(checkpoint)

        # Create from object ref
        checkpoint = ray.get(obj_ref)

        with checkpoint.as_directory() as checkpoint_dir:
            assert os.path.exists(checkpoint_dir)
            assert Path(checkpoint_dir).stem.endswith(checkpoint._uuid.hex)

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

        checkpoint = Checkpoint.from_uri("memory:///some/bucket")

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
        obj_ref = ray.put(source_checkpoint)
        checkpoint = ray.get(obj_ref)

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

    def testDictCheckpointSetPreprocessor(self):
        preprocessor = DummyPreprocessor(1)
        data = {"metric": 5}
        checkpoint = Checkpoint.from_dict(data)
        checkpoint.set_preprocessor(preprocessor)
        preprocessor = checkpoint.get_preprocessor()
        assert preprocessor.multiplier == 1

        # Check that we can set it to None
        checkpoint.set_preprocessor(None)
        preprocessor = checkpoint.get_preprocessor()
        assert preprocessor is None

    def testDictCheckpointSetPreprocessorAsDir(self):
        preprocessor = DummyPreprocessor(1)
        data = {"metric": 5}
        checkpoint = Checkpoint.from_dict(data)
        checkpoint.set_preprocessor(preprocessor)
        checkpoint_path = checkpoint.to_directory()
        checkpoint = Checkpoint.from_directory(checkpoint_path)
        preprocessor = checkpoint.get_preprocessor()
        assert preprocessor.multiplier == 1

    def testDirCheckpointSetPreprocessor(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            preprocessor = DummyPreprocessor(1)
            data = {"metric": 5}
            checkpoint_dir = os.path.join(tmpdir, "existing_checkpoint")
            os.mkdir(checkpoint_dir, 0o755)
            with open(os.path.join(checkpoint_dir, "test_data.pkl"), "wb") as fp:
                pickle.dump(data, fp)
            checkpoint = Checkpoint.from_directory(checkpoint_dir)
            checkpoint.set_preprocessor(preprocessor)
            preprocessor = checkpoint.get_preprocessor()
            assert preprocessor.multiplier == 1

            # Also check that loading from dir works
            new_checkpoint_dir = os.path.join(tmpdir, "new_checkpoint")
            checkpoint.to_directory(new_checkpoint_dir)
            checkpoint = Checkpoint.from_directory(new_checkpoint_dir)
            preprocessor = checkpoint.get_preprocessor()
            assert preprocessor.multiplier == 1

    def testDirCheckpointSetPreprocessorAsDict(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            preprocessor = DummyPreprocessor(1)
            data = {"metric": 5}
            checkpoint_dir = os.path.join(tmpdir, "existing_checkpoint")
            os.mkdir(checkpoint_dir, 0o755)
            with open(os.path.join(checkpoint_dir, "test_data.pkl"), "wb") as fp:
                pickle.dump(data, fp)
            checkpoint = Checkpoint.from_directory(checkpoint_dir)
            checkpoint.set_preprocessor(preprocessor)
            checkpoint_dict = checkpoint.to_dict()
            checkpoint = checkpoint.from_dict(checkpoint_dict)
            preprocessor = checkpoint.get_preprocessor()
            assert preprocessor.multiplier == 1

    def testObjectRefCheckpointSetPreprocessor(self):
        ckpt = Checkpoint.from_dict({"x": 1})
        ckpt = ray.get(ray.put(ckpt))
        preprocessor = DummyPreprocessor(1)
        ckpt.set_preprocessor(preprocessor)

        assert ckpt.get_preprocessor() == preprocessor

    def testAttrPath(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint = Checkpoint.from_directory(tmpdir)
            with self.assertRaises(TypeError):
                os.path.exists(checkpoint)

    def testCheckpointUri(self):
        orig_checkpoint = Checkpoint.from_dict({"data": 2})

        self.assertEqual(orig_checkpoint.uri, None)

        # file:// URI
        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint = Checkpoint.from_directory(orig_checkpoint.to_directory(tmpdir))
            self.assertEqual(checkpoint.uri, "file://" + tmpdir)

        # cloud URI
        checkpoint = Checkpoint.from_uri(
            orig_checkpoint.to_uri("memory://some/location")
        )
        self.assertEqual(checkpoint.uri, "memory://some/location")


class URITestCheckpoint(Checkpoint):
    def _to_directory(self, path: str, move_instead_of_copy: bool = False) -> None:
        super()._to_directory(path, move_instead_of_copy)
        # Drop a marker file with the current pid.
        # Only one file should be created, as only one task should
        # download the data, with the rest waiting.
        with open(Path(path, f"_pid_marker_{os.getpid()}"), "w"):
            pass


@contextmanager
def mock_s3_bucket_uri():
    port = 5002
    region = "us-west-2"
    with simulate_storage("s3", port=port, region=region) as s3_uri:
        s3 = boto3.client(
            "s3", region_name=region, endpoint_url=f"http://localhost:{port}"
        )
        # Bucket name will be autogenerated/unique per test
        bucket_name = URI(s3_uri).name
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )
        # Disable server HTTP request logging
        logging.getLogger("werkzeug").setLevel(logging.WARNING)
        yield URI(s3_uri)
        logging.getLogger("werkzeug").setLevel(logging.INFO)


@ray.remote
def download_uri_checkpoint(checkpoint: URITestCheckpoint):
    with checkpoint.as_directory() as dir:
        dir = Path(dir)
        all_pid_marker_files = list(dir.glob("_pid_marker_*"))
        # There should be only one file, as only one task should
        # download.
        assert len(all_pid_marker_files) == 1
        assert (dir / "mock.file").exists()


class TestCheckpointURIConstantUUID(unittest.TestCase):
    def setUp(self) -> None:
        ray.shutdown()
        ray.init(num_cpus=4)

    def tearDown(self) -> None:
        ray.shutdown()

    def testCheckpointURIConstantUUID(self):
        """Test that multiple workers using the same URI checkpoint
        share the local directory, and that only one worker downloads
        the data."""
        with mock_s3_bucket_uri() as base_uri, tempfile.TemporaryDirectory() as tmpdir:
            checkpoint_dir = Path(tmpdir, "checkpoint")
            os.makedirs(checkpoint_dir)
            with open(checkpoint_dir / "mock.file", "w"):
                pass
            checkpoint_uri = str(base_uri / "model")
            uri = Checkpoint.from_directory(checkpoint_dir).to_uri(checkpoint_uri)

            # Check that two separate checkpoints have the same uuid
            checkpoint = URITestCheckpoint.from_uri(uri)
            checkpoint2 = URITestCheckpoint.from_uri(uri)
            assert checkpoint._uuid == checkpoint2._uuid

            # Create a separate checkpoint for each task
            tasks = [
                download_uri_checkpoint.remote(URITestCheckpoint.from_uri(uri))
                for _ in range(4)
            ]
            ray.get(tasks)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
