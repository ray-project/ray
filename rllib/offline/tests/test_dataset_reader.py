import tempfile
import os
from pathlib import Path
import unittest
import pytest


import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.offline import IOContext
from ray.rllib.offline.dataset_reader import (
    DatasetReader,
    get_dataset_and_shards,
    _unzip_if_needed,
)


class TestDatasetReader(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()
        # TODO(Kourosh): Hitting S3 in CI is currently broken due to some AWS
        #  credentials issues, using a local file instead for now.

        # cls.dset_path = "s3://air-example-data/rllib/cartpole/large.json"
        cls.dset_path = "tests/data/pendulum/large.json"

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_dataset_reader_itr_batches(self):
        """Test that the dataset reader iterates over batches of rows correctly."""
        input_config = {"format": "json", "paths": self.dset_path}
        dataset, _ = get_dataset_and_shards(
            AlgorithmConfig().offline_data(input_="dataset", input_config=input_config)
        )

        ioctx = IOContext(
            config=(
                AlgorithmConfig()
                .training(train_batch_size=1200)
                .offline_data(actions_in_input_normalized=True)
            ),
            worker_index=0,
        )
        reader = DatasetReader(dataset, ioctx)
        assert len(reader.next()) >= 1200

    def test_dataset_shard_with_only_local(self):
        """Tests whether the dataset_shard function works correctly for a single shard
        for the local worker."""
        config = AlgorithmConfig().offline_data(
            input_="dataset", input_config={"format": "json", "paths": self.dset_path}
        )

        # two ways of doing this:

        # we have no remote workers
        _, shards = get_dataset_and_shards(config, num_workers=0)

        assert len(shards) == 1
        assert isinstance(shards[0], ray.data.Dataset)

    def test_dataset_shard_remote_workers_with_local_worker(self):
        """Tests whether the dataset_shard function works correctly for the remote
        workers with a dummy dataset shard for the local worker."""

        config = AlgorithmConfig().offline_data(
            input_="dataset", input_config={"format": "json", "paths": self.dset_path}
        )
        NUM_WORKERS = 4

        _, shards = get_dataset_and_shards(config, num_workers=NUM_WORKERS)

        assert len(shards) == NUM_WORKERS + 1
        assert shards[0] is None
        assert all(
            isinstance(remote_shard, ray.data.Dataset) for remote_shard in shards[1:]
        )

    def test_dataset_shard_with_task_parallelization(self):
        """Tests whether the dataset_shard function works correctly with parallelism
        for reading the dataset."""
        config = (
            AlgorithmConfig()
            .offline_data(
                input_="dataset",
                input_config={
                    "format": "json",
                    "paths": self.dset_path,
                },
            )
            .rollouts(num_rollout_workers=10)
        )
        NUM_WORKERS = 4

        _, shards = get_dataset_and_shards(config, num_workers=NUM_WORKERS)

        assert len(shards) == NUM_WORKERS + 1
        assert shards[0] is None
        assert all(
            isinstance(remote_shard, ray.data.Dataset) for remote_shard in shards[1:]
        )

    def test_dataset_shard_with_loader_fn(self):
        """Tests whether the dataset_shard function works correctly with loader_fn."""
        dset = ray.data.range(100)
        config = AlgorithmConfig().offline_data(
            input_="dataset", input_config={"loader_fn": lambda: dset}
        )

        ret_dataset, _ = get_dataset_and_shards(config)
        assert ret_dataset.count() == dset.count()

    def test_dataset_shard_error_with_unsupported_dataset_format(self):
        """Tests whether the dataset_shard function raises an error when an unsupported
        dataset format is specified."""

        config = AlgorithmConfig().offline_data(
            input_="dataset",
            input_config={
                "format": "__UNSUPPORTED_FORMAT__",
                "paths": self.dset_path,
            },
        )

        with self.assertRaises(ValueError):
            get_dataset_and_shards(config)

    def test_dataset_shard_error_with_both_format_and_loader_fn(self):
        """Tests whether the dataset_shard function raises an error when both format
        and loader_fn are specified."""
        dset = ray.data.range(100)

        config = AlgorithmConfig().offline_data(
            input_="dataset",
            input_config={
                "format": "json",
                "paths": self.dset_path,
                "loader_fn": lambda: dset,
            },
        )

        with self.assertRaises(ValueError):
            get_dataset_and_shards(config)

    def test_default_ioctx(self):
        # Test DatasetReader without passing in IOContext
        input_config = {"format": "json", "paths": self.dset_path}
        config = AlgorithmConfig().offline_data(
            input_="dataset", input_config=input_config
        )
        dataset, _ = get_dataset_and_shards(config)
        reader = DatasetReader(dataset)
        # Reads in one line of Pendulum dataset with 600 timesteps
        assert len(reader.next()) == 600


class TestUnzipIfNeeded(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.s3_path = "s3://air-example-data/rllib/pendulum"
        cls.relative_path = "tests/data/pendulum"
        cls.absolute_path = str(
            Path(__file__).parent.parent.parent / "tests" / "data" / "pendulum"
        )

    # @TODO: unskip when this is fixed
    @pytest.mark.skip(reason="Shouldn't hit S3 in CI")
    def test_s3_zip(self):
        """Tests whether the unzip_if_needed function works correctly on s3 zip
        files"""
        unzipped_paths = _unzip_if_needed([self.s3_path + "/enormous.zip"], "json")
        self.assertEqual(
            str(Path(unzipped_paths[0]).absolute()),
            str(Path("./").absolute() / "enormous.json"),
        )

    def test_relative_zip(self):
        """Tests whether the unzip_if_needed function works correctly on relative zip
        files"""

        # this should work regardless of where th current working directory is.
        with tempfile.TemporaryDirectory() as tmp_dir:
            cwdir = os.getcwd()
            os.chdir(tmp_dir)
            unzipped_paths = _unzip_if_needed(
                [str(Path(self.relative_path) / "enormous.zip")], "json"
            )
            self.assertEqual(
                str(Path(unzipped_paths[0]).absolute()),
                str(Path("./").absolute() / "enormous.json"),
            )

            assert all(Path(fpath).exists() for fpath in unzipped_paths)
            os.chdir(cwdir)

    def test_absolute_zip(self):
        """Tests whether the unzip_if_needed function works correctly on absolute zip
        files"""

        # this should work regardless of where th current working directory is.
        with tempfile.TemporaryDirectory() as tmp_dir:
            cwdir = os.getcwd()
            os.chdir(tmp_dir)
            unzipped_paths = _unzip_if_needed(
                [str(Path(self.absolute_path) / "enormous.zip")], "json"
            )
            self.assertEqual(
                str(Path(unzipped_paths[0]).absolute()),
                str(Path("./").absolute() / "enormous.json"),
            )

            assert all(Path(fpath).exists() for fpath in unzipped_paths)
            os.chdir(cwdir)

    # @TODO: unskip when this is fixed
    @pytest.mark.skip(reason="Shouldn't hit S3 in CI")
    def test_s3_json(self):
        """Tests whether the unzip_if_needed function works correctly on s3 json
        files"""

        # this should work regardless of where th current working directory is.
        with tempfile.TemporaryDirectory() as tmp_dir:
            cwdir = os.getcwd()
            os.chdir(tmp_dir)
            unzipped_paths = _unzip_if_needed([self.s3_path + "/large.json"], "json")
            self.assertEqual(
                unzipped_paths[0],
                self.s3_path + "/large.json",
            )

            os.chdir(cwdir)

    def test_relative_json(self):
        """Tests whether the unzip_if_needed function works correctly on relative json
        files"""
        # this should work regardless of where th current working directory is.
        with tempfile.TemporaryDirectory() as tmp_dir:
            cwdir = os.getcwd()
            os.chdir(tmp_dir)
            unzipped_paths = _unzip_if_needed(
                [str(Path(self.relative_path) / "large.json")], "json"
            )
            self.assertEqual(
                os.path.realpath(str(Path(unzipped_paths[0]).absolute())),
                os.path.realpath(
                    str(
                        Path(__file__).parent.parent.parent
                        / self.relative_path
                        / "large.json"
                    )
                ),
            )

            assert all(Path(fpath).exists() for fpath in unzipped_paths)
            os.chdir(cwdir)

    def test_absolute_json(self):
        """Tests whether the unzip_if_needed function works correctly on absolute json
        files"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            cwdir = os.getcwd()
            os.chdir(tmp_dir)
            unzipped_paths = _unzip_if_needed(
                [str(Path(self.absolute_path) / "large.json")], "json"
            )
            self.assertEqual(
                os.path.realpath(unzipped_paths[0]),
                os.path.realpath(
                    str(Path(self.absolute_path).absolute() / "large.json")
                ),
            )

            assert all(Path(fpath).exists() for fpath in unzipped_paths)
            os.chdir(cwdir)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
