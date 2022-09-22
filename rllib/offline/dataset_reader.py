import logging
import math
from pathlib import Path
import re
import numpy as np
from typing import List, Tuple, Optional
import zipfile

import ray.data
from ray.rllib.offline.input_reader import InputReader
from ray.rllib.offline.io_context import IOContext
from ray.rllib.offline.json_reader import from_json_data, postprocess_actions
from ray.rllib.policy.sample_batch import concat_samples, SampleBatch, DEFAULT_POLICY_ID
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.typing import SampleBatchType, AlgorithmConfigDict

logger = logging.getLogger(__name__)

DEFAULT_NUM_CPUS_PER_TASK = 0.5


# TODO: @avnishn what is the use of this function anymore?
def _get_resource_bundles(config: AlgorithmConfigDict):
    input_config = config.get("input_config", {})
    parallelism = input_config.get("parallelism", config.get("num_workers", 1))
    cpus_per_task = input_config.get(
        "num_cpus_per_read_task", DEFAULT_NUM_CPUS_PER_TASK
    )
    return [{"CPU": math.ceil(parallelism * cpus_per_task)}]


def _unzip_this_path(fpath: Path, extract_path: str):
    with zipfile.ZipFile(str(fpath), "r") as zip_ref:
        zip_ref.extractall(extract_path)


def _unzip_if_needed(paths: List[str], format: str):
    """If a path in paths is a zip file, unzip it and use path of the unzipped file"""
    ret_paths = []
    for path in paths:
        if re.search("\\.zip$", str(path)):
            # TODO: We need to add unzip support for s3
            if str(path).startswith("s3://"):
                raise ValueError(
                    "unzip_if_needed currently does not support remote paths from s3"
                )
            extract_path = "./"
            try:
                _unzip_this_path(str(path), extract_path)
            except FileNotFoundError:
                # intrepreted as a relative path to rllib folder
                try:
                    # TODO: remove this later when we replace all tests with s3 paths
                    _unzip_this_path(Path(__file__).parent.parent / path, extract_path)
                except FileNotFoundError:
                    raise FileNotFoundError(f"File not found: {path}")

            unzipped_path = str(
                Path(extract_path).absolute() / f"{Path(path).stem}.{format}"
            )
            ret_paths.append(unzipped_path)
        else:
            # TODO: We can get rid of this logic when we replace all tests with s3 paths
            if str(path).startswith("s3://"):
                ret_paths.append(path)
            else:
                if not Path(path).exists():
                    relative_path = str(Path(__file__).parent.parent / path)
                    if not Path(relative_path).exists():
                        raise FileNotFoundError(f"File not found: {path}")
                    path = relative_path
                ret_paths.append(path)
    return ret_paths


@PublicAPI
def get_dataset_and_shards(
    config: AlgorithmConfigDict, num_workers: int = 0
) -> Tuple[ray.data.dataset.Dataset, List[ray.data.dataset.Dataset]]:
    """Returns a dataset and a list of shards.

    This function uses algorithm configs to create a dataset and a list of shards.
    The following config keys are used to create the dataset:
        input: The input type should be "dataset".
        input_config: A dict containing the following key and values:
            `format`: str, speciifies the format of the input data. This will be the
            format that ray dataset supports. See ray.data.dataset.Dataset for
            supported formats. Only "parquet" or "json" are supported for now.
            `paths`: str, a single string or a list of strings. Each string is a path
            to a file or a directory holding the dataset. It can be either a local path
            or a remote path (e.g. to an s3 bucket).
            `loader_fn`: Callable[None, ray.data.dataset.Dataset], Instead of
            specifying paths and format, you can specify a function to load the dataset.
            `parallelism`: int, The number of tasks to use for loading the dataset.
            If not specified, it will be set to the number of workers.
            `num_cpus_per_read_task`: float, The number of CPUs to use for each read
            task. If not specified, it will be set to 0.5.

    Args:
        config: The config dict for the algorithm.
        num_workers: The number of shards to create for remote workers.

    Returns:
        dataset: The dataset object.
        shards: A list of dataset shards. For num_workers > 0 the first returned
        shared would be a dummy None shard for local_worker.
    """
    # check input and input config keys
    assert config["input"] == "dataset", (
        f"Must specify input as dataset if"
        f" calling `get_dataset_and_shards`. Got {config['input']}"
    )
    assert (
        "input_config" in config
    ), "Must specify input_config dict if using Dataset input."

    # check input config format
    input_config = config["input_config"]
    format = input_config.get("format")

    supported_fmts = ["json", "parquet"]
    if format is not None and format not in supported_fmts:
        raise ValueError(
            f"Unsupported format {format}. Supported formats are {supported_fmts}"
        )

    # check paths and loader_fn since only one of them is required.
    paths = input_config.get("paths")
    loader_fn = input_config.get("loader_fn")
    if loader_fn and (format or paths):
        raise ValueError(
            "When using a `loader_fn`, you cannot specify a `format` or `path`."
        )

    # check if at least loader_fn or format + path is specified.
    if not (format and paths) and not loader_fn:
        raise ValueError(
            f"If using a loader_fn: {loader_fn} that constructs a dataset, "
            "neither format: {format} and paths: {paths} must not be specified. If "
            "format and paths are specified, a loader_fn must not be specified."
        )

    # check paths to be a str or list[str] if not None
    if paths is not None:
        if isinstance(paths, str):
            paths = [paths]
        elif isinstance(paths, list):
            assert isinstance(paths[0], str), "Paths must be a list of path strings."
        else:
            raise ValueError("Paths must be a path string or a list of path strings.")
        paths = _unzip_if_needed(paths, format)

    parallelism = input_config.get("parallelism", num_workers or 1)
    cpus_per_task = input_config.get(
        "num_cpus_per_read_task", DEFAULT_NUM_CPUS_PER_TASK
    )

    if loader_fn:
        dataset = loader_fn()
    elif format == "json":
        dataset = ray.data.read_json(
            paths, parallelism=parallelism, ray_remote_args={"num_cpus": cpus_per_task}
        )
    elif format == "parquet":
        dataset = ray.data.read_parquet(
            paths, parallelism=parallelism, ray_remote_args={"num_cpus": cpus_per_task}
        )
    else:
        raise ValueError("Un-supported Ray dataset format: ", format)

    # Local worker will be responsible for sampling.
    if num_workers == 0:
        # Dataset is the only shard we need.
        return dataset, [dataset]
    # Remote workers are responsible for sampling:
    else:
        # Each remote worker gets 1 shard.
        remote_shards = dataset.repartition(
            num_blocks=num_workers, shuffle=False
        ).split(num_workers)

        # The first None shard is for the local worker, which
        # shouldn't be doing rollout work anyways.
        return dataset, [None] + remote_shards


@PublicAPI
class DatasetReader(InputReader):
    """Reader object that loads data from Ray Dataset.

    Examples:
        config = {
            "input": "dataset",
            "input_config": {
                "format": "json",
                # A single data file, a directory, or anything
                # that ray.data.dataset recognizes.
                "paths": "/tmp/sample_batches/",
                # By default, parallelism=num_workers.
                "parallelism": 3,
                # Dataset allocates 0.5 CPU for each reader by default.
                # Adjust this value based on the size of your offline dataset.
                "num_cpus_per_read_task": 0.5,
            }
        }
    """

    @PublicAPI
    def __init__(self, ds: ray.data.Dataset, ioctx: Optional[IOContext] = None):
        """Initializes a DatasetReader instance.

        Args:
            ds: Ray dataset to sample from.
        """
        self._ioctx = ioctx or IOContext()
        self._default_policy = self.policy_map = None
        self.preprocessor = None
        self._dataset = ds
        self.count = None if not self._dataset else self._dataset.count()
        # do this to disable the ray data stdout logging
        ray.data.set_progress_bars(enabled=False)

        # the number of steps to return per call to next()
        self.batch_size = self._ioctx.config.get("train_batch_size", 1)
        num_workers = self._ioctx.config.get("num_workers", 0)
        seed = self._ioctx.config.get("seed", None)
        if num_workers:
            self.batch_size = max(math.ceil(self.batch_size / num_workers), 1)
        # We allow the creation of a non-functioning None DatasetReader.
        # It's useful for example for a non-rollout local worker.
        if ds:
            if self._ioctx.worker is not None:
                self._policy_map = self._ioctx.worker.policy_map
                self._default_policy = self._policy_map.get(DEFAULT_POLICY_ID)
                self.preprocessor = (
                    self._ioctx.worker.preprocessors.get(DEFAULT_POLICY_ID)
                    if not self._ioctx.config.get("_disable_preprocessors", False)
                    else None
                )
            self._dataset.random_shuffle(seed=seed)
            print(
                f"DatasetReader {self._ioctx.worker_index} has {ds.count()}, samples."
            )
            # TODO: @avnishn make this call seeded.
            # calling random_shuffle_each_window shuffles the dataset after
            # each time the whole dataset has been read.
            self._iter = self._dataset.repeat().random_shuffle_each_window().iter_rows()
        else:
            self._iter = None

    @override(InputReader)
    def next(self) -> SampleBatchType:
        # next() should not get called on None DatasetReader.
        assert self._iter is not None
        ret = []
        count = 0
        while count < self.batch_size:
            d = next(self._iter).as_pydict()
            # Columns like obs are compressed when written by DatasetWriter.
            d = from_json_data(d, self._ioctx.worker)
            count += d.count
            d = self._preprocess_if_needed(d)
            d = postprocess_actions(d, self._ioctx)
            d = self._postprocess_if_needed(d)
            ret.append(d)
        ret = concat_samples(ret)
        return ret

    def _preprocess_if_needed(self, batch: SampleBatchType) -> SampleBatchType:
        # TODO: @kourosh, preprocessor is only supported for single agent case.
        if self.preprocessor:
            for key in (SampleBatch.CUR_OBS, SampleBatch.NEXT_OBS):
                if key in batch:
                    batch[key] = np.stack(
                        [self.preprocessor.transform(s) for s in batch[key]]
                    )
        return batch

    def _postprocess_if_needed(self, batch: SampleBatchType) -> SampleBatchType:
        if not self._ioctx.config.get("postprocess_inputs"):
            return batch

        if isinstance(batch, SampleBatch):
            out = []
            for sub_batch in batch.split_by_episode():
                if self._default_policy is not None:
                    out.append(self._default_policy.postprocess_trajectory(sub_batch))
                else:
                    out.append(sub_batch)
            return concat_samples(out)
        else:
            # TODO(ekl) this is trickier since the alignments between agent
            #  trajectories in the episode are not available any more.
            raise NotImplementedError(
                "Postprocessing of multi-agent data not implemented yet."
            )
