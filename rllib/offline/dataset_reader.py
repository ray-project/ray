import logging
import math
import re
import zipfile
from pathlib import Path

import ray.data
from ray.rllib.offline.input_reader import InputReader
from ray.rllib.offline.io_context import IOContext
from ray.rllib.offline.json_reader import from_json_data
from ray.rllib.policy.sample_batch import concat_samples, SampleBatch, DEFAULT_POLICY_ID
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.typing import SampleBatchType, AlgorithmConfigDict
from typing import List, Tuple

logger = logging.getLogger(__name__)

DEFAULT_NUM_CPUS_PER_TASK = 0.5


def _get_resource_bundles(config: AlgorithmConfigDict):
    input_config = config.get("input_config", {})
    parallelism = input_config.get("parallelism", config.get("num_workers", 1))
    cpus_per_task = input_config.get(
        "num_cpus_per_read_task", DEFAULT_NUM_CPUS_PER_TASK
    )
    return [{"CPU": math.ceil(parallelism * cpus_per_task)}]


def _unzip_if_needed(paths: List[str], format: str):
    """If a path in paths is a zip file, unzip it and use path of the unzipped file"""
    ret = []
    for path in paths:
        fpath = Path(path).absolute()
        if not fpath.exists():
            fpath = Path(__file__).parent.parent / path
        if not fpath.exists():
            raise FileNotFoundError(f"File not found: {path}")
        if re.search("\\.zip$", str(fpath)):
            with zipfile.ZipFile(str(fpath), "r") as zip_ref:
                zip_ref.extractall(str(fpath.parent))
            fpath = re.sub("\\.zip$", f".{format}", str(fpath))
        fpath = str(fpath)
        ret.append(fpath)
    return ret


@PublicAPI
def get_dataset_and_shards(
    config: AlgorithmConfigDict, num_workers: int, local_worker: bool
) -> Tuple[ray.data.dataset.Dataset, List[ray.data.dataset.Dataset]]:
    assert config["input"] == "dataset", (
        "Must specify input as dataset if" " calling `get_dataset_and_shards`"
    )
    assert (
        "input_config" in config
    ), "Must specify input_config dict if using Dataset input."

    input_config = config["input_config"]

    format = input_config.get("format")
    assert format in ("json", "parquet"), (
        "Offline input data format must be " "parquet " "or json"
    )
    paths = input_config.get("paths")
    loader_fn = input_config.get("loader_fn")
    if loader_fn and (format or paths):
        raise ValueError(
            "When using a `loader_fn`, you cannot specify a `format` or `path`."
        )

    if not (format and paths) and not loader_fn:
        raise ValueError(
            "Must specify format and path, or a loader_fn via input_config key"
            " when using Ray dataset input."
        )

    if not isinstance(paths, (list, str)):
        raise ValueError("Paths must be a list of path strings or a path string")
    if isinstance(paths, str):
        paths = [paths]
    paths = _unzip_if_needed(paths, format)

    parallelism = input_config.get("parallelism", num_workers or 1)
    cpus_per_task = input_config.get(
        "num_cpus_per_read_task", DEFAULT_NUM_CPUS_PER_TASK
    )

    assert loader_fn or (format and paths), (
        f"If using a loader_fn: {loader_fn} that constructs a dataset, "
        "format: {format} and paths: {paths} must be specified. If format and "
        "paths are specified, a loader_fn must not be specified."
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
    if local_worker and num_workers == 0:
        # Dataset is the only shard we need.
        return dataset, [dataset]
    # Remote workers are responsible for sampling:
    else:
        # Each remote worker gets 1 shard.
        # The first None shard is for the local worker, which
        # shouldn't be doing rollout work anyways.
        return dataset, [None] + dataset.repartition(
            num_blocks=num_workers, shuffle=False
        ).split(num_workers)


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
    def __init__(self, ioctx: IOContext, ds: ray.data.Dataset):
        """Initializes a DatasetReader instance.

        Args:
            ds: Ray dataset to sample from.
        """
        self._ioctx = ioctx
        self._default_policy = self.policy_map = None
        self._dataset = ds
        self.count = None if not self._dataset else self._dataset.count()
        # do this to disable the ray data stdout logging
        ray.data.set_progress_bars(enabled=False)

        # the number of rows to return per call to next()
        if self._ioctx:
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
            ret.append(self._postprocess_if_needed(d))
        ret = concat_samples(ret)
        return ret

    def _postprocess_if_needed(self, batch: SampleBatchType) -> SampleBatchType:
        if not self._ioctx or not self._ioctx.config.get("postprocess_inputs"):
            return batch

        if isinstance(batch, SampleBatch):
            out = []
            for sub_batch in batch.split_by_episode():
                out.append(self._default_policy.postprocess_trajectory(sub_batch))
            return SampleBatch.concat_samples(out)
        else:
            # TODO(ekl) this is trickier since the alignments between agent
            #  trajectories in the episode are not available any more.
            raise NotImplementedError(
                "Postprocessing of multi-agent data not implemented yet."
            )
