import logging
import math

import ray.data
from ray.rllib.offline.input_reader import InputReader
from ray.rllib.offline.io_context import IOContext
from ray.rllib.offline.json_reader import from_json_data
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.typing import SampleBatchType, AlgorithmConfigDict
from typing import List

logger = logging.getLogger(__name__)

DEFAULT_NUM_CPUS_PER_TASK = 0.5


def _get_resource_bundles(config: AlgorithmConfigDict):
    input_config = config.get("input_config", {})
    parallelism = input_config.get("parallelism", config.get("num_workers", 1))
    cpus_per_task = input_config.get(
        "num_cpus_per_read_task", DEFAULT_NUM_CPUS_PER_TASK
    )
    return [{"CPU": math.ceil(parallelism * cpus_per_task)}]


@PublicAPI
def get_dataset_and_shards(
    config: AlgorithmConfigDict, num_workers: int, local_worker: bool
) -> (ray.data.dataset.Dataset, List[ray.data.dataset.Dataset]):
    assert config["input"] == "dataset"
    assert (
        "input_config" in config
    ), "Must specify input_config dict if using Dataset input."

    input_config = config["input_config"]

    format = input_config.get("format")
    path = input_config.get("path")
    loader_fn = input_config.get("loader_fn")

    if loader_fn and (format or path):
        raise ValueError(
            "When using a `loader_fn`, you cannot specify a `format` or `path`."
        )

    if not (format and path) and not loader_fn:
        raise ValueError(
            "Must specify format and path, or a loader_fn via input_config key"
            " when using Ray dataset input."
        )

    parallelism = input_config.get("parallelism", num_workers or 1)
    cpus_per_task = input_config.get(
        "num_cpus_per_read_task", DEFAULT_NUM_CPUS_PER_TASK
    )

    assert loader_fn or (format and path)

    if loader_fn:
        dataset = loader_fn()
    elif format == "json":
        dataset = ray.data.read_json(
            path, parallelism=parallelism, ray_remote_args={"num_cpus": cpus_per_task}
        )
    elif format == "parquet":
        dataset = ray.data.read_parquet(
            path, parallelism=parallelism, ray_remote_args={"num_cpus": cpus_per_task}
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
                "path": "/tmp/sample_batches/",
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
        self._dataset = ds
        # We allow the creation of a non-functioning None DatasetReader.
        # It's useful for example for a non-rollout local worker.
        if ds:
            print(
                "DatasetReader ", ioctx.worker_index, " has ", ds.count(), " samples."
            )
            self._iter = self._dataset.repeat().iter_rows()
        else:
            self._iter = None

    @override(InputReader)
    def next(self) -> SampleBatchType:
        # next() should not get called on None DatasetReader.
        assert self._iter is not None

        d = next(self._iter).as_pydict()
        # Columns like obs are compressed when written by DatasetWriter.
        d = from_json_data(d, self._ioctx.worker)

        return d
