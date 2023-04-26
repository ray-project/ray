import logging
import os
import time

import ray
from ray.rllib.offline.io_context import IOContext
from ray.rllib.offline.json_writer import _to_json_dict
from ray.rllib.offline.output_writer import OutputWriter
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.utils.io import is_remote_path
from typing import Dict, List

logger = logging.getLogger(__name__)


@PublicAPI
class DatasetWriter(OutputWriter):
    """Writer object that saves experiences using Datasets."""

    @PublicAPI
    def __init__(
        self,
        ioctx: IOContext = None,
        compress_columns: List[str] = frozenset(["obs", "new_obs"]),
    ):
        """Initializes a DatasetWriter instance.

        Examples:
        config = {
            "output": "dataset",
            "output_config": {
                "format": "json",
                "path": "/tmp/test_samples/",
                "max_num_samples_per_file": 100000,
            }
        }

        Args:
            ioctx: current IO context object.
            compress_columns: list of sample batch columns to compress.
        """
        self.ioctx = ioctx or IOContext()

        output_config: Dict = ioctx.output_config
        assert (
            "format" in output_config
        ), "output_config.format must be specified when using Dataset output."
        assert (
            "path" in output_config
        ), "output_config.path must be specified when using Dataset output."

        self.format = output_config["format"]

        output_path = output_config["path"]
        if is_remote_path(output_path):
            self.path = output_path
        else:
            self.path = os.path.abspath(os.path.expanduser(output_config["path"]))
        self.max_num_samples_per_file = (
            output_config["max_num_samples_per_file"]
            if "max_num_samples_per_file" in output_config
            else 100000
        )
        self.compress_columns = compress_columns

        self.samples = []

    @override(OutputWriter)
    def write(self, sample_batch: SampleBatchType):
        start = time.time()

        # Make sure columns like obs are compressed and writable.
        d = _to_json_dict(sample_batch, self.compress_columns)
        self.samples.append(d)

        # Todo: We should flush at the end of sampling even if this
        # condition was not reached.
        if len(self.samples) >= self.max_num_samples_per_file:
            self._write_samples_to_file()
            logger.debug("Wrote dataset in {}s".format(time.time() - start))

    def close(self):
        if self.samples:
            self._write_samples_to_file()

    def _write_samples_to_file(self):
        ds = ray.data.from_items(self.samples, output_arrow_format=True).repartition(
            num_blocks=1, shuffle=False
        )
        if self.format == "json":
            ds.write_json(self.path, try_create_dir=True)
        elif self.format == "parquet":
            ds.write_parquet(self.path, try_create_dir=True)
        else:
            raise ValueError("Unknown output type: ", self.format)
        self.samples = []


    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

