from datetime import datetime
import json
import logging
import numpy as np
import os
from urllib.parse import urlparse
import time

try:
    from smart_open import smart_open
except ImportError:
    smart_open = None

from ray.air._internal.json import SafeFallbackEncoder
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.offline.io_context import IOContext
from ray.rllib.offline.output_writer import OutputWriter
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.compression import pack, compression_supported
from ray.rllib.utils.typing import FileType, SampleBatchType
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

WINDOWS_DRIVES = [chr(i) for i in range(ord("c"), ord("z") + 1)]


# TODO(jungong): use DatasetWriter to back JsonWriter, so we reduce codebase complexity
#  without losing existing functionality.
@PublicAPI
class JsonWriter(OutputWriter):
    """Writer object that saves experiences in JSON file chunks."""

    @PublicAPI
    def __init__(
        self,
        path: str,
        ioctx: IOContext = None,
        max_file_size: int = 64 * 1024 * 1024,
        compress_columns: List[str] = frozenset(["obs", "new_obs"]),
    ):
        """Initializes a JsonWriter instance.

        Args:
            path: a path/URI of the output directory to save files in.
            ioctx: current IO context object.
            max_file_size: max size of single files before rolling over.
            compress_columns: list of sample batch columns to compress.
        """
        logger.info(
            "You are using JSONWriter. It is recommended to use "
            + "DatasetWriter instead."
        )

        self.ioctx = ioctx or IOContext()
        self.max_file_size = max_file_size
        self.compress_columns = compress_columns
        if urlparse(path).scheme not in [""] + WINDOWS_DRIVES:
            self.path_is_uri = True
        else:
            path = os.path.abspath(os.path.expanduser(path))
            # Try to create local dirs if they don't exist
            os.makedirs(path, exist_ok=True)
            assert os.path.exists(path), "Failed to create {}".format(path)
            self.path_is_uri = False
        self.path = path
        self.file_index = 0
        self.bytes_written = 0
        self.cur_file = None

    @override(OutputWriter)
    def write(self, sample_batch: SampleBatchType):
        start = time.time()
        data = _to_json(sample_batch, self.compress_columns)
        f = self._get_file()
        f.write(data)
        f.write("\n")
        if hasattr(f, "flush"):  # legacy smart_open impls
            f.flush()
        self.bytes_written += len(data)
        logger.debug(
            "Wrote {} bytes to {} in {}s".format(len(data), f, time.time() - start)
        )

    def _get_file(self) -> FileType:
        if not self.cur_file or self.bytes_written >= self.max_file_size:
            if self.cur_file:
                self.cur_file.close()
            timestr = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            path = os.path.join(
                self.path,
                "output-{}_worker-{}_{}.json".format(
                    timestr, self.ioctx.worker_index, self.file_index
                ),
            )
            if self.path_is_uri:
                if smart_open is None:
                    raise ValueError(
                        "You must install the `smart_open` module to write "
                        "to URIs like {}".format(path)
                    )
                self.cur_file = smart_open(path, "w")
            else:
                self.cur_file = open(path, "w")
            self.file_index += 1
            self.bytes_written = 0
            logger.info("Writing to new output file {}".format(self.cur_file))
        return self.cur_file


def _to_jsonable(v, compress: bool) -> Any:
    if compress and compression_supported():
        return str(pack(v))
    elif isinstance(v, np.ndarray):
        return v.tolist()

    return v


def _to_json_dict(batch: SampleBatchType, compress_columns: List[str]) -> Dict:
    out = {}
    if isinstance(batch, MultiAgentBatch):
        out["type"] = "MultiAgentBatch"
        out["count"] = batch.count
        policy_batches = {}
        for policy_id, sub_batch in batch.policy_batches.items():
            policy_batches[policy_id] = {}
            for k, v in sub_batch.items():
                policy_batches[policy_id][k] = _to_jsonable(
                    v, compress=k in compress_columns
                )
        out["policy_batches"] = policy_batches
    else:
        out["type"] = "SampleBatch"
        for k, v in batch.items():
            out[k] = _to_jsonable(v, compress=k in compress_columns)
    return out


def _to_json(batch: SampleBatchType, compress_columns: List[str]) -> str:
    out = _to_json_dict(batch, compress_columns)
    return json.dumps(out, cls=SafeFallbackEncoder)
