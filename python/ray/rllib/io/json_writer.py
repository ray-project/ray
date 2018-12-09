from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime
import json
import logging
import numpy as np
import os
from six.moves.urllib.parse import urlparse
import time

try:
    from smart_open import smart_open
except ImportError:
    smart_open = None

from ray.rllib.io.output_writer import OutputWriter
from ray.rllib.utils.annotations import override
from ray.rllib.utils.compression import pack

logger = logging.getLogger(__name__)


class JsonWriter(OutputWriter):
    """Writer object that saves experiences in JSON file chunks."""

    def __init__(self,
                 ioctx,
                 path,
                 max_file_size=64 * 1024 * 1024,
                 compress_columns=frozenset(["obs", "new_obs"])):
        """Initialize a JsonWriter.

        Arguments:
            ioctx (IOContext): current IO context object.
            path (str): a path/URI of the output directory to save files in.
        """

        self.ioctx = ioctx
        self.path = path
        self.max_file_size = max_file_size
        self.compress_columns = compress_columns
        if urlparse(path).scheme:
            self.path_is_uri = True
        else:
            # Try to create local dirs if they don't exist
            try:
                os.makedirs(path)
            except OSError:
                pass  # already exists
            assert os.path.exists(path), "Failed to create {}".format(path)
            self.path_is_uri = False
        self.file_index = 0
        self.bytes_written = 0
        self.cur_file = None

    @override(OutputWriter)
    def write(self, sample_batch):
        start = time.time()
        data = _to_json(sample_batch, self.compress_columns)
        f = self._get_file()
        f.write(data)
        f.write("\n")
        if hasattr(f, "flush"):  # legacy smart_open impls
            f.flush()
        self.bytes_written += len(data)
        logger.debug("Wrote {} bytes to {} in {}s".format(
            len(data), f,
            time.time() - start))

    def _get_file(self):
        if not self.cur_file or self.bytes_written >= self.max_file_size:
            if self.cur_file:
                self.cur_file.close()
            timestr = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            path = os.path.join(
                self.path, "output-{}_worker-{}_{}.json".format(
                    timestr, self.ioctx.worker_index, self.file_index))
            if self.path_is_uri:
                if smart_open is None:
                    raise ValueError(
                        "You must install the `smart_open` module to write "
                        "to URIs like {}".format(path))
                self.cur_file = smart_open(path, "w")
            else:
                self.cur_file = open(path, "w")
            self.file_index += 1
            self.bytes_written = 0
            logger.info("Writing to new output file {}".format(self.cur_file))
        return self.cur_file


def _to_jsonable(v, compress):
    if compress:
        return str(pack(v))
    elif isinstance(v, np.ndarray):
        return v.tolist()
    return v


def _to_json(batch, compress_columns):
    return json.dumps({
        k: _to_jsonable(v, compress=k in compress_columns)
        for k, v in batch.data.items()
    })
