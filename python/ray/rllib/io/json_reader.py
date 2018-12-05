from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime
import glob
import json
import logging
import numpy as np
import os
import random
from six.moves.urllib.parse import urlparse

try:
    from smart_open import smart_open
except ImportError:
    smart_open = None

from ray.rllib.io.input_reader import InputReader
from ray.rllib.evaluation.sample_batch import SampleBatch
from ray.rllib.utils.compression import unpack

logger = logging.getLogger(__name__)


class JsonReader(InputReader):
    """Reader object that loads experiences from JSON file chunks.
    
    The input files will be read from in an random order."""

    def __init__(self, ioctx, inputs):
        """Initialize a JsonReader.

        Arguments:
            ioctx (IOContext): current IO context object.
            inputs (str|list): either a glob expression for files, e.g.,
                "/tmp/**/*.json", or a list of single file paths or URIs, e.g.,
                ["hdfs:/dir/file.json", "hdfs:/dir/file2.json"].
        """

        self.ioctx = ioctx
        if type(inputs) is str:
            self.files = glob.glob(inputs)
        elif type(inputs) is list:
            self.files = inputs
        else:
            raise ValueError(
                "type of inputs must be list or str, not {}".format(inputs))
        if self.files:
            logger.info("Found {} input files.".format(len(self.files)))
        else:
            raise ValueError("No files found matching {}".format(inputs))
        self.cur_file = None

    def next(self):
        line = self._next_line().strip()
        tries = 0
        while not line and tries < 100:
            tries += 1
            logger.debug("Skipping empty line in {}".format(self.cur_file))
            line = self._next_line().strip()
        if not line:
            raise ValueError(
                "Failed to read non-empty line from file: {}".format(
                    self.cur_file))
        return _from_json(line)

    def _next_line(self):
        if not self.cur_file:
            self.cur_file = self._next_file()
        line = self.cur_file.readline()
        tries = 0
        while not line and tries < 100:
            tries += 1
            self.cur_file.close()
            self.cur_file = self._next_file()
            line = self.cur_file.readline()
            if not line:
                logger.debug("Ignoring empty file {}".format(self.cur_file))
        if not line:
            raise ValueError(
                "Failed to read next line from files: {}".format(self.files))
        return line

    def _next_file(self):
        path = random.choice(self.files)
        if urlparse(path).scheme:
            if smart_open is None:
                raise ValueError(
                    "You must install the `smart_open` module to read "
                    "from URIs like {}".format(path))
            return smart_open(path, "r")
        else:
            return open(path, "r")


def _from_json(batch):
    data = json.loads(batch)
    for k, v in data.items():
        if type(v) is str:
            data[k] = unpack(v)
        else:
            data[k] = np.array(v)
    return SampleBatch(data)
