from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.io.output_writer import OutputWriter


class JsonWriter(OutputWriter):
    """Writer object that saves experiences in JSON file chunks."""

    def __init__(self, ioctx, path, max_file_size=64000000):
        self.ioctx = ioctx
        self.path = path
        self.max_file_size = max_file_size
        try:
            os.makedirs(path)
        except OSError:
            pass  # already exists
        assert os.path.exists(path), "Failed to create {}".format(path)

    def write(self, sample_batch):
        print("Writing to", self.path)
        print(sample_batch)
