from contextlib import contextmanager

import os
import tempfile
import ray


@ray.remote
class Counter:
    def __init__(self):
        self.count = 0

    def increment(self):
        self.count += 1

    def get(self):
        return self.count

    def reset(self):
        self.count = 0


@contextmanager
def gen_bin_files(n):
    with tempfile.TemporaryDirectory() as temp_dir:
        paths = []
        for i in range(n):
            path = os.path.join(temp_dir, f"{i}.bin")
            paths.append(path)
            fp = open(path, "wb")
            to_write = str(i) * 500
            fp.write(to_write.encode())
        yield (temp_dir, paths)
