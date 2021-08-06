from contextlib import contextmanager

import os
import tempfile


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
