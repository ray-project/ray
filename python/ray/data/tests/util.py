from contextlib import contextmanager

import os
import tempfile
import ray

STRICT_MODE = ray.data.DatasetContext.get_current().strict_mode


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


def column_udf(col, udf):
    def wraps(row):
        return {col: udf(row[col])}

    return wraps


# Ex: named_values("id", [1, 2, 3])
# Ex: named_values(["id", "id2"], [(1, 1), (2, 2), (3, 3)])
def named_values(col_names, tuples):
    output = []
    if isinstance(col_names, list):
        for t in tuples:
            output.append({name: value for (name, value) in zip(col_names, t)})
    else:
        for t in tuples:
            output.append({name: value for (name, value) in zip((col_names,), (t,))})
    return output


def extract_values(col_name, tuples):
    return [t[col_name] for t in tuples]
