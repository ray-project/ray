import gzip
import json
import sys
from pathlib import Path

import pytest

from ray_release.util import read_json, write_json


@pytest.mark.parametrize("file_format", (".json", ".gz", ".gz_bad_extension"))
def test_read_json(tmpdir, file_format):
    file = Path(tmpdir) / f"file{file_format}"
    data = {"test": "data"}

    if file_format == ".json":
        with open(file, "wt") as f:
            json.dump(data, f)
    else:
        with gzip.open(file, "w") as f:
            f.write(json.dumps(data).encode("utf-8"))

    loaded_data = read_json(file)
    assert data == loaded_data


@pytest.mark.parametrize("file_format", (".json", ".gz"))
def test_write_json(tmpdir, file_format):
    file = Path(tmpdir) / f"file{file_format}"
    data = {"test": "data"}
    path = write_json(data, file)

    if file_format == ".json":
        with open(path, "rt") as f:
            loaded_data = json.load(f)
    else:
        with gzip.open(path, "r") as f:
            loaded_data = json.loads(f.read().decode("utf-8"))

    assert data == loaded_data


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
