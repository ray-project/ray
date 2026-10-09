import sys

import pytest

from ray._common.test_utils import run_string_as_driver


def test_dynamically_imported():
    script = """
import sys

assert "ray" not in sys.modules
import ray
assert "ray" in sys.modules

# `ray.data` shouldn't be imported when `ray` is imported.
assert "ray.data" not in sys.modules
ray.data
# `ray.data` should be cached on import.
assert "ray.data" in sys.modules
"""
    run_string_as_driver(script)


def test_import_does_not_load_pyarrow_dataset():
    script = """
import sys

import ray.data

# `pyarrow.dataset` loads several native extensions, so `import ray.data`
# should defer it until a read needs it.
loaded = sorted(m for m in sys.modules if m.startswith("pyarrow._dataset"))
assert "pyarrow.dataset" not in sys.modules, loaded
assert not loaded, loaded
"""
    run_string_as_driver(script)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
