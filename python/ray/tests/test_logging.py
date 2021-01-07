import os

import ray
from ray.test_utils import (
    run_string_as_driver_nonblocking,
    wait_for_condition,
    wait_for_num_actors,
)


def test_log_rotation(tmp_path, shutdown_only):
    temp_folder = tmp_path / "spill"
    ray.init(num_cpus=1})



if __name__ == "__main__":
    import pytest
    import sys
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
