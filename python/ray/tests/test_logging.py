import os

import ray


def test_log_rotation(tmp_path, shutdown_only):
    temp_folder = tmp_path / "spill"
    print(temp_folder)
    ray.init(num_cpus=1)


if __name__ == "__main__":
    import pytest
    import sys
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
