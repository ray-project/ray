import sys

import pytest

import ray

if __name__ == "__main__":
    exit_code = pytest.main(sys.argv[1:])
    if exit_code is pytest.ExitCode.NO_TESTS_COLLECTED:
        exit_code = pytest.ExitCode.OK

    ray.shutdown()

    sys.exit(exit_code)
