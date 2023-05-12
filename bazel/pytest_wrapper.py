import sys

import pytest

import ray

if __name__ == "__main__":
    exit_code = pytest.main(sys.argv[1:])
    ray.shutdown()
    sys.exit(exit_code)
