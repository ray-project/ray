import ray
import pytest
import sys

from ray.experimental import shuffle


def test_shuffle():
    try:
        shuffle.main()
    finally:
        ray.shutdown()


# https://github.com/ray-project/ray/pull/16408
@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_shuffle_hang():
    try:
        shuffle.main(
            object_store_memory=1e9, num_partitions=200, partition_size=10e6)
    finally:
        ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
