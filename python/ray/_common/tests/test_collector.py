import ray
import pytest
from ray._common.test_utils import Collector


def test_collector_class():
    collector = Collector.remote()

    random_items = ["this", "is", 1, "demo", "string"]

    for item in random_items:
        collector.add.remote(item)

    result = ray.get(collector.get.remote())

    assert len(result) == len(random_items)

    for i in range(0, len(result)):
        assert result[i] == random_items[i]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
