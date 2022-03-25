import os
import pytest
import sys

import ray


@pytest.mark.parametrize(
    "call_ray_start_with_external_redis",
    [
        "6379",
        "6379,6380",
        "6379,6380,6381",
    ],
    indirect=True,
)
def test_using_hostnames(call_ray_start_with_external_redis):
    ray.init(address="127.0.0.1:6379", _redis_password="123")

    @ray.remote
    def f():
        return 1

    assert ray.get(f.remote()) == 1

    @ray.remote
    class Counter:
        def __init__(self):
            self.count = 0

        def inc_and_get(self):
            self.count += 1
            return self.count

    counter = Counter.remote()
    assert ray.get(counter.inc_and_get.remote()) == 1


if __name__ == "__main__":
    import pytest

    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
