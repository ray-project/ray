import os
import sys

import pytest

import ray
from ray.experimental.batch_remote import batch_remote


@ray.remote
class Counter:
    def __init__(self, value):
        self.value = value

    def increase(self):
        self.value += 1
        return self.value

    def get_value(self):
        return self.value

    def reset(self):
        self.value = 0


def test_basic_batch_remote(ray_start_regular_shared):
    num_actors = 10
    # Create multiple actors.
    actors = [Counter.remote(i) for i in range(num_actors)]
    # print(actors[0].get_value.remote())
    object_refs = batch_remote(actors).get_value.remote()
    print(ray.get(object_refs))


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
