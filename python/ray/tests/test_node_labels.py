import os
import sys
import pytest

import ray


@pytest.mark.parametrize(
    "call_ray_start",
    ['ray start --head --labels={"gpu_type":"A100","region":"us"}'],
    indirect=True,
)
def test_ray_start_set_node_labels(call_ray_start):
    ray.init(address=call_ray_start)
    assert ray.nodes()[0]["Labels"] == {"gpu_type": "A100", "region": "us"}


@pytest.mark.parametrize(
    "call_ray_start",
    [
        "ray start --head --labels={}",
    ],
    indirect=True,
)
def test_ray_start_set_empty_node_labels(call_ray_start):
    ray.init(address=call_ray_start)
    assert ray.nodes()[0]["Labels"] == {}


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
