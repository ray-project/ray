# coding: utf-8
import os
import sys
import pytest
import torch

import ray
import ray.experimental.collective as collective
from ray.dag import InputNode, MultiOutputNode
from ray.tests.conftest import *  # noqa

USE_GPU = bool(os.environ.get("RAY_PYTEST_USE_GPU", 0))

logger = logging.getLogger(__name__)


@ray.remote(num_gpus=1)
class Actor:
    def __init__(self):
        return

    def compute(self, _):
        return torch.ones(1, 1)

    def update(self, _):
        return


@pytest.mark.skipif(not USE_GPU, reason="Skipping GPU Test")
@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 4}], indirect=True)
@pytest.mark.parametrize("num_gpus", [2])
def test_simple(ray_start_regular, num_gpus):
    actors = [Actor.remote() for _ in range(num_gpus)]

    upds = []
    with InputNode() as inp:
        grads = [actor.compute.bind(inp) for actor in actors]
        grads_reduced = collective.allreduce.bind(grads)
        upds.extend(
            [actor.update.bind(grad) for actor, grad in zip(actors, grads_reduced)]
        )
        grads = [actor.compute.bind(grad) for actor, grad in zip(actors, grads)]
        grads_reduced = collective.allreduce.bind(grads)
        upds.extend(
            [actor.update.bind(grad) for actor, grad in zip(actors, grads_reduced)]
        )
        dag = MultiOutputNode(upds)

    compiled_dag = dag.experimental_compile()
    print(compiled_dag.actor_to_execution_schedule)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
