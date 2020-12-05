"""Some fixtures for collective tests."""
import pytest

import ray
from ray.util.collective.const import get_nccl_store_name


def clean_up():
    group_names = ["default", "test", "123?34!", "default2", "random"]
    group_names.extend([str(i) for i in range(10)])
    for group_name in group_names:
        try:
            store_name = get_nccl_store_name(group_name)
            actor = ray.get_actor(store_name)
        except ValueError:
            actor = None
        if actor:
            ray.kill(actor)


@pytest.fixture
def ray_start_single_node_2_gpus():
    # Please start this fixture in a cluster with 2 GPUs.
    address_info = ray.init(num_gpus=2)
    yield address_info
    ray.shutdown()


# Hao: this fixture is a bit tricky.
# I use a bash script to start a ray cluster on
# my own on-premise cluster before run this fixture.
@pytest.fixture
def ray_start_distributed_2_nodes_4_gpus():
    ray.init("auto")
    yield
    clean_up()
    ray.shutdown()
