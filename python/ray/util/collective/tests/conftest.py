"""Some fixtures for collective tests."""
import pytest

import ray


@pytest.fixture
def ray_start_single_node_2_gpus():
    # Please start this fixture in a cluster with 2 GPUs.
    address_info = ray.init(num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


# TODO (Hao): implement this one.
@pytest.fixture
def ray_start_distributed_2_nodes_4_gpus():
    pass
