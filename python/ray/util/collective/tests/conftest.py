"""Some fixtures for collective tests."""
import logging

import pytest
import ray
from ray.util.collective.collective_group.nccl_collective_group import (
    _get_comm_key_from_devices,
    _get_comm_key_send_recv,
)
from ray.util.collective.const import get_store_name

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


# TODO (Hao): remove this clean_up function as it sometimes crashes Ray.
def clean_up():
    group_names = ["default", "test", "123?34!", "default2", "random"]
    group_names.extend([str(i) for i in range(10)])
    max_world_size = 4
    all_keys = []
    for name in group_names:
        devices = [[0], [0, 1], [1, 0]]
        for d in devices:
            collective_communicator_key = _get_comm_key_from_devices(d)
            all_keys.append(collective_communicator_key + "@" + name)
        for i in range(max_world_size):
            for j in range(max_world_size):
                if i < j:
                    p2p_communicator_key = _get_comm_key_send_recv(i, 0, j, 0)
                    all_keys.append(p2p_communicator_key + "@" + name)
    for group_key in all_keys:
        store_name = get_store_name(group_key)
        try:
            actor = ray.get_actor(store_name)
        except ValueError:
            actor = None
        if actor:
            logger.debug(
                "Killing actor with group_key: '{}' and store: '{}'.".format(
                    group_key, store_name
                )
            )
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
    # The cluster has a setup of 2 nodes, each node with 2
    # GPUs. Each actor will be allocated 1 GPU.
    ray.init("auto")
    yield
    clean_up()
    ray.shutdown()


@pytest.fixture
def ray_start_distributed_multigpu_2_nodes_4_gpus():
    # The cluster has a setup of 2 nodes, each node with 2
    # GPUs. Each actor will be allocated 2 GPUs.
    ray.init("auto")
    yield
    clean_up()
    ray.shutdown()


@pytest.fixture
def ray_start_single_node():
    address_info = ray.init(num_cpus=8)
    yield address_info
    ray.shutdown()


@pytest.fixture
def ray_start_distributed_2_nodes():
    # The cluster has a setup of 2 nodes.
    # no GPUs!
    ray.init("auto")
    yield
    ray.shutdown()
