from unittest.mock import patch

import pytest
import ray
import torch.nn as nn
from ray import tune
from ray.cluster_utils import Cluster
from ray.tune.utils import merge_dicts
from ray.util.sgd.torch import TorchTrainer
from ray.util.sgd.torch.examples.train_example import (
    model_creator,
    optimizer_creator,
    data_creator,
)
from ray.util.sgd.torch.training_operator import TrainingOperator

Operator = TrainingOperator.from_creators(
    model_creator, optimizer_creator, data_creator, loss_creator=nn.MSELoss
)


@pytest.fixture
def ray_4_node_1_cpu():
    cluster = Cluster()
    for _ in range(4):
        cluster.add_node(num_cpus=1)

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_8_node_2_cpu():
    cluster = Cluster()
    for _ in range(8):
        cluster.add_node(num_cpus=2)

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_4_node_8_cpu():
    cluster = Cluster()
    for _ in range(4):
        cluster.add_node(num_cpus=8)

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


def test_train_spread(ray_8_node_2_cpu):
    """Tests if workers are spread across nodes."""
    assert ray.available_resources()["CPU"] == 16
    trainer = TorchTrainer(
        training_operator_cls=Operator,
        num_workers=7,
        use_gpu=False,
    )

    assert ray.available_resources()["CPU"] == 9

    node_id_set = set()
    for actor_info in ray.state.actors().values():
        node_id = actor_info["Address"]["NodeID"]
        node_id_set.add(node_id)
    assert len(node_id_set) == 7

    trainer.shutdown()
    assert ray.available_resources()["CPU"] == 16


@pytest.mark.parametrize("num_workers", [1, 7, 8, 15])
def test_tune_train_pack(ray_4_node_8_cpu, num_workers):
    """Tests if workers are colocated when running Tune."""

    def custom_train_func(trainer, info):
        train_stats = trainer.train(profile=True)
        val_stats = trainer.validate(profile=True)
        stats = merge_dicts(train_stats, val_stats)

        actors = ray.state.actors().values()
        assert len(actors) == num_workers + 1

        node_id_set = set()
        for actor_info in actors:
            node_id = actor_info["Address"]["NodeID"]
            node_id_set.add(node_id)

        assert len(node_id_set) == 1 + num_workers // 8
        return stats

    TorchTrainable = TorchTrainer.as_trainable(
        override_tune_step=custom_train_func,
        **{
            "training_operator_cls": Operator,
            "num_workers": num_workers,
            "use_gpu": False,
            "backend": "gloo",
            "config": {"batch_size": 512, "lr": 0.001},
        }
    )

    tune.run(TorchTrainable, num_samples=1, stop={"training_iteration": 2}, verbose=1)


def test_shutdown(ray_8_node_2_cpu):
    """Tests if placement group is removed when worker group is shut down."""
    assert ray.available_resources()["CPU"] == 16
    placement_group_table = ray.state.state.placement_group_table()
    assert len(placement_group_table) == 0

    trainer = TorchTrainer(
        training_operator_cls=Operator,
        num_workers=7,
        use_gpu=False,
    )
    assert ray.available_resources()["CPU"] == 9
    placement_group_table = ray.state.state.placement_group_table()
    assert len(placement_group_table) == 1
    placement_group_id = list(placement_group_table)[0]
    placement_group = placement_group_table[placement_group_id]
    assert placement_group["strategy"] == "SPREAD"
    assert placement_group["state"] == "CREATED"

    trainer.shutdown()

    assert ray.available_resources()["CPU"] == 16
    placement_group_table = ray.state.state.placement_group_table()
    assert len(placement_group_table) == 1
    placement_group = placement_group_table[placement_group_id]
    assert placement_group["strategy"] == "SPREAD"
    assert placement_group["state"] == "REMOVED"


def test_resize(ray_8_node_2_cpu):
    """Tests if placement group is removed when trainer is resized."""
    assert ray.available_resources()["CPU"] == 16
    placement_group_table = ray.state.state.placement_group_table()
    assert len(placement_group_table) == 0

    trainer = TorchTrainer(
        training_operator_cls=Operator,
        num_workers=7,
        use_gpu=False,
    )

    assert ray.available_resources()["CPU"] == 9
    placement_group_table = ray.state.state.placement_group_table()
    assert len(placement_group_table) == 1
    placement_group_id = list(placement_group_table)[0]
    placement_group = placement_group_table[placement_group_id]
    assert placement_group["state"] == "CREATED"

    trainer._resize_worker_group(trainer.state_dict())

    assert ray.available_resources()["CPU"] == 9
    placement_group_table = ray.state.state.placement_group_table()
    assert len(placement_group_table) == 2
    placement_group = placement_group_table[placement_group_id]
    assert placement_group["state"] == "REMOVED"
    placement_group_table_keys = list(placement_group_table)
    placement_group_table_keys.remove(placement_group_id)
    second_placement_group_id = placement_group_table_keys[0]
    second_placement_group = placement_group_table[second_placement_group_id]
    assert second_placement_group["state"] == "CREATED"

    trainer.shutdown()

    assert ray.available_resources()["CPU"] == 16
    placement_group_table = ray.state.state.placement_group_table()
    assert len(placement_group_table) == 2
    placement_group = placement_group_table[placement_group_id]
    assert placement_group["state"] == "REMOVED"
    second_placement_group = placement_group_table[second_placement_group_id]
    assert second_placement_group["state"] == "REMOVED"


@patch("ray.util.sgd.torch.worker_group.SGD_PLACEMENT_GROUP_TIMEOUT_S", 5)
def test_timeout(ray_4_node_1_cpu):
    """Tests that an error is thrown when placement group setup times out."""
    with pytest.raises(TimeoutError):
        trainer = TorchTrainer(
            training_operator_cls=Operator, num_workers=7, use_gpu=False
        )
        trainer.shutdown()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
