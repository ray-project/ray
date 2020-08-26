import pytest

import ray
from ray import tune
from ray.tune.integration.horovod import (DistributedTrainableCreator,
                                          _train_simple)

# pytest.importorskip("horovod")


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_connect_cluster():
    try:
        address_info = ray.init(address="auto")
    except Exception as e:
        pytest.skip(str(e))
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_single_step(ray_start_2_cpus):  # noqa: F811
    trainable_cls = DistributedTrainableCreator(
        _train_simple, num_nodes=1, num_workers_per_node=2)
    trainer = trainable_cls()
    trainer.train()
    trainer.stop()


def test_step_after_completion(ray_start_2_cpus):  # noqa: F811
    trainable_cls = DistributedTrainableCreator(
        _train_simple, num_nodes=1, num_workers_per_node=2)
    trainer = trainable_cls(config={"epochs": 1})
    with pytest.raises(RuntimeError):
        for i in range(10):
            trainer.train()


def test_validation(ray_start_2_cpus):  # noqa: F811
    def bad_func(a, b, c):
        return 1

    with pytest.raises(ValueError):
        DistributedTrainableCreator(bad_func, num_workers_per_node=2)


def test_set_global(ray_start_2_cpus):  # noqa: F811
    trainable_cls = DistributedTrainableCreator(
        _train_simple, num_workers_per_node=2)
    trainable = trainable_cls()
    result = trainable.train()
    trainable.stop()
    assert result["rank"] == 0


def test_simple_tune(ray_start_4_cpus):
    trainable_cls = DistributedTrainableCreator(
        _train_simple, num_workers_per_node=2)
    analysis = tune.run(
        trainable_cls, num_samples=2, stop={"training_iteration": 2})
    assert analysis.trials[0].last_result["training_iteration"] == 2


def test_coordinator_registration():
    from ray.tune.integration.horovod import Coordinator, MiniSettings
    settings = MiniSettings()
    coord = Coordinator(settings)
    assert coord.world_size == 0
    assert coord.hoststring == ""
    ranks = list(range(12))

    for i, hostname in enumerate(["a", "b", "c"]):
        for r in ranks:
            if r % 3 == i:
                coord.register(hostname, world_rank=r)

    rank_to_info = coord.finalize_registration()
    assert len(rank_to_info) == len(ranks)
    assert all(info["node_world_size"] == 3 for info in rank_to_info.values())
    assert {info["node_world_rank"]
            for info in rank_to_info.values()} == {0, 1, 2}
    assert all(info["local_size"] == 4 for info in rank_to_info.values())
    assert {info["local_rank"]
            for info in rank_to_info.values()} == {0, 1, 2, 3}


def test_colocator():
    pass


def test_colocator_gpu():
    pass


def test_horovod_mixin():
    pass


@pytest.mark.parametrize("use_gpu", [True, False])
def test_resource_tune(ray_connect_cluster, use_gpu):
    if use_gpu and not ray.cluster_resources().get("GPU", 0):
        pytest.skip("No GPU available.")
    trainable_cls = DistributedTrainableCreator(
        _train_simple, num_workers_per_node=2, use_gpu=use_gpu)
    analysis = tune.run(
        trainable_cls, num_samples=2, stop={"training_iteration": 2})
    assert analysis.trials[0].last_result["training_iteration"] == 2


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
