import os
import pytest

import ray
from ray import tune
from ray.tune.integration.horovod import (DistributedTrainableCreator,
                                          _train_simple)
from ray.tune.integration._horovod_job import NodeColocator

try:
    import horovod
except ImportError:
    horovod = None
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
def ray_start_6_cpus():
    address_info = ray.init(num_cpus=6)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_4_cpus_4_gpus():
    os.environ["CUDA_VISIBLE_DEVICES"] = "0,1,2,3"
    address_info = ray.init(num_cpus=4, num_gpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
    del os.environ["CUDA_VISIBLE_DEVICES"]


@pytest.fixture
def ray_connect_cluster():
    try:
        address_info = ray.init(address="auto")
    except Exception as e:
        pytest.skip(str(e))
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.mark.skipif(horovod is None, reason="Horovod not found.")
def test_single_step(ray_start_2_cpus):  # noqa: F811
    trainable_cls = DistributedTrainableCreator(
        _train_simple, num_nodes=1, num_workers_per_node=2)
    trainer = trainable_cls()
    trainer.train()
    trainer.stop()


@pytest.mark.skipif(horovod is None, reason="Horovod not found.")
def test_step_after_completion(ray_start_2_cpus):  # noqa: F811
    trainable_cls = DistributedTrainableCreator(
        _train_simple, num_nodes=1, num_workers_per_node=2)
    trainer = trainable_cls(config={"epochs": 1})
    with pytest.raises(RuntimeError):
        for i in range(10):
            trainer.train()


@pytest.mark.skipif(horovod is None, reason="Horovod not found.")
def test_validation(ray_start_2_cpus):  # noqa: F811
    def bad_func(a, b, c):
        return 1

    with pytest.raises(ValueError):
        DistributedTrainableCreator(bad_func, num_workers_per_node=2)


@pytest.mark.skipif(horovod is None, reason="Horovod not found.")
def test_set_global(ray_start_2_cpus):  # noqa: F811
    trainable_cls = DistributedTrainableCreator(
        _train_simple, num_workers_per_node=2)
    trainable = trainable_cls()
    result = trainable.train()
    trainable.stop()
    assert result["rank"] == 0


@pytest.mark.skipif(horovod is None, reason="Horovod not found.")
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


def test_colocator(tmpdir, ray_start_6_cpus):
    colocator = NodeColocator.options(num_cpus=4).remote(
        num_workers=4, use_gpu=False)
    colocator.create_workers.remote(_MockHorovodTrainable, {"hi": 1}, tmpdir)
    worker_handles = ray.get(colocator.get_workers.remote())
    assert len(set(ray.get(
        [h.hostname.remote() for h in worker_handles]))) == 1

    resources = ray.available_resources()
    ip_address = ray.services.get_node_ip_address()
    assert resources.get("CPU", 0) == 2, resources
    assert resources.get(f"node:{ip_address}", 0) == 1 - 4 * 0.01


def test_colocator_gpu(tmpdir, ray_start_4_cpus_4_gpus):
    colocator = NodeColocator.options(
        num_cpus=4, num_gpus=4).remote(
            num_workers=4, use_gpu=True)
    colocator.create_workers.remote(_MockHorovodTrainable, {"hi": 1}, tmpdir)
    worker_handles = ray.get(colocator.get_workers.remote())
    assert len(set(ray.get(
        [h.hostname.remote() for h in worker_handles]))) == 1
    resources = ray.available_resources()
    ip_address = ray.services.get_node_ip_address()
    assert resources.get("CPU", 0) == 0, resources
    assert resources.get("GPU", 0) == 0, resources
    assert resources.get(f"node:{ip_address}", 0) == 1 - 4 * 0.01

    all_envs = ray.get([h.env_vars.remote() for h in worker_handles])
    assert len({ev["CUDA_VISIBLE_DEVICES"] for ev in all_envs}) == 4


def test_horovod_mixin(ray_start_2_cpus):
    class Test(HorovodMixin):
        pass

    assert Test().hostname() == ray.services.get_node_ip_address()
    actor = ray.remote(HorovodMixin).remote()
    DUMMY_VALUE = 1123123
    actor.update_env_vars.remote({"TEST": DUMMY_VALUE})
    assert ray.get(actor.env_vars.remote())["TEST"] == str(DUMMY_VALUE)


@pytest.mark.skipif(horovod is None, reason="Horovod not found.")
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
