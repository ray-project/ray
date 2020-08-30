import ray

from ray.tune.integration._horovod_job import (BaseHorovodWorker,
                                               NodeColocator)
import pytest


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
    SetColocator = NodeColocator.options(num_cpus=4)
    colocator = SetColocator.remote(
        node_rank=4, num_slots=4, world_size=5, use_gpu=False)
    colocator.create_workers.remote()
    worker_handles = ray.get(colocator.get_workers.remote())
    assert len(set(ray.get(
        [h.hostname.remote() for h in worker_handles]))) == 1

    resources = ray.available_resources()
    ip_address = ray.services.get_node_ip_address()
    assert resources.get("CPU", 0) == 2, resources
    assert resources.get(f"node:{ip_address}", 0) == 1 - 4 * 0.01


def test_colocator_gpu(tmpdir, ray_start_4_cpus_4_gpus):
    SetColocator = NodeColocator.options(num_cpus=4, num_gpus=4)
    colocator = SetColocator.remote(
        node_rank=0, num_slots=4, world_size=4, use_gpu=True)
    colocator.create_workers.remote()
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
    class Test(BaseHorovodWorker):
        pass

    assert Test().hostname() == ray.services.get_node_ip_address()
    actor = ray.remote(BaseHorovodWorker).remote()
    DUMMY_VALUE = 1123123
    actor.update_env_vars.remote({"TEST": DUMMY_VALUE})
    assert ray.get(actor.env_vars.remote())["TEST"] == str(DUMMY_VALUE)
