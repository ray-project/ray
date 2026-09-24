import os
import sys

import pytest

import ray
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

GB = 10**9
VRAM_LABEL = ray._raylet.RAY_NODE_GPU_MEMORY_PER_DEVICE_KEY


@pytest.fixture
def small_and_big_gpu_cluster(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)
    small = cluster.add_node(
        num_cpus=4, num_gpus=1, labels={VRAM_LABEL: str(24 * GB)}
    ).node_id
    big = cluster.add_node(
        num_cpus=4, num_gpus=2, labels={VRAM_LABEL: str(80 * GB)}
    ).node_id
    cluster.wait_for_nodes()
    yield cluster, small, big


@ray.remote
class Replica:
    def info(self):
        context = ray.get_runtime_context()
        return (
            context.get_node_id(),
            context.get_assigned_resources().get("GPU"),
            ray.get_gpu_ids(),
            os.environ.get("CUDA_MPS_PINNED_DEVICE_MEM_LIMIT"),
        )


def test_gpu_memory_uses_per_node_vram(small_and_big_gpu_cluster):
    _, small, big = small_and_big_gpu_cluster

    replicas = [Replica.options(gpu_memory=50 * GB).remote() for _ in range(2)]
    infos = ray.get([r.info.remote() for r in replicas])
    assert [info[0] for info in infos] == [big, big]
    assert [info[1] for info in infos] == [0.625, 0.625]
    assert sorted(info[2][0] for info in infos) == [0, 1]

    # 30GB fills the rest of each 80GB GPU and never fits a 24GB one.
    fillers = [Replica.options(gpu_memory=30 * GB).remote() for _ in range(2)]
    assert [ray.get(r.info.remote())[0] for r in fillers] == [big, big]

    small_replica = Replica.options(gpu_memory=20 * GB).remote()
    node_id, gpus, gpu_ids, _ = ray.get(small_replica.info.remote())
    assert node_id == small
    assert gpus == pytest.approx(0.8334)
    assert len(gpu_ids) == 1

    pending = Replica.options(gpu_memory=50 * GB).remote()
    ready, _ = ray.wait([pending.info.remote()], timeout=3)
    assert not ready


def test_gpu_memory_larger_than_any_gpu_stays_pending(small_and_big_gpu_cluster):
    replica = Replica.options(gpu_memory=81 * GB).remote()
    ready, _ = ray.wait([replica.info.remote()], timeout=3)
    assert not ready


def test_gpu_memory_task(small_and_big_gpu_cluster):
    _, _, big = small_and_big_gpu_cluster

    @ray.remote(gpu_memory=40 * GB)
    def f():
        return ray.get_runtime_context().get_node_id()

    assert f._default_options["max_calls"] == 1
    assert ray.get(f.remote()) == big


def test_mps_gpu_memory_limit(monkeypatch, ray_start_cluster):
    monkeypatch.setenv("RAY_ENABLE_MPS_GPU_MEMORY_LIMIT", "1")
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2, num_gpus=1, labels={VRAM_LABEL: str(80 * GB)})
    ray.init(address=cluster.address)

    limited = Replica.options(gpu_memory=40 * GB).remote()
    assert ray.get(limited.info.remote())[3] == f"0={-(-40 * GB // 2**20)}MB"

    unlimited = Replica.options(num_gpus=0.25).remote()
    assert ray.get(unlimited.info.remote())[3] is None


def test_gpu_memory_rejected_in_placement_group(small_and_big_gpu_cluster):
    with pytest.raises(ValueError, match="gpu_memory"):
        placement_group([{"CPU": 1, "gpu_memory": GB}])

    pg = placement_group([{"CPU": 1, "GPU": 1}])
    ray.get(pg.ready())
    with pytest.raises(ValueError, match="placement group"):
        Replica.options(
            gpu_memory=GB,
            scheduling_strategy=PlacementGroupSchedulingStrategy(pg),
        ).remote()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
