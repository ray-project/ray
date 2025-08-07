import sys

import pytest

import ray
import ray._private.accelerators.tpu
import ray.cluster_utils
from ray.tests.conftest import _ray_start_cluster
from ray.train.constants import TRAIN_PLACEMENT_GROUP_TIMEOUT_S_ENV
from ray.train.v2.jax.tpu_utils import (
    fetch_tpu_slice_name_from_pg,
    reserve_tpu_slice,
)


@pytest.fixture
def ray_start_cpu():
    address_info = ray.init(num_cpus=1)
    yield address_info
    ray.shutdown()


@pytest.fixture
def ray_tpu_cluster(monkeypatch):
    """Start a mock TPU Ray cluster."""
    with _ray_start_cluster() as cluster:
        monkeypatch.setenv("TPU_NAME", "test-slice-0")
        monkeypatch.setenv("TPU_WORKER_ID", "0")
        monkeypatch.setenv("TPU_ACCELERATOR_TYPE", "v4-8")
        monkeypatch.setenv("TPU_TOPOLOGY", "2x2x2")

        cluster.add_node(
            num_cpus=2,
            resources={"TPU": 4, "TPU-v4-8-head": 1},
        )
        monkeypatch.setenv("TPU_WORKER_ID", "1")
        cluster.add_node(
            num_cpus=2,
            resources={"TPU": 4},
        )
        ray.init(address=cluster.address)

        yield cluster
        ray.shutdown()


def test_fetch_tpu_slice_name_from_pg(ray_tpu_cluster):
    """Tests that the slice name can be fetched from a PG."""
    tpu_head_pg = ray.util.placement_group(bundles=[{"TPU-v4-8-head": 1}])
    ray.get(tpu_head_pg.ready())

    tpu_slice_name = "test-slice-0"
    slice_name = fetch_tpu_slice_name_from_pg(tpu_head_pg)
    assert slice_name == tpu_slice_name

    ray.util.remove_placement_group(tpu_head_pg)


def test_reserve_tpu_slice(ray_tpu_cluster):
    """Tests that a TPU slice can be successfully reserved."""
    tpu_slice_name = "test-slice-0"
    reserved_name = reserve_tpu_slice(topology="2x2x2", accelerator_type="TPU-V4")
    assert reserved_name == tpu_slice_name


def test_reserve_tpu_slice_timeout(ray_start_cpu, monkeypatch):
    """Tests that reserving a slice times out if no head node is available."""
    # This cluster has no TPU head resources, so PG creation will time out.
    monkeypatch.setenv(TRAIN_PLACEMENT_GROUP_TIMEOUT_S_ENV, "10")
    with pytest.raises(TimeoutError):
        reserve_tpu_slice(topology="2x2x2", accelerator_type="TPU-V4")


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
