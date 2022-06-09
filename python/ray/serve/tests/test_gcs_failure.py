import pytest
import ray
import grpc
import ray.serve as serve
from ray.tests.conftest import external_redis
from ray.serve.storage.kv_store import RayInternalKVStore, KVStoreError


@pytest.fixture
def serve_ha(external_redis):
    address_info = ray.init(
        num_cpus=36,
        namespace="default_test_namespace",
        _metrics_export_port=9999,
        _system_config={"metrics_report_interval_ms": 1000, "task_retry_delay_ms": 50},
    )
    yield (address_info, serve.start(detached=True))
    ray.shutdown()


def test_ray_internal_kv_timeout(serve_ha):  # noqa: F811
    # Firstly make sure it's workin
    kv1 = RayInternalKVStore()
    kv1.put("1", b"1")
    assert kv1.get("1") == b"1"

    # Killfg the GCS
    ray.worker._global_node.kill_gcs_server()

    with pytest.raises(KVStoreError) as e:
        kv1.put("2", b"2")
    assert e.value.orig_exce.code() == grpc.StatusCode.DEADLINE_EXCEEDED
