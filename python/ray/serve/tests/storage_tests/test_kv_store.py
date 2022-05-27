import os
import tempfile
from typing import Optional

import pytest
from ray._private.test_utils import simulate_storage
from ray.serve.constants import DEFAULT_CHECKPOINT_PATH
from ray.serve.storage.checkpoint_path import make_kv_store
from ray.serve.storage.kv_store import RayInternalKVStore, RayLocalKVStore, RayS3KVStore
from ray.serve.storage.kv_store_base import KVStoreBase
from ray.serve.storage.ray_gcs_kv_store import RayGcsKVStore


@pytest.mark.asyncio
async def test_ray_internal_kv(serve_instance):  # noqa: F811
    with pytest.raises(TypeError):
        RayInternalKVStore(namespace=1)
        RayInternalKVStore(namespace=b"")

    kv = RayInternalKVStore()

    with pytest.raises(TypeError):
        await kv.put(1, b"1")
    with pytest.raises(TypeError):
        await kv.put("1", 1)
    with pytest.raises(TypeError):
        await kv.put("1", "1")

    await kv.put("1", b"2")
    assert await kv.get("1") == b"2"
    await kv.put("2", b"4")
    assert await kv.get("2") == b"4"
    await kv.put("1", b"3")
    assert await kv.get("1") == b"3"
    assert await kv.get("2") == b"4"


@pytest.mark.asyncio
async def test_ray_internal_kv_collisions(serve_instance):  # noqa: F811
    kv1 = RayInternalKVStore()
    await kv1.put("1", b"1")
    assert await kv1.get("1") == b"1"

    kv2 = RayInternalKVStore("namespace")

    assert await kv2.get("1") is None

    await kv2.put("1", b"-1")
    assert await kv2.get("1") == b"-1"
    assert await kv1.get("1") == b"1"


async def _test_operations(kv_store):
    # Trival get & put
    await kv_store.put("1", b"1")
    assert await kv_store.get("1") == b"1"
    await kv_store.put("2", b"2")
    assert await kv_store.get("1") == b"1"
    assert await kv_store.get("2") == b"2"

    # Overwrite same key
    await kv_store.put("1", b"-1")
    assert await kv_store.get("1") == b"-1"

    # Get non-existing key
    assert await kv_store.get("3") is None

    # Delete existing key
    await kv_store.delete("1")
    await kv_store.delete("2")
    assert await kv_store.get("1") is None
    assert await kv_store.get("2") is None

    # Delete non-existing key
    await kv_store.delete("3")


@pytest.mark.asyncio
async def test_external_kv_local_disk():
    kv_store = RayLocalKVStore(
        "namespace", os.path.join(tempfile.gettempdir(), "test_kv_store.db")
    )

    await _test_operations(kv_store)


@pytest.mark.asyncio
async def test_external_kv_aws_s3():
    with simulate_storage("s3", "serve-test") as url:
        # s3://serve-test/checkpoint?region=us-west-2&endpoint_override=http://localhost:5002
        import boto3

        s3 = boto3.client(
            "s3",
            region_name="us-west-2",
            endpoint_url="http://localhost:5002",
        )
        s3.create_bucket(Bucket="serve-test")

        kv_store = RayS3KVStore(
            "namespace",
            bucket="serve-test",
            prefix="checkpoint",
            region_name="us-west-2",
            endpoint_url="http://localhost:5002",
        )

        await _test_operations(kv_store)


@pytest.mark.asyncio
@pytest.mark.skip(reason="Need to figure out credentials for testing")
async def test_external_kv_gcs():
    kv_store = RayGcsKVStore(
        "namespace",
        bucket="jiao-test",
        prefix="/checkpoint",
    )

    await _test_operations(kv_store)


class MyNonCompliantStoreCls:
    pass


class MyCustomStorageCls(KVStoreBase):
    def __init__(self, namespace, **kwargs):
        self.namespace = namespace
        self.kwargs = kwargs

    def delete(self, key: str) -> None:
        return super().delete(key)

    def get(self, key: str) -> Optional[bytes]:
        return super().get(key)

    def get_storage_key(self, key: str) -> str:
        return super().get_storage_key(key)

    def put(self, key: str, val: bytes) -> bool:
        return super().put(key, val)


@pytest.mark.asyncio
async def test_make_kv_store(serve_instance):
    namespace = "ns"
    assert isinstance(
        make_kv_store(DEFAULT_CHECKPOINT_PATH, namespace), RayInternalKVStore
    )
    assert isinstance(
        make_kv_store("file:///tmp/deep/dir/my_path", namespace), RayLocalKVStore
    )
    assert isinstance(
        make_kv_store("s3://object_store/my_path", namespace), RayS3KVStore
    )

    with pytest.raises(ValueError, match="shouldn't be empty"):
        # Empty path
        make_kv_store("file://", namespace)

    with pytest.raises(ValueError, match="must be one of"):
        # Wrong prefix
        make_kv_store("s4://some_path", namespace)

    module_name = "ray.serve.tests.storage_tests.test_kv_store"
    with pytest.raises(ValueError, match="doesn't inherit"):
        make_kv_store(
            f"custom://{module_name}.MyNonCompliantStoreCls", namespace=namespace
        )

    store = make_kv_store(
        f"custom://{module_name}.MyCustomStorageCls?arg1=val1&arg2=val2",
        namespace=namespace,
    )
    assert store.namespace == namespace
    assert store.kwargs == {"arg1": "val1", "arg2": "val2"}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
