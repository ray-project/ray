import os
import tempfile
from typing import Optional

import pytest
from ray.serve.constants import DEFAULT_CHECKPOINT_PATH
from ray.serve.storage.checkpoint_path import make_kv_store
from ray.serve.storage.kv_store import (RayInternalKVStore, RayLocalKVStore,
                                        RayS3KVStore)
from ray.serve.storage.kv_store_base import KVStoreBase
from ray.serve.storage.ray_gcs_kv_store import RayGcsKVStore


def test_ray_internal_kv(serve_instance):  # noqa: F811
    with pytest.raises(TypeError):
        RayInternalKVStore(namespace=1)
        RayInternalKVStore(namespace=b"")

    kv = RayInternalKVStore()

    with pytest.raises(TypeError):
        kv.put(1, b"1")
    with pytest.raises(TypeError):
        kv.put("1", 1)
    with pytest.raises(TypeError):
        kv.put("1", "1")

    kv.put("1", b"2")
    assert kv.get("1") == b"2"
    kv.put("2", b"4")
    assert kv.get("2") == b"4"
    kv.put("1", b"3")
    assert kv.get("1") == b"3"
    assert kv.get("2") == b"4"


def test_ray_internal_kv_collisions(serve_instance):  # noqa: F811
    kv1 = RayInternalKVStore()
    kv1.put("1", b"1")
    assert kv1.get("1") == b"1"

    kv2 = RayInternalKVStore("namespace")

    assert kv2.get("1") is None

    kv2.put("1", b"-1")
    assert kv2.get("1") == b"-1"
    assert kv1.get("1") == b"1"


def _test_operations(kv_store):
    # Trival get & put
    kv_store.put("1", b"1")
    assert kv_store.get("1") == b"1"
    kv_store.put("2", b"2")
    assert kv_store.get("1") == b"1"
    assert kv_store.get("2") == b"2"

    # Overwrite same key
    kv_store.put("1", b"-1")
    assert kv_store.get("1") == b"-1"

    # Get non-existing key
    assert kv_store.get("3") is None

    # Delete existing key
    kv_store.delete("1")
    kv_store.delete("2")
    assert kv_store.get("1") is None
    assert kv_store.get("2") is None

    # Delete non-existing key
    kv_store.delete("3")


def test_external_kv_local_disk():
    kv_store = RayLocalKVStore(
        "namespace", os.path.join(tempfile.gettempdir(), "test_kv_store.db"))

    _test_operations(kv_store)


@pytest.mark.skip(reason="Need to figure out credentials for testing")
def test_external_kv_aws_s3():
    kv_store = RayS3KVStore(
        "namespace",
        bucket="jiao-test",
        s3_path="/checkpoint",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", None),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", None),
        aws_session_token=os.environ.get("AWS_SESSION_TOKEN", None),
    )

    _test_operations(kv_store)


@pytest.mark.skip(reason="Need to figure out credentials for testing")
def test_external_kv_gcs():
    kv_store = RayGcsKVStore(
        "namespace",
        bucket="jiao-test",
        prefix="/checkpoint",
    )

    _test_operations(kv_store)


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


def test_make_kv_store(serve_instance):
    namespace = "ns"
    assert isinstance(
        make_kv_store(DEFAULT_CHECKPOINT_PATH, namespace), RayInternalKVStore)
    assert isinstance(
        make_kv_store("file:///tmp/deep/dir/my_path", namespace),
        RayLocalKVStore)
    assert isinstance(
        make_kv_store("s3://object_store/my_path", namespace), RayS3KVStore)

    with pytest.raises(ValueError, match="shouldn't be empty"):
        # Empty path
        make_kv_store("file://", namespace)

    with pytest.raises(ValueError, match="must be one of"):
        # Wrong prefix
        make_kv_store("s4://some_path", namespace)

    module_name = "ray.serve.tests.storage_tests.test_kv_store"
    with pytest.raises(ValueError, match="doesn't inherit"):
        make_kv_store(
            f"custom://{module_name}.MyNonCompliantStoreCls",
            namespace=namespace)

    store = make_kv_store(
        f"custom://{module_name}.MyCustomStorageCls?arg1=val1&arg2=val2",
        namespace=namespace,
    )
    assert store.namespace == namespace
    assert store.kwargs == {"arg1": "val1", "arg2": "val2"}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
