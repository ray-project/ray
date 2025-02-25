import logging
from typing import Optional
import ray
from ray._raylet import InnerGcsClient

logger = logging.getLogger(__name__)


class GcsAioClient:
    """
    Async GCS client.

    Historical note: there was a `ray::gcs::PythonGcsClient` C++ binding which has only
    sync API and in Python we wrap it with ThreadPoolExecutor. It's been removed in
    favor of `ray::gcs::GcsClient` which contains async API.
    """

    def __init__(
        self,
        address: str = None,
        loop=None,
        executor=None,
        nums_reconnect_retry: int = 5,
        cluster_id: Optional[str] = None,
    ):
        # This must be consistent with GcsClient.__cinit__ in _raylet.pyx
        timeout_ms = ray._config.py_gcs_connect_timeout_s() * 1000
        self.inner = InnerGcsClient.standalone(
            str(address), cluster_id=cluster_id, timeout_ms=timeout_ms
        )
        # Forwarded Methods. Not using __getattr__ because we want one fewer layer of
        # indirection.
        self.internal_kv_get = self.inner.async_internal_kv_get
        self.internal_kv_multi_get = self.inner.async_internal_kv_multi_get
        self.internal_kv_put = self.inner.async_internal_kv_put
        self.internal_kv_del = self.inner.async_internal_kv_del
        self.internal_kv_exists = self.inner.async_internal_kv_exists
        self.internal_kv_keys = self.inner.async_internal_kv_keys
        self.check_alive = self.inner.async_check_alive
        self.get_all_job_info = self.inner.async_get_all_job_info
        # Forwarded Properties.
        self.address = self.inner.address
        self.cluster_id = self.inner.cluster_id
        # Note: these only exists in the new client.
        self.get_all_actor_info = self.inner.async_get_all_actor_info
        self.get_all_node_info = self.inner.async_get_all_node_info
        self.kill_actor = self.inner.async_kill_actor
        self.get_cluster_status = self.inner.async_get_cluster_status
