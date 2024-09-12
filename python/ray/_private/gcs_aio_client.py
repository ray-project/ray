import os
import logging
from functools import partial
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor
from ray._raylet import GcsClient, NewGcsClient, JobID
from ray.core.generated import (
    gcs_pb2,
)
import ray._private.utils
from ray._private.ray_constants import env_integer
import ray

# Number of executor threads. No more than this number of concurrent GcsAioClient calls
# can happen. Extra requests will need to wait for the existing requests to finish.
# Executor rules:
# If the arg `executor` in GcsAioClient constructor is set, use it.
# Otherwise if env var `GCS_AIO_CLIENT_DEFAULT_THREAD_COUNT` is set, use it.
# Otherwise, use 5.
# This is only used for the OldGcsAioClient.
GCS_AIO_CLIENT_DEFAULT_THREAD_COUNT = env_integer(
    "GCS_AIO_CLIENT_DEFAULT_THREAD_COUNT", 5
)

logger = logging.getLogger(__name__)


class GcsAioClient:
    """
    Async GCS client.

    This class is in transition to use the new C++ GcsClient binding. The old
    PythonGcsClient binding is not deleted until we are confident that the new
    binding is stable.

    Defaults to the new binding. If you want to use the old binding, please
    set the environment variable `RAY_USE_OLD_GCS_CLIENT=1`.
    """

    def __new__(cls, *args, **kwargs):
        use_old_client = os.getenv("RAY_USE_OLD_GCS_CLIENT") == "1"
        logger.debug(f"Using {'old' if use_old_client else 'new'} GCS client")
        if use_old_client:
            return OldGcsAioClient(*args, **kwargs)
        else:
            return NewGcsAioClient(*args, **kwargs)


class NewGcsAioClient:
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
        self.inner = NewGcsClient.standalone(
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


class AsyncProxy:
    def __init__(self, inner, loop, executor):
        self.inner = inner
        self.loop = loop
        self.executor = executor

    def _function_to_async(self, func):
        async def wrapper(*args, **kwargs):
            partial_func = partial(func, *args, **kwargs)
            return await self.loop.run_in_executor(self.executor, partial_func)

        return wrapper

    def __getattr__(self, name):
        """
        If attr is callable, wrap it into an async function.
        """
        attr = getattr(self.inner, name)
        if callable(attr):
            return self._function_to_async(attr)
        else:
            return attr


class OldGcsAioClient:
    def __init__(
        self,
        loop=None,
        executor=None,
        address: Optional[str] = None,
        nums_reconnect_retry: int = 5,
        cluster_id: Optional[str] = None,
    ):
        if loop is None:
            loop = ray._private.utils.get_or_create_event_loop()
        if executor is None:
            executor = ThreadPoolExecutor(
                max_workers=GCS_AIO_CLIENT_DEFAULT_THREAD_COUNT,
                thread_name_prefix="gcs_aio_client",
            )

        self._gcs_client = GcsClient(address, nums_reconnect_retry, cluster_id)
        self._async_proxy = AsyncProxy(self._gcs_client, loop, executor)
        self._nums_reconnect_retry = nums_reconnect_retry

    @property
    def address(self):
        return self._gcs_client.address

    @property
    def cluster_id(self):
        return self._gcs_client.cluster_id

    async def check_alive(
        self, node_ips: List[bytes], timeout: Optional[float] = None
    ) -> List[bool]:
        logger.debug(f"check_alive {node_ips!r}")
        return await self._async_proxy.check_alive(node_ips, timeout)

    async def internal_kv_get(
        self, key: bytes, namespace: Optional[bytes], timeout: Optional[float] = None
    ) -> Optional[bytes]:
        logger.debug(f"internal_kv_get {key!r} {namespace!r}")
        return await self._async_proxy.internal_kv_get(key, namespace, timeout)

    async def internal_kv_multi_get(
        self,
        keys: List[bytes],
        namespace: Optional[bytes],
        timeout: Optional[float] = None,
    ) -> Dict[bytes, bytes]:
        logger.debug(f"internal_kv_multi_get {keys!r} {namespace!r}")
        return await self._async_proxy.internal_kv_multi_get(keys, namespace, timeout)

    async def internal_kv_put(
        self,
        key: bytes,
        value: bytes,
        overwrite: bool,
        namespace: Optional[bytes],
        timeout: Optional[float] = None,
    ) -> int:
        """Put a key-value pair into the GCS.

        Args:
            key: The key to put.
            value: The value to put.
            overwrite: Whether to overwrite the value if the key already exists.
            namespace: The namespace to put the key-value pair into.
            timeout: The timeout in seconds.

        Returns:
            The number of keys added. If overwrite is True, this will be 1 if the
                key was added and 0 if the key was updated. If overwrite is False,
                this will be 1 if the key was added and 0 if the key already exists.
        """
        logger.debug(f"internal_kv_put {key!r} {value!r} {overwrite} {namespace!r}")
        return await self._async_proxy.internal_kv_put(
            key, value, overwrite, namespace, timeout
        )

    async def internal_kv_del(
        self,
        key: bytes,
        del_by_prefix: bool,
        namespace: Optional[bytes],
        timeout: Optional[float] = None,
    ) -> int:
        logger.debug(f"internal_kv_del {key!r} {del_by_prefix} {namespace!r}")
        return await self._async_proxy.internal_kv_del(
            key, del_by_prefix, namespace, timeout
        )

    async def internal_kv_exists(
        self, key: bytes, namespace: Optional[bytes], timeout: Optional[float] = None
    ) -> bool:
        logger.debug(f"internal_kv_exists {key!r} {namespace!r}")
        return await self._async_proxy.internal_kv_exists(key, namespace, timeout)

    async def internal_kv_keys(
        self, prefix: bytes, namespace: Optional[bytes], timeout: Optional[float] = None
    ) -> List[bytes]:
        logger.debug(f"internal_kv_keys {prefix!r} {namespace!r}")
        return await self._async_proxy.internal_kv_keys(prefix, namespace, timeout)

    async def get_all_job_info(
        self,
        *,
        job_or_submission_id: Optional[str] = None,
        skip_submission_job_info_field: bool = False,
        skip_is_running_tasks_field: bool = False,
        timeout: Optional[float] = None,
    ) -> Dict[JobID, gcs_pb2.JobTableData]:
        """
        Return dict key: bytes of job_id; value: JobTableData pb message.
        """
        return await self._async_proxy.get_all_job_info(
            job_or_submission_id=job_or_submission_id,
            skip_submission_job_info_field=skip_submission_job_info_field,
            skip_is_running_tasks_field=skip_is_running_tasks_field,
            timeout=timeout,
        )
