import logging
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor
from ray._raylet import GcsClient
from ray.core.generated import (
    gcs_pb2,
)
import ray._private.utils


logger = logging.getLogger(__name__)


class AsyncProxy:
    def __init__(self, inner, loop, executor):
        self.inner = inner
        self.loop = loop
        self.executor = executor

    def _function_to_async(self, func):
        async def wrapper(*args, **kwargs):
            return await self.loop.run_in_executor(self.executor, func, *args, **kwargs)

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


class GcsAioClient:
    def __init__(
        self,
        loop=None,
        executor=None,
        address: Optional[str] = None,
        nums_reconnect_retry: int = 5,
    ):
        if loop is None:
            loop = ray._private.utils.get_or_create_event_loop()
        if executor is None:
            # TODO(ryw): make this number configurable
            executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="gcs_aio_client"
            )

        self._gcs_client = GcsClient(address, nums_reconnect_retry)
        self._async_proxy = AsyncProxy(self._gcs_client, loop, executor)
        self._connect()
        self._nums_reconnect_retry = nums_reconnect_retry

    def _connect(self):
        self._gcs_client._connect()

    @property
    def address(self):
        return self._gcs_client.address

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
        self, timeout: Optional[float] = None
    ) -> Dict[bytes, gcs_pb2.JobTableData]:
        """
        Return dict key: bytes of job_id; value: JobTableData pb message.
        """
        return await self._async_proxy.get_all_job_info(timeout)
