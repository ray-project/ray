import logging
from ray._raylet import MyGcsClient

logger = logging.getLogger(__name__)


class GcsAioClient:
    def __init__(
        self,
        address: str = None,
        loop=None,
        executor=None,
        nums_reconnect_retry: int = 5,
    ):
        self.my_gcs_client = MyGcsClient.standalone(str(address))

    def __getattr__(self, name):
        async_names = [
            "internal_kv_get",
            "internal_kv_multi_get",
            "internal_kv_put",
            "internal_kv_del",
            "internal_kv_exists",
            "internal_kv_keys",
            "check_alive",
            "get_all_job_info",
        ]
        if name in async_names:
            return getattr(self.my_gcs_client, "async_" + name)
        property_names = ["address", "cluster_id"]
        if name in property_names:
            return getattr(self.my_gcs_client, name)
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )
