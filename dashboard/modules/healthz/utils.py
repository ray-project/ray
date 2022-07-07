from typing import Optional
from ray._private.gcs_utils import GcsAioClient


class HealthChecker:
    def __init__(
        self, gcs_aio_client: GcsAioClient, local_node_address: Optional[str] = None
    ):
        self._gcs_aio_client = gcs_aio_client
        self._local_node_address = local_node_address

    async def check_local_raylet_liveness(self) -> bool:
        if self._local_node_address is None:
            return False
        return await self._gcs_aio_client.check_alive(
            [self.local_node_address.encode()], 1
        )[0]

    async def check_gcs_liveness(self) -> bool:
        try:
            await self._gcs_aio_client.check_alive([], 1)
        except Exception:
            return False
        return True
