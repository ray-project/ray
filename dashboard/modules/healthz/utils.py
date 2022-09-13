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

        liveness = await self._gcs_aio_client.check_alive(
            [self._local_node_address.encode()], 0.1
        )
        return liveness[0]

    async def check_gcs_liveness(self) -> bool:
        await self._gcs_aio_client.check_alive([], 0.1)
        return True
