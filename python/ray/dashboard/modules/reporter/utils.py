from typing import Optional

from ray._common.utils import hex_to_binary
from ray._raylet import GcsClient


class HealthChecker:
    def __init__(self, gcs_client: GcsClient, local_node_id: Optional[str] = None):
        self._gcs_client = gcs_client
        self._local_node_id_bytes = (
            hex_to_binary(local_node_id) if local_node_id is not None else None
        )

    async def check_local_raylet_liveness(self) -> bool:
        if self._local_node_id_bytes is None:
            return False
        liveness = await self._gcs_client.async_check_alive(
            [self._local_node_id_bytes], 0.1
        )
        return liveness[0]

    async def check_gcs_liveness(self) -> bool:
        await self._gcs_client.async_check_alive([], 0.1)
        return True
