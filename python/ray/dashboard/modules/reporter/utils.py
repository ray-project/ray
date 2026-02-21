from typing import Optional

from ray._raylet import GcsClient, NodeID


class HealthChecker:
    def __init__(self, gcs_client: GcsClient, local_node_id: Optional[NodeID] = None):
        self._gcs_client = gcs_client
        self._local_node_id = local_node_id

    async def check_local_raylet_liveness(self) -> bool:
        if self._local_node_id is None:
            return False
        liveness = await self._gcs_client.async_check_alive([self._local_node_id], 0.1)
        return liveness[0]

    async def check_gcs_liveness(self) -> bool:
        await self._gcs_client.async_check_alive([], 0.1)
        return True
