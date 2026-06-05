import logging
from typing import List, Optional

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.serve._private.request_router.request_router import RequestRouter

logger = logging.getLogger(SERVE_LOGGER_NAME)


class KVAwareRouter(RequestRouter):
    """Routes each request to the candidate that best balances expected KV-cache
    overlap against the worker's current prefill/decode load.
    """

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        """Choose the candidate replica(s) to route ``pending_request`` to.

        Args:
            candidate_replicas: The replicas eligible to serve the request.
            pending_request: The request being routed.

        Returns:
            Ranked groups of replicas.
        """
        raise NotImplementedError("KVAwareRouter.choose_replicas is not implemented")
