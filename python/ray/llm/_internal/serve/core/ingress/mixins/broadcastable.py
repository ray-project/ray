import asyncio
from typing import Any, List

from ray.llm._internal.serve.utils.broadcast import broadcast


class ReplicaBroadcastable:
    async def _broadcast_to_replicas(
        self, model: str, method: str, kwargs: dict | None = None
    ) -> List[Any]:
        """Broadcast a command to all replicas and return their results.

        Args:
            model: The model ID to broadcast to.
            method: The method name to call on each replica.
            kwargs: Optional kwargs to pass to the method.

        Returns:
            List of results from each replica.
        """
        model_id = await self._get_model_id(model)
        handle = self._get_configured_serve_handle(model_id)
        # Run blocking broadcast() in a thread to avoid blocking the event loop.
        # broadcast() uses ray.get() internally which is synchronous.
        results = await asyncio.to_thread(broadcast, handle, method, kwargs=kwargs)
        return results
