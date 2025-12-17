from fastapi import Response

from ray.llm._internal.serve.utils.broadcast import broadcast


class ReplicaBroadcastable:
    async def _broadcast_to_replicas(
        self, model: str, method: str, kwargs: dict | None = None
    ) -> Response:
        """Helper to broadcast a command to all replicas and return a 200 response.

        Args:
            model: The model ID or None to use default.
            method: The method name to call on each replica.
            kwargs: Optional kwargs to pass to the method.

        Returns:
            200 OK response.
        """
        model_id = await self._get_model_id(model)
        handle = self._get_configured_serve_handle(model_id)
        results = broadcast(handle, method, kwargs=kwargs)
        return results
