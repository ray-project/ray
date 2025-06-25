from typing import List

from python.ray.serve._private.config import MiddlewareConfig


class ServeMiddleware:
    """Base class for Ray Serve middleware."""

    def __init__(self, get_response):
        self.get_response = get_response

    async def __call__(self, request):
        # Pre-processing
        response = await self.process_request(request)
        if response is None:
            response = await self.get_response(request)

        # Post-processing
        return await self.process_response(request, response)

    async def process_request(self, request):
        """Override to process request before deployment call."""
        return None

    async def process_response(self, request, response):
        """Override to process response after deployment call."""
        return response


class MiddlewareManager:
    """Manages middleware stack for deployments."""

    def __init__(self, middlewares: List[MiddlewareConfig]):
        self.middlewares = middlewares

    def build_middleware_stack(self, base_handler):
        """Build the middleware stack around the base handler."""
        handler = base_handler

        # Apply middleware in reverse order (last middleware wraps first)
        for middleware_config in reversed(self.middlewares):
            middleware_class = middleware_config.middleware_class
            args = middleware_config.args
            kwargs = middleware_config.kwargs
            handler = middleware_class(*args, **kwargs)(handler)

        return handler
