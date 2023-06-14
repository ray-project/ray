from starlette.middleware import Middleware
import logging


class ASGIMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        scope.get("headers").append((b"custom_header_key", "custom_header_value"))
        await self.app(scope, receive, send)


def add_middleware():
    return [Middleware(ASGIMiddleware)]


class MyServeFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord):
        log_msg = super().format(record)
        return "MyCustom message: hello " + log_msg


def add_logger():
    ray_logger = logging.getLogger("ray.serve")
    handler = logging.StreamHandler()
    handler.setFormatter(MyServeFormatter())
    ray_logger.addHandler(handler)
