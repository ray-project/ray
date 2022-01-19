import asyncio
from dataclasses import dataclass
import inspect
import json
from typing import Any, Dict, List, Optional, Tuple, Type

import starlette.responses
import starlette.requests

from ray.serve.exceptions import RayServeException


@dataclass
class HTTPRequestWrapper:
    scope: Dict[Any, Any]
    body: bytes


def build_starlette_request(scope, serialized_body: bytes):
    """Build and return a Starlette Request from ASGI payload.

    This function is intended to be used immediately before task invocation
    happens.
    """

    # Simulates receiving HTTP body from TCP socket.  In reality, the body has
    # already been streamed in chunks and stored in serialized_body.
    received = False

    async def mock_receive():
        nonlocal received

        # If the request has already been received, starlette will keep polling
        # for HTTP disconnect. We will pause forever. The coroutine should be
        # cancelled by starlette after the response has been sent.
        if received:
            block_forever = asyncio.Event()
            await block_forever.wait()

        received = True
        return {
            "body": serialized_body,
            "type": "http.request",
            "more_body": False
        }

    return starlette.requests.Request(scope, mock_receive)


class Response:
    """ASGI compliant response class.

    It is expected to be called in async context and pass along
    `scope, receive, send` as in ASGI spec.

    >>> await Response({"k": "v"}).send(scope, receive, send)
    """

    def __init__(self, content=None, status_code=200):
        """Construct a HTTP Response based on input type.

        Args:
            content (optional): Any JSON serializable object.
            status_code (int, optional): Default status code is 200.
        """
        self.status_code = status_code
        self.raw_headers = []

        if content is None:
            self.body = b""
            self.set_content_type("text")
        elif isinstance(content, bytes):
            self.body = content
            self.set_content_type("text")
        elif isinstance(content, str):
            self.body = content.encode("utf-8")
            self.set_content_type("text-utf8")
        else:
            # Delayed import since utils depends on http_util
            from ray.serve.utils import ServeEncoder
            self.body = json.dumps(
                content, cls=ServeEncoder, indent=2).encode()
            self.set_content_type("json")

    def set_content_type(self, content_type):
        if content_type == "text":
            self.raw_headers.append([b"content-type", b"text/plain"])
        elif content_type == "text-utf8":
            self.raw_headers.append(
                [b"content-type", b"text/plain; charset=utf-8"])
        elif content_type == "json":
            self.raw_headers.append([b"content-type", b"application/json"])
        else:
            raise ValueError("Invalid content type {}".format(content_type))

    async def send(self, scope, receive, send):
        await send({
            "type": "http.response.start",
            "status": self.status_code,
            "headers": self.raw_headers,
        })
        await send({"type": "http.response.body", "body": self.body})


async def receive_http_body(scope, receive, send):
    body_buffer = []
    more_body = True
    while more_body:
        message = await receive()
        assert message["type"] == "http.request"

        more_body = message["more_body"]
        body_buffer.append(message["body"])

    return b"".join(body_buffer)


class ASGIHTTPSender:
    """Implement the interface for ASGI sender, build Starlette Response"""

    def __init__(self) -> None:
        self.status_code: Optional[int] = 200
        self.headers: List[Tuple[bytes, bytes]] = []
        self.buffer: List[bytes] = []

    async def __call__(self, message):
        if (message["type"] == "http.response.start"):
            self.status_code = message["status"]
            self.headers = message["headers"]
        elif (message["type"] == "http.response.body"):
            self.buffer.append(message["body"])
        else:
            raise ValueError("ASGI type must be one of "
                             "http.responses.{body,start}.")

    def build_starlette_response(self) -> starlette.responses.Response:
        resp = starlette.responses.Response(
            b"".join(self.buffer), status_code=self.status_code)
        resp.raw_headers.extend(self.headers)
        return resp


def make_fastapi_class_based_view(fastapi_app, cls: Type) -> None:
    """Transform the `cls`'s methods and class annotations to FastAPI routes.

    Modified from
    https://github.com/dmontagu/fastapi-utils/blob/master/fastapi_utils/cbv.py

    Usage:
    >>> app = FastAPI()
    >>> class A:
            @app.route("/{i}")
            def func(self, i: int) -> str:
                return self.dep + i
    >>> # just running the app won't work, here.
    >>> make_fastapi_class_based_view(app, A)
    >>> # now app can be run properly
    """
    # Delayed import to prevent ciruclar imports in workers.
    from fastapi import Depends, APIRouter
    from fastapi.routing import APIRoute

    def get_current_servable_instance():
        from ray import serve
        return serve.get_replica_context().servable_object

    # Find all the class method routes
    class_method_routes = [
        route for route in fastapi_app.routes if
        # User defined routes must all be APIRoute.
        isinstance(route, APIRoute)
        # We want to find the route that's bound to the `cls`.
        # NOTE(simon): we can't use `route.endpoint in inspect.getmembers(cls)`
        # because the FastAPI supports different routes for the methods with
        # same name. See #17559.
        and (cls.__qualname__ in route.endpoint.__qualname__)
    ]

    # Modify these routes and mount it to a new APIRouter.
    # We need to to this (instead of modifying in place) because we want to use
    # the laster fastapi_app.include_router to re-run the dependency analysis
    # for each routes.
    new_router = APIRouter()
    for route in class_method_routes:
        fastapi_app.routes.remove(route)

        # This block just adds a default values to the self parameters so that
        # FastAPI knows to inject the object when calling the route.
        # Before: def method(self, i): ...
        # After: def method(self=Depends(...), *, i):...
        old_endpoint = route.endpoint
        old_signature = inspect.signature(old_endpoint)
        old_parameters = list(old_signature.parameters.values())
        if len(old_parameters) == 0:
            # TODO(simon): make it more flexible to support no arguments.
            raise RayServeException(
                "Methods in FastAPI class-based view must have ``self`` as "
                "their first argument.")
        old_self_parameter = old_parameters[0]
        new_self_parameter = old_self_parameter.replace(
            default=Depends(get_current_servable_instance))
        new_parameters = [new_self_parameter] + [
            # Make the rest of the parameters keyword only because
            # the first argument is no longer positional.
            parameter.replace(kind=inspect.Parameter.KEYWORD_ONLY)
            for parameter in old_parameters[1:]
        ]
        new_signature = old_signature.replace(parameters=new_parameters)
        setattr(route.endpoint, "__signature__", new_signature)
        setattr(route.endpoint, "_serve_cls", cls)
        new_router.routes.append(route)
    fastapi_app.include_router(new_router)

    routes_to_remove = list()
    for route in fastapi_app.routes:
        if not isinstance(route, APIRoute):
            continue

        # If there is a response model, FastAPI creates a copy of the fields.
        # But FastAPI creates the field incorrectly by missing the outer_type_.
        if route.response_model:
            original_resp_fields = (
                route.response_field.outer_type_.__fields__)
            cloned_resp_fields = (
                route.secure_cloned_response_field.outer_type_.__fields__)
            for key, field in cloned_resp_fields.items():
                field.outer_type_ = original_resp_fields[key].outer_type_

        # Remove endpoints that belong to other class based views.
        serve_cls = getattr(route.endpoint, "_serve_cls", None)
        if serve_cls is not None and serve_cls != cls:
            routes_to_remove.append(route)
    fastapi_app.routes[:] = [
        r for r in fastapi_app.routes if r not in routes_to_remove
    ]
