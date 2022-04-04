import inspect
from abc import abstractmethod
from typing import Any, Callable, Optional, Union

import starlette
from fastapi import Depends, FastAPI

from ray._private.utils import import_attr
from ray.serve.deployment_graph import RayServeDAGHandle
from ray.serve.http_util import ASGIHTTPSender
from ray import serve

DEFAULT_INPUT_SCHEMA = "ray.serve.http_adapters.starlette_request"
InputSchemaFn = Callable[[Any], Any]


def load_input_schema(
    input_schema: Optional[Union[str, InputSchemaFn]]
) -> InputSchemaFn:
    if input_schema is None:
        input_schema = DEFAULT_INPUT_SCHEMA

    if isinstance(input_schema, str):
        input_schema = import_attr(input_schema)

    if not inspect.isfunction(input_schema):
        raise ValueError("input schema must be a callable function.")

    if any(
        param.annotation == inspect.Parameter.empty
        for param in inspect.signature(input_schema).parameters.values()
    ):
        raise ValueError("input schema function's signature should be type annotated.")
    return input_schema


class SimpleSchemaIngress:
    def __init__(self, input_schema: Optional[Union[str, InputSchemaFn]] = None):
        """Create a FastAPI endpoint annotated with input_schema dependency.

        Args:
            input_schema(str, InputSchemaFn, None): The FastAPI input conversion
              function. By default, Serve will directly pass in the request object
              starlette.requests.Request. You can pass in any FastAPI dependency
              resolver. When you pass in a string, Serve will import it.
              Please refer to Serve HTTP adatper documentation to learn more.
        """
        input_schema = load_input_schema(input_schema)
        self.app = FastAPI()

        @self.app.get("/")
        @self.app.post("/")
        async def handle_request(inp=Depends(input_schema)):
            resp = await self.predict(inp)
            return resp

    @abstractmethod
    async def predict(self, inp):
        raise NotImplementedError()

    async def __call__(self, request: starlette.requests.Request):
        # NOTE(simon): This is now duplicated from ASGIAppWrapper because we need to
        # generate FastAPI on the fly, we should find a way to unify the two.
        sender = ASGIHTTPSender()
        await self.app(request.scope, receive=request.receive, send=sender)
        return sender.build_asgi_response()


@serve.deployment(route_prefix="/")
class DAGDriver(SimpleSchemaIngress):
    def __init__(
        self,
        dag_handle: RayServeDAGHandle,
        *,
        input_schema: Optional[Union[str, Callable]] = None,
    ):
        self.dag_handle = dag_handle
        super().__init__(input_schema)

    async def predict(self, inp):
        """Perform inference directly without HTTP."""
        return await self.dag_handle.remote(inp)
