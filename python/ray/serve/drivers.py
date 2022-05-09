import inspect
from abc import abstractmethod
from typing import Any, Callable, Optional, Type, Union
from pydantic import BaseModel
from ray.serve.utils import install_serve_encoders_to_fastapi

import starlette
from fastapi import Body, Depends, FastAPI

from ray._private.utils import import_attr
from ray.serve.deployment_graph import RayServeDAGHandle
from ray.serve.http_util import ASGIHTTPSender
from ray import serve

DEFAULT_HTTP_ADAPTER = "ray.serve.http_adapters.starlette_request"
HTTPAdapterFn = Callable[[Any], Any]


def load_http_adapter(
    http_adapter: Optional[Union[str, HTTPAdapterFn, Type[BaseModel]]]
) -> HTTPAdapterFn:
    if http_adapter is None:
        http_adapter = DEFAULT_HTTP_ADAPTER

    if isinstance(http_adapter, str):
        http_adapter = import_attr(http_adapter)

    if inspect.isclass(http_adapter) and issubclass(http_adapter, BaseModel):

        def http_adapter(inp: http_adapter = Body(...)):
            return inp

    if not inspect.isfunction(http_adapter):
        raise ValueError(
            "input schema must be a callable function or pydantic model class."
        )

    if any(
        param.annotation == inspect.Parameter.empty
        for param in inspect.signature(http_adapter).parameters.values()
    ):
        raise ValueError("input schema function's signature should be type annotated.")
    return http_adapter


class SimpleSchemaIngress:
    def __init__(
        self, http_adapter: Optional[Union[str, HTTPAdapterFn, Type[BaseModel]]] = None
    ):
        """Create a FastAPI endpoint annotated with http_adapter dependency.

        Args:
            http_adapter(str, HTTPAdapterFn, None, Type[pydantic.BaseModel]):
              The FastAPI input conversion function or a pydantic model class.
              By default, Serve will directly pass in the request object
              starlette.requests.Request. You can pass in any FastAPI dependency
              resolver. When you pass in a string, Serve will import it.
              Please refer to Serve HTTP adatper documentation to learn more.
        """
        install_serve_encoders_to_fastapi()
        http_adapter = load_http_adapter(http_adapter)
        self.app = FastAPI()

        @self.app.get("/")
        @self.app.post("/")
        async def handle_request(inp=Depends(http_adapter)):
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
        http_adapter: Optional[Union[str, Callable]] = None,
    ):
        self.dag_handle = dag_handle
        super().__init__(http_adapter)

    async def predict(self, *args, **kwargs):
        """Perform inference directly without HTTP."""
        return await self.dag_handle.remote(*args, **kwargs)
