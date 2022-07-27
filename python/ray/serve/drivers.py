import inspect
from abc import abstractmethod
from typing import Any, Callable, Optional, Type, Union, Dict
from pydantic import BaseModel
from ray.serve._private.utils import install_serve_encoders_to_fastapi
from ray.util.annotations import DeveloperAPI, PublicAPI

import starlette
from fastapi import Body, Depends, FastAPI

from ray._private.utils import import_attr
from ray.serve.deployment_graph import RayServeDAGHandle
from ray.serve._private.http_util import ASGIHTTPSender
from ray.serve.handle import RayServeLazySyncHandle
from ray.serve.exceptions import RayServeException
from ray import serve

DEFAULT_HTTP_ADAPTER = "ray.serve.http_adapters.starlette_request"
HTTPAdapterFn = Callable[[Any], Any]


def _load_http_adapter(
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


@DeveloperAPI
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
        http_adapter = _load_http_adapter(http_adapter)
        self.app = FastAPI()

        @self.app.get("/")
        @self.app.post("/")
        async def handle_request(
            request: starlette.requests.Request, inp=Depends(http_adapter)
        ):
            resp = await self.predict(inp)
            return resp

    @abstractmethod
    async def predict(self, inp, route_path):
        raise NotImplementedError()

    async def __call__(self, request: starlette.requests.Request):
        # NOTE(simon): This is now duplicated from ASGIAppWrapper because we need to
        # generate FastAPI on the fly, we should find a way to unify the two.
        sender = ASGIHTTPSender()
        await self.app(request.scope, receive=request.receive, send=sender)
        return sender.build_asgi_response()


@PublicAPI(stability="beta")
@serve.deployment(route_prefix="/")
class DAGDriver:
    def __init__(
        self,
        dags: Union[RayServeDAGHandle, Dict[str, RayServeDAGHandle]],
        http_adapter: Optional[Union[str, Callable]] = None,
    ):
        install_serve_encoders_to_fastapi()
        http_adapter = load_http_adapter(http_adapter)
        self.app = FastAPI()

        if isinstance(dags, dict):
            self.dags = dags
            for route in dags:

                @self.app.get(f"{route}")
                @self.app.post(f"{route}")
                async def handle_request(
                    request: starlette.requests.Request, inp=Depends(http_adapter)
                ):
                    return await self.multi_dag_predict(request.url.path, inp)

        else:
            assert isinstance(dags, (RayServeDAGHandle, RayServeLazySyncHandle))
            self.dag = dags

            @self.app.get(f"/")
            @self.app.post(f"/")
            async def handle_request(inp=Depends(http_adapter)):
                return await self.predict(inp)

    async def __call__(self, request: starlette.requests.Request):
        # NOTE(simon): This is now duplicated from ASGIAppWrapper because we need to
        # generate FastAPI on the fly, we should find a way to unify the two.
        sender = ASGIHTTPSender()
        await self.app(request.scope, receive=request.receive, send=sender)
        return sender.build_asgi_response()

    async def predict(self, *args, **kwargs):
        """Perform inference directly without HTTP."""
        return await self.dag.remote(*args, **kwargs)

    async def multi_dag_predict(self, route_path, *args, **kwargs):
        """Perform inference directly without HTTP for multi dags."""
        if self.dags is None:
            raise RayServeException("No dags route found")
        if route_path not in self.dags:
            raise RayServeException(f"{route_path} does not exist in dags routes")
        return await self.dags[route_path].remote(*args, **kwargs)
