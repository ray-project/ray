import functools
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
from ray.serve.handle import RayServeDeploymentHandle
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


@PublicAPI(stability="beta")
@serve.deployment(route_prefix="/")
class DAGDriver:

    MATCH_ALL_ROUTE_PREFIX = "/{path:path}"

    def __init__(
        self,
        dags: Union[RayServeDAGHandle, Dict[str, RayServeDAGHandle]],
        http_adapter: Optional[Union[str, Callable]] = None,
    ):
        install_serve_encoders_to_fastapi()
        http_adapter = _load_http_adapter(http_adapter)
        self.app = FastAPI()

        if isinstance(dags, dict):
            self.dags = dags
            for route in dags.keys():

                def endpoint_create(route):
                    @self.app.get(f"{route}")
                    @self.app.post(f"{route}")
                    async def handle_request(inp=Depends(http_adapter)):
                        return await self.predict_with_route(
                            route, inp  # noqa: B023 function redefinition
                        )

                # bind current handle with endpoint creation function
                endpoint_create_func = functools.partial(endpoint_create, route)
                endpoint_create_func()

        else:
            assert isinstance(dags, (RayServeDAGHandle, RayServeDeploymentHandle))
            self.dags = {self.MATCH_ALL_ROUTE_PREFIX: dags}

            # Single dag case, we will receive all prefix route
            @self.app.get(self.MATCH_ALL_ROUTE_PREFIX)
            @self.app.post(self.MATCH_ALL_ROUTE_PREFIX)
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
        return await (
            await self.dags[self.MATCH_ALL_ROUTE_PREFIX].remote(*args, **kwargs)
        )

    async def predict_with_route(self, route_path, *args, **kwargs):
        """Perform inference directly without HTTP for multi dags."""
        if route_path not in self.dags:
            raise RayServeException(f"{route_path} does not exist in dags routes")
        return await (await self.dags[route_path].remote(*args, **kwargs))
