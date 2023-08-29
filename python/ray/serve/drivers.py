import asyncio
import functools
import logging
import sys
from typing import Any, Callable, Optional, Union, Dict

from fastapi import Depends, FastAPI
import grpc

import ray
from ray import cloudpickle
from ray._private.utils import get_or_create_event_loop
from ray._private.tls_utils import add_port_to_grpc_server
from ray.util.annotations import PublicAPI

from ray import serve
from ray.serve.deployment_graph import RayServeDAGHandle
from ray.serve.drivers_utils import load_http_adapter
from ray.serve.exceptions import RayServeException
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.serve.handle import RayServeHandle
from ray.serve._private.constants import DEFAULT_GRPC_PORT, SERVE_LOGGER_NAME
from ray.serve._private.http_util import ASGIAppReplicaWrapper
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import install_serve_encoders_to_fastapi

logger = logging.getLogger(SERVE_LOGGER_NAME)


@PublicAPI(stability="beta")
@serve.deployment(route_prefix="/")
class DAGDriver(ASGIAppReplicaWrapper):
    """A driver implementation that accepts HTTP requests."""

    MATCH_ALL_ROUTE_PREFIX = "/{path:path}"

    def __init__(
        self,
        dags: Union[RayServeDAGHandle, Dict[str, RayServeDAGHandle]],
        http_adapter: Optional[Union[str, Callable]] = None,
    ):
        """Create a DAGDriver.

        Args:
            dags: a handle to a Ray Serve DAG or a dictionary of handles.
            http_adapter: a callable function or import string to convert
                HTTP requests to Ray Serve input.
        """

        ServeUsageTag.DAG_DRIVER_USED.record("1")
        if http_adapter is not None:
            ServeUsageTag.HTTP_ADAPTER_USED.record("1")

        install_serve_encoders_to_fastapi()
        http_adapter = load_http_adapter(http_adapter)
        app = FastAPI()

        if isinstance(dags, dict):
            self.dags = dags
            for route in dags.keys():

                def endpoint_create(route):
                    @app.get(f"{route}")
                    @app.post(f"{route}")
                    async def handle_request(inp=Depends(http_adapter)):
                        return await self.predict_with_route(
                            route, inp  # noqa: B023 function redefinition
                        )

                # bind current handle with endpoint creation function
                endpoint_create_func = functools.partial(endpoint_create, route)
                endpoint_create_func()

        else:
            assert isinstance(dags, (RayServeDAGHandle, RayServeHandle))
            self.dags = {self.MATCH_ALL_ROUTE_PREFIX: dags}

            # Single dag case, we will receive all prefix route
            @app.get(self.MATCH_ALL_ROUTE_PREFIX)
            @app.post(self.MATCH_ALL_ROUTE_PREFIX)
            async def handle_request(inp=Depends(http_adapter)):
                return await self.predict(inp)

        frozen_app = cloudpickle.loads(cloudpickle.dumps(app))
        super().__init__(frozen_app)

    async def predict(self, *args, _ray_cache_refs: bool = False, **kwargs):
        """Perform inference directly without HTTP."""
        dag = self.dags[self.MATCH_ALL_ROUTE_PREFIX]
        # `dag` may also be a vanilla `RayServeHandle`; in that case, it doesn't take
        # the `_ray_cache_refs` kwarg.
        if isinstance(dag, RayServeDAGHandle):
            kwargs["_ray_cache_refs"] = _ray_cache_refs

        return await (await dag.remote(*args, **kwargs))

    async def predict_with_route(self, route_path, *args, **kwargs):
        """Perform inference directly without HTTP for multi dags."""
        if route_path not in self.dags:
            raise RayServeException(f"{route_path} does not exist in dags routes")
        return await (await self.dags[route_path].remote(*args, **kwargs))

    async def get_intermediate_object_refs(self) -> Dict[str, Any]:
        """Gets latest cached object refs from latest call to predict().

        Gets the latest cached references to the results of the default executors on
        each node in the DAG found at self.MATCH_ALL_ROUTE_PREFIX. Should be called
        after predict() has been called with _cache_refs set to True.
        """
        dag_handle = self.dags[self.MATCH_ALL_ROUTE_PREFIX]
        root_dag_node = dag_handle.dag_node

        if root_dag_node is None:
            raise AssertionError(
                "Predict has not been called. Cannot retrieve intermediate object refs."
            )

        return await root_dag_node.get_object_refs_from_last_execute()

    async def get_pickled_dag_node(self) -> bytes:
        """Returns the serialized root dag node."""
        return self.dags[self.MATCH_ALL_ROUTE_PREFIX].pickled_dag_node


@PublicAPI(stability="alpha")
class gRPCIngress:
    """
    gRPC Ingress that starts gRPC server based on the port
    """

    def __init__(self, port: int = DEFAULT_GRPC_PORT):
        """Create a gRPC Ingress.

        Args:
            port: Set the port that the gRPC server will listen to.
        """

        self.server = grpc.aio.server()
        self.port = port

        self._attach_grpc_server_with_schema()

        self.setup_complete = asyncio.Event()
        self.running_task = get_or_create_event_loop().create_task(self.run())
        ServeUsageTag.GRPC_INGRESS_USED.record("1")

    async def run(self):
        """Start gRPC Server"""
        logger.info(
            "Starting gRPC server with on node:{} "
            "listening on port {}".format(ray.util.get_node_ip_address(), self.port)
        )
        address = "[::]:{}".format(self.port)
        try:
            # Depending on whether RAY_USE_TLS is on, `add_port_to_grpc_server`
            # can create a secure or insecure channel
            self.grpc_port = add_port_to_grpc_server(self.server, address)
        except Exception:
            # TODO(SongGuyang): Catch the exception here because there is
            # port conflict issue which brought from static port. We should
            # remove this after we find better port resolution.
            logger.exception(
                "Failed to add port to grpc server. GRPC service will be disabled"
            )
            self.server = None
            self.grpc_port = None

        self.setup_complete.set()
        await self.server.start()
        await self.server.wait_for_termination()

    def _attach_grpc_server_with_schema(self):
        """Attach the gRPC server with schema implementation

        Protobuf Schema gRPC should generate bind function
        (e.g. add_PredictAPIsServiceServicer_to_server) to bind gRPC server
        and schema interface
        """
        # protobuf Schema gRPC should generate bind function
        # (e.g. add_PredictAPIsServiceServicer_to_server) to bind gRPC server
        # and schema interface
        bind_function_name = "add_{}_to_server"
        for index in range(len(self.__class__.__bases__)):
            module_name = self.__class__.__bases__[index].__module__
            servicer_name = self.__class__.__bases__[index].__name__
            try:
                getattr(
                    sys.modules[module_name], bind_function_name.format(servicer_name)
                )(self, self.server)
                return
            except AttributeError:
                pass
        raise RayServeException(
            "Fail to attach the gRPC server with schema implementation"
        )


@serve.deployment(is_driver_deployment=True, ray_actor_options={"num_cpus": 0})
class DefaultgRPCDriver(serve_pb2_grpc.PredictAPIsServiceServicer, gRPCIngress):
    """
    gRPC Driver that responsible for redirecting the gRPC requests
    and hold dag handle
    """

    def __init__(self, dag: RayServeDAGHandle, port=DEFAULT_GRPC_PORT):
        """Create a grpc driver based on the PredictAPIsService schema.

        Args:
            dags: a handle to a Ray Serve DAG.
            port: Port to use to listen to receive the request
        """
        self.dag = dag
        # TODO(Sihan) we will add a gRPCOption class
        # once we have more options to use
        super().__init__(port)

    async def Predict(self, request, context):
        """
        gRPC Predict function implementation
        """
        res = await (await self.dag.remote(dict(request.input)))

        return serve_pb2.PredictResponse(prediction=res)
