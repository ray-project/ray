import collections
import inspect

from ray.dashboard.optional_deps import aiohttp
from ray.dashboard.routes import BaseRouteTable
from ray.dashboard.subprocesses.handle import SubprocessModuleHandle
from ray.dashboard.subprocesses.utils import ResponseType


class SubprocessRouteTable(BaseRouteTable):
    """
    A route table to bind http route to SubprocessModuleHandle and SubprocessModule.

    This class is used in cls object: all the decorator methods are @classmethod, and
    the routes are binded to the cls object.

    Note we have 2 handlers:
    1. the child side handler, that is `handler` that contains real logic and
        is executed in the child process. It's added with __route_method__ and
        __route_path__ attributes. The child process runs a standalone aiohttp
        server.
    2. the parent side handler, that just sends the request to the
        SubprocessModuleHandle at cls._bind_map[method][path].instance.

    With modifications:
    - __route_method__ and __route_path__ are added to both side's handlers.
    - method and path are added to self._bind_map.

    Lifecycle of a request:
    1. Parent receives an aiohttp request.
    2. Router finds by [method][path] and calls parent_side_handler.
    3. `parent_side_handler` bookkeeps the request with a Future and sends a
            request to the subprocess.
    4. Subprocesses receives the response and sends it back to the parent.
    5. Parent responds to the aiohttp request with the response from the subprocess.
    """

    _bind_map = collections.defaultdict(dict)
    _routes = aiohttp.web.RouteTableDef()

    @classmethod
    def bind(cls, instance: "SubprocessModuleHandle"):
        # __route_method__ and __route_path__ are added to SubprocessModule's methods,
        # not the SubprocessModuleHandle's methods.
        def predicate(o):
            if inspect.isfunction(o):
                return hasattr(o, "__route_method__") and hasattr(o, "__route_path__")
            return False

        handler_routes = inspect.getmembers(instance.module_cls, predicate)
        for _, h in handler_routes:
            cls._bind_map[h.__route_method__][h.__route_path__].instance = instance

    @classmethod
    def _register_route(
        cls, method, path, resp_type: ResponseType = ResponseType.HTTP, **kwargs
    ):
        """
        Register a route to the module and return the decorated handler.
        """

        def _wrapper(handler):
            if path in cls._bind_map[method]:
                bind_info = cls._bind_map[method][path]
                raise Exception(
                    f"Duplicated route path: {path}, "
                    f"previous one registered at "
                    f"{bind_info.filename}:{bind_info.lineno}"
                )

            bind_info = cls._BindInfo(
                handler.__code__.co_filename, handler.__code__.co_firstlineno, None
            )

            cls._bind_map[method][path] = bind_info

            async def parent_side_handler(
                request: aiohttp.web.Request,
            ) -> aiohttp.web.Response:
                bind_info = cls._bind_map[method][path]
                subprocess_module_handle = bind_info.instance
                return await subprocess_module_handle.proxy_request(request, resp_type)

            # Used in bind().
            handler.__route_method__ = method
            handler.__route_path__ = path
            # Used in bound_routes().
            parent_side_handler.__route_method__ = method
            parent_side_handler.__route_path__ = path
            parent_side_handler.__name__ = handler.__name__

            cls._routes.route(method, path)(parent_side_handler)

            return handler

        return _wrapper
