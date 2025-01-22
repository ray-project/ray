import abc
import collections
import functools
import inspect
import json
import logging
import os
import traceback
from typing import Any

from ray.dashboard.optional_deps import PathLike, RouteDef, aiohttp, hdrs
from ray.dashboard.utils import CustomEncoder, to_google_style

logger = logging.getLogger(__name__)


class BaseRouteTable(abc.ABC):
    """A base class to bind http route to a target instance. Subclass should implement
    the _register_route method. It should define how the handler interacts with
    _BindInfo.instance.

    Subclasses must declare their own _bind_map and _routes properties to avoid
    conflicts.
    """

    class _BindInfo:
        def __init__(self, filename, lineno, instance):
            self.filename = filename
            self.lineno = lineno
            self.instance = instance

    @classmethod
    @property
    @abc.abstractmethod
    def _bind_map(cls):
        pass

    @classmethod
    @property
    @abc.abstractmethod
    def _routes(cls):
        pass

    @classmethod
    @abc.abstractmethod
    def _register_route(cls, method, path, **kwargs):
        pass

    @classmethod
    @abc.abstractmethod
    def bind(cls, instance):
        pass

    @classmethod
    def routes(cls):
        return cls._routes

    @classmethod
    def bound_routes(cls):
        bound_items = []
        for r in cls._routes._items:
            if isinstance(r, RouteDef):
                route_method = r.handler.__route_method__
                route_path = r.handler.__route_path__
                instance = cls._bind_map[route_method][route_path].instance
                if instance is not None:
                    bound_items.append(r)
            else:
                bound_items.append(r)
        routes = aiohttp.web.RouteTableDef()
        routes._items = bound_items
        return routes

    @classmethod
    def head(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_HEAD, path, **kwargs)

    @classmethod
    def get(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_GET, path, **kwargs)

    @classmethod
    def post(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_POST, path, **kwargs)

    @classmethod
    def put(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_PUT, path, **kwargs)

    @classmethod
    def patch(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_PATCH, path, **kwargs)

    @classmethod
    def delete(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_DELETE, path, **kwargs)

    @classmethod
    def view(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_ANY, path, **kwargs)

    @classmethod
    def static(cls, prefix: str, path: PathLike, **kwargs: Any) -> None:
        cls._routes.static(prefix, path, **kwargs)


def method_route_table_factory():
    """
    Return a method-based route table class, for in-process HeadModule objects.
    """

    class MethodRouteTable(BaseRouteTable):
        """A helper class to bind http route to class method. Each _BindInfo.instance
        is a class instance, and for an inbound request, we invoke the async handler
        method."""

        _bind_map = collections.defaultdict(dict)
        _routes = aiohttp.web.RouteTableDef()

        @classmethod
        def _register_route(cls, method, path, **kwargs):
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

                @functools.wraps(handler)
                async def _handler_route(*args) -> aiohttp.web.Response:
                    try:
                        # Make the route handler as a bound method.
                        # The args may be:
                        #   * (Request, )
                        #   * (self, Request)
                        req = args[-1]
                        return await handler(bind_info.instance, req)
                    except Exception:
                        logger.exception("Handle %s %s failed.", method, path)
                        return rest_response(
                            success=False, message=traceback.format_exc()
                        )

                cls._bind_map[method][path] = bind_info
                _handler_route.__route_method__ = method
                _handler_route.__route_path__ = path
                return cls._routes.route(method, path, **kwargs)(_handler_route)

            return _wrapper

        @classmethod
        def bind(cls, instance):
            def predicate(o):
                if inspect.ismethod(o):
                    return hasattr(o, "__route_method__") and hasattr(
                        o, "__route_path__"
                    )
                return False

            handler_routes = inspect.getmembers(instance, predicate)
            for _, h in handler_routes:
                cls._bind_map[h.__func__.__route_method__][
                    h.__func__.__route_path__
                ].instance = instance

    return MethodRouteTable


def rest_response(
    success, message, convert_google_style=True, **kwargs
) -> aiohttp.web.Response:
    # In the dev context we allow a dev server running on a
    # different port to consume the API, meaning we need to allow
    # cross-origin access
    if os.environ.get("RAY_DASHBOARD_DEV") == "1":
        headers = {"Access-Control-Allow-Origin": "*"}
    else:
        headers = {}
    return aiohttp.web.json_response(
        {
            "result": success,
            "msg": message,
            "data": to_google_style(kwargs) if convert_google_style else kwargs,
        },
        dumps=functools.partial(json.dumps, cls=CustomEncoder),
        headers=headers,
        status=200 if success else 500,
    )
