"""
Optional utils module contains utility methods
that require optional dependencies.
"""
from aiohttp.web import Response
import asyncio
import collections
import functools
import inspect
import json
import logging
import os
import time
import traceback
from collections import namedtuple
from typing import Any, Callable

import ray
import ray.dashboard.consts as dashboard_consts
from ray.ray_constants import env_bool

try:
    create_task = asyncio.create_task
except AttributeError:
    create_task = asyncio.ensure_future

# All third-party dependencies that are not included in the minimal Ray
# installation must be included in this file. This allows us to determine if
# the agent has the necessary dependencies to be started.
from ray.dashboard.optional_deps import aiohttp, hdrs, PathLike, RouteDef
from ray.dashboard.utils import to_google_style, CustomEncoder

logger = logging.getLogger(__name__)

RAY_INTERNAL_DASHBOARD_NAMESPACE = "_ray_internal_dashboard"


class ClassMethodRouteTable:
    """A helper class to bind http route to class method."""

    _bind_map = collections.defaultdict(dict)
    _routes = aiohttp.web.RouteTableDef()

    class _BindInfo:
        def __init__(self, filename, lineno, instance):
            self.filename = filename
            self.lineno = lineno
            self.instance = instance

    @classmethod
    def routes(cls):
        return cls._routes

    @classmethod
    def bound_routes(cls):
        bound_items = []
        for r in cls._routes._items:
            if isinstance(r, RouteDef):
                route_method = getattr(r.handler, "__route_method__")
                route_path = getattr(r.handler, "__route_path__")
                instance = cls._bind_map[route_method][route_path].instance
                if instance is not None:
                    bound_items.append(r)
            else:
                bound_items.append(r)
        routes = aiohttp.web.RouteTableDef()
        routes._items = bound_items
        return routes

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
                    return rest_response(success=False, message=traceback.format_exc())

            cls._bind_map[method][path] = bind_info
            _handler_route.__route_method__ = method
            _handler_route.__route_path__ = path
            return cls._routes.route(method, path, **kwargs)(_handler_route)

        return _wrapper

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

    @classmethod
    def bind(cls, instance):
        def predicate(o):
            if inspect.ismethod(o):
                return hasattr(o, "__route_method__") and hasattr(o, "__route_path__")
            return False

        handler_routes = inspect.getmembers(instance, predicate)
        for _, h in handler_routes:
            cls._bind_map[h.__func__.__route_method__][
                h.__func__.__route_path__
            ].instance = instance


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
    )


# The cache value type used by aiohttp_cache.
_AiohttpCacheValue = namedtuple("AiohttpCacheValue", ["data", "expiration", "task"])
# The methods with no request body used by aiohttp_cache.
_AIOHTTP_CACHE_NOBODY_METHODS = {hdrs.METH_GET, hdrs.METH_DELETE}


def aiohttp_cache(
    ttl_seconds=dashboard_consts.AIOHTTP_CACHE_TTL_SECONDS,
    maxsize=dashboard_consts.AIOHTTP_CACHE_MAX_SIZE,
    enable=not env_bool(dashboard_consts.AIOHTTP_CACHE_DISABLE_ENVIRONMENT_KEY, False),
):
    assert maxsize > 0
    cache = collections.OrderedDict()

    def _wrapper(handler):
        if enable:

            @functools.wraps(handler)
            async def _cache_handler(*args) -> aiohttp.web.Response:
                # Make the route handler as a bound method.
                # The args may be:
                #   * (Request, )
                #   * (self, Request)
                req = args[-1]
                # Make key.
                if req.method in _AIOHTTP_CACHE_NOBODY_METHODS:
                    key = req.path_qs
                else:
                    key = (req.path_qs, await req.read())
                # Query cache.
                value = cache.get(key)
                if value is not None:
                    cache.move_to_end(key)
                    if not value.task.done() or value.expiration >= time.time():
                        # Update task not done or the data is not expired.
                        return aiohttp.web.Response(**value.data)

                def _update_cache(task):
                    try:
                        response = task.result()
                    except Exception:
                        response = rest_response(
                            success=False, message=traceback.format_exc()
                        )
                    data = {
                        "status": response.status,
                        "headers": dict(response.headers),
                        "body": response.body,
                    }
                    cache[key] = _AiohttpCacheValue(
                        data, time.time() + ttl_seconds, task
                    )
                    cache.move_to_end(key)
                    if len(cache) > maxsize:
                        cache.popitem(last=False)
                    return response

                task = create_task(handler(*args))
                task.add_done_callback(_update_cache)
                if value is None:
                    return await task
                else:
                    return aiohttp.web.Response(**value.data)

            suffix = f"[cache ttl={ttl_seconds}, max_size={maxsize}]"
            _cache_handler.__name__ += suffix
            _cache_handler.__qualname__ += suffix
            return _cache_handler
        else:
            return handler

    if inspect.iscoroutinefunction(ttl_seconds):
        target_func = ttl_seconds
        ttl_seconds = dashboard_consts.AIOHTTP_CACHE_TTL_SECONDS
        return _wrapper(target_func)
    else:
        return _wrapper


def init_ray_and_catch_exceptions(connect_to_serve: bool = False) -> Callable:
    """Decorator to be used on methods that require being connected to Ray."""

    def decorator_factory(f: Callable) -> Callable:
        @functools.wraps(f)
        async def decorator(self, *args, **kwargs):
            try:
                if not ray.is_initialized():
                    try:
                        address = self._dashboard_head.gcs_address
                        logger.info(f"Connecting to ray with address={address}")
                        ray.init(
                            address=address,
                            namespace=RAY_INTERNAL_DASHBOARD_NAMESPACE,
                        )
                    except Exception as e:
                        ray.shutdown()
                        raise e from None

                if connect_to_serve:
                    from ray import serve

                    serve.start(detached=True, _override_controller_namespace="serve")

                return await f(self, *args, **kwargs)
            except Exception as e:
                logger.exception(f"Unexpected error in handler: {e}")
                return Response(
                    text=traceback.format_exc(),
                    status=aiohttp.web.HTTPInternalServerError.status_code,
                )

        return decorator

    return decorator_factory
