"""
Optional utils module contains utility methods
that require optional dependencies.
"""
import asyncio
import collections
import functools
import inspect
import logging
import os
import time
import traceback
from collections import namedtuple
from typing import Callable, Union

from aiohttp.web import Request, Response

import ray
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.utils as dashboard_utils
from ray._private.ray_constants import env_bool
from ray._raylet import RAY_INTERNAL_DASHBOARD_NAMESPACE

# All third-party dependencies that are not included in the minimal Ray
# installation must be included in this file. This allows us to determine if
# the agent has the necessary dependencies to be started.
from ray.dashboard.optional_deps import aiohttp, hdrs
from ray.dashboard.routes import method_route_table_factory, rest_response
from ray.dashboard.utils import (
    DashboardAgentModule,
    DashboardHeadModule,
)

try:
    create_task = asyncio.create_task
except AttributeError:
    create_task = asyncio.ensure_future


logger = logging.getLogger(__name__)

DashboardHeadRouteTable = method_route_table_factory()
DashboardAgentRouteTable = method_route_table_factory()


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
                # If nocache=1 in query string, bypass cache.
                if req.query.get("nocache") == "1":
                    return await handler(*args)

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
                            status_code=dashboard_utils.HTTPStatusCode.INTERNAL_ERROR,
                            message=traceback.format_exc(),
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


def is_browser_request(req: Request) -> bool:
    """Best-effort detection if the request was made by a browser.

    Uses three heuristics:
        1) If the `User-Agent` header starts with 'Mozilla'. This heuristic is weak,
        but hard for a browser to bypass e.g., fetch/xhr and friends cannot alter the
        user agent, but requests made with an HTTP library can stumble into this if
        they choose to user a browser-like user agent. At the time of writing, all
        common browsers' user agents start with 'Mozilla'.
        2) If any of the `Sec-Fetch-*` headers are present.
        3) If any of the various CORS headers are present
    """
    return req.headers.get("User-Agent", "").startswith("Mozilla") or any(
        h in req.headers
        for h in (
            # Origin and Referer are sent by browser user agents to give
            # information about the requesting origin
            "Referer",
            "Origin",
            # Sec-Fetch headers are sent with many but not all `fetch`
            # requests, and will eventually be sent on all requests.
            "Sec-Fetch-Mode",
            "Sec-Fetch-Dest",
            "Sec-Fetch-Site",
            "Sec-Fetch-User",
            # CORS headers specifying which other headers are modified
            "Access-Control-Request-Method",
            "Access-Control-Request-Headers",
        )
    )


def deny_browser_requests() -> Callable:
    """Reject any requests that appear to be made by a browser."""

    def decorator_factory(f: Callable) -> Callable:
        @functools.wraps(f)
        async def decorator(self, req: Request):
            if is_browser_request(req):
                return Response(
                    text="Browser requests not allowed",
                    status=aiohttp.web.HTTPMethodNotAllowed.status_code,
                )

            return await f(self, req)

        return decorator

    return decorator_factory


def init_ray_and_catch_exceptions() -> Callable:
    """Decorator to be used on methods that require being connected to Ray."""

    def decorator_factory(f: Callable) -> Callable:
        @functools.wraps(f)
        async def decorator(
            self: Union[DashboardAgentModule, DashboardHeadModule], *args, **kwargs
        ):
            try:
                if not ray.is_initialized():
                    try:
                        address = self.gcs_address
                        logger.info(f"Connecting to ray with address={address}")
                        # Set the gcs rpc timeout to shorter
                        os.environ["RAY_gcs_server_request_timeout_seconds"] = str(
                            dashboard_consts.GCS_RPC_TIMEOUT_SECONDS
                        )
                        # Init ray without logging to driver
                        # to avoid infinite logging issue.
                        ray.init(
                            address=address,
                            log_to_driver=False,
                            configure_logging=False,
                            namespace=RAY_INTERNAL_DASHBOARD_NAMESPACE,
                            _skip_env_hook=True,
                        )
                    except Exception as e:
                        ray.shutdown()
                        raise e from None
                return await f(self, *args, **kwargs)
            except Exception as e:
                logger.exception(f"Unexpected error in handler: {e}")
                return Response(
                    text=traceback.format_exc(),
                    status=aiohttp.web.HTTPInternalServerError.status_code,
                )

        return decorator

    return decorator_factory
