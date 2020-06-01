import asyncio
from functools import wraps, singledispatch
import inspect
import json
import logging
import random
import string
import time
from typing import List
import io
import os

import ray
import requests
from pygments import formatters, highlight, lexers
from ray.serve.constants import HTTP_PROXY_TIMEOUT
from ray.serve.context import FakeFlaskRequest, TaskContext
from ray.serve.http_util import build_flask_request
import numpy as np

try:
    import pydantic
except ImportError:
    pydantic = None

ACTOR_FAILURE_RETRY_TIMEOUT_S = 60


def parse_request_item(request_item):
    if request_item.request_context == TaskContext.Web:
        is_web_context = True
        asgi_scope, body_bytes = request_item.request_args

        flask_request = build_flask_request(asgi_scope, io.BytesIO(body_bytes))
        args = (flask_request, )
        kwargs = {}
    else:
        is_web_context = False
        args = (FakeFlaskRequest(), )
        kwargs = request_item.request_kwargs

    return args, kwargs, is_web_context


def _get_logger():
    logger = logging.getLogger("ray.serve")
    # TODO(simon): Make logging level configurable.
    if os.environ.get("SERVE_LOG_DEBUG"):
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    return logger


logger = _get_logger()


class ServeEncoder(json.JSONEncoder):
    """Ray.Serve's utility JSON encoder. Adds support for:
        - bytes
        - Pydantic types
        - Exceptions
        - numpy.ndarray
    """

    def default(self, o):  # pylint: disable=E0202
        if isinstance(o, bytes):
            return o.decode("utf-8")
        if pydantic is not None and isinstance(o, pydantic.BaseModel):
            return o.dict()
        if isinstance(o, Exception):
            return str(o)
        if isinstance(o, np.ndarray):
            if o.dtype.kind == "f":  # floats
                o = o.astype(float)
            if o.dtype.kind in {"i", "u"}:  # signed and unsigned integers.
                o = o.astype(int)
            return o.tolist()
        return super().default(o)


def pformat_color_json(d):
    """Use pygments to pretty format and colroize dictionary"""
    formatted_json = json.dumps(d, sort_keys=True, indent=4)

    colorful_json = highlight(formatted_json, lexers.JsonLexer(),
                              formatters.TerminalFormatter())

    return colorful_json


def block_until_http_ready(http_endpoint,
                           backoff_time_s=1,
                           timeout=HTTP_PROXY_TIMEOUT):
    http_is_ready = False
    start_time = time.time()

    while not http_is_ready:
        try:
            resp = requests.get(http_endpoint)
            assert resp.status_code == 200
            http_is_ready = True
        except Exception:
            pass

        if 0 < timeout < time.time() - start_time:
            raise TimeoutError(
                "HTTP proxy not ready after {} seconds.".format(timeout))

        time.sleep(backoff_time_s)


def get_random_letters(length=6):
    return "".join(random.choices(string.ascii_letters, k=length))


def async_retryable(cls):
    """Make all actor method invocations on the class retryable.

    Note: This will retry actor_handle.method_name.remote(), but it must
    be invoked in an async context.

    Usage:
        @ray.remote(max_restarts=10000)
        @async_retryable
        class A:
            pass
    """
    for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):

        def decorate_with_retry(f):
            @wraps(f)
            async def retry_method(*args, **kwargs):
                start = time.time()
                while time.time() - start < ACTOR_FAILURE_RETRY_TIMEOUT_S:
                    try:
                        return await f(*args, **kwargs)
                    except ray.exceptions.RayActorError:
                        logger.warning(
                            "Actor method '{}' failed, retrying after 100ms.".
                            format(name))
                        await asyncio.sleep(0.1)
                raise RuntimeError("Timed out after {}s waiting for actor "
                                   "method '{}' to succeed.".format(
                                       ACTOR_FAILURE_RETRY_TIMEOUT_S, name))

            return retry_method

        method.__ray_invocation_decorator__ = decorate_with_retry
    return cls


def retry_actor_failures(f, *args, **kwargs):
    start = time.time()
    while time.time() - start < ACTOR_FAILURE_RETRY_TIMEOUT_S:
        try:
            return ray.get(f.remote(*args, **kwargs))
        except ray.exceptions.RayActorError:
            logger.warning(
                "Actor method '{}' failed, retrying after 100ms".format(
                    f._method_name))
            time.sleep(0.1)
    raise RuntimeError("Timed out after {}s waiting for actor "
                       "method '{}' to succeed.".format(
                           ACTOR_FAILURE_RETRY_TIMEOUT_S, f._method_name))


async def retry_actor_failures_async(f, *args, **kwargs):
    start = time.time()
    while time.time() - start < ACTOR_FAILURE_RETRY_TIMEOUT_S:
        try:
            return await f.remote(*args, **kwargs)
        except ray.exceptions.RayActorError:
            logger.warning(
                "Actor method '{}' failed, retrying after 100ms".format(
                    f._method_name))
            await asyncio.sleep(0.1)
    raise RuntimeError("Timed out after {}s waiting for actor "
                       "method '{}' to succeed.".format(
                           ACTOR_FAILURE_RETRY_TIMEOUT_S, f._method_name))


def format_actor_name(actor_name, cluster_name=None):
    if cluster_name is None:
        return actor_name
    else:
        return "{}:{}".format(cluster_name, actor_name)


@singledispatch
def chain_future(src, dst):
    """Base method for chaining futures together.

    Chaining futures means the output from source future(s) are written as the
    results of the destination future(s). This method can work with the
    following inputs:
        - src: Future, dst: Future
        - src: List[Future], dst: List[Future]
    """
    raise NotImplementedError()


@chain_future.register(asyncio.Future)
def _chain_future_single(src: asyncio.Future, dst: asyncio.Future):
    asyncio.futures._chain_future(src, dst)


@chain_future.register(list)
def _chain_future_list(src: List[asyncio.Future], dst: List[asyncio.Future]):
    if len(src) != len(dst):
        raise ValueError(
            "Source and destination list doesn't have the same length. "
            "Source: {}. Destination: {}.".foramt(len(src), len(dst)))

    for s, d in zip(src, dst):
        chain_future(s, d)


def unpack_future(src: asyncio.Future, num_items: int) -> List[asyncio.Future]:
    """Unpack the result of source future to num_items futures.

    This function takes in a Future and splits its result into many futures. If
    the result of the source future is an exception, then all destination
    futures will have the same exception.
    """
    dest_futures = [
        asyncio.get_event_loop().create_future() for _ in range(num_items)
    ]

    def unwrap_callback(fut: asyncio.Future):
        exception = fut.exception()
        if exception is not None:
            [f.set_exception(exception) for f in dest_futures]
            return

        result = fut.result()
        assert len(result) == num_items
        for item, future in zip(result, dest_futures):
            future.set_result(item)

    src.add_done_callback(unwrap_callback)

    return dest_futures
