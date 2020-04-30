import asyncio
from functools import wraps
import inspect
import json
import logging
import random
import string
import time
import io
import os

import ray
import requests
from pygments import formatters, highlight, lexers
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
            return o.tolist()
        return super().default(o)


def pformat_color_json(d):
    """Use pygments to pretty format and colroize dictionary"""
    formatted_json = json.dumps(d, sort_keys=True, indent=4)

    colorful_json = highlight(formatted_json, lexers.JsonLexer(),
                              formatters.TerminalFormatter())

    return colorful_json


def block_until_http_ready(http_endpoint, num_retries=5, backoff_time_s=1):
    http_is_ready = False
    retries = num_retries

    while not http_is_ready:
        try:
            resp = requests.get(http_endpoint)
            assert resp.status_code == 200
            http_is_ready = True
        except Exception:
            pass

        # Exponential backoff
        time.sleep(backoff_time_s)
        backoff_time_s *= 2

        retries -= 1
        if retries == 0:
            raise Exception(
                "HTTP proxy not ready after {} retries.".format(num_retries))


def get_random_letters(length=6):
    return "".join(random.choices(string.ascii_letters, k=length))


def async_retryable(cls):
    """Make all actor method invocations on the class retryable.

    Note: This will retry actor_handle.method_name.remote(), but it must
    be invoked in an async context.

    Usage:
        @ray.remote(max_reconstructions=10000)
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


def _callable_accepts_batch(func_or_class):
    if inspect.isfunction(func_or_class):
        return hasattr(func_or_class, "_serve_accept_batch")
    elif inspect.isclass(func_or_class):
        return hasattr(func_or_class.__call__, "_serve_accept_batch")


def validate_replica_config(func_or_class,
                            *actor_init_args,
                            max_batch_size=1,
                            ray_actor_options=None):
    config = {}

    # Validate that func_or_class is a function or class.
    if inspect.isfunction(func_or_class):
        if len(actor_init_args) != 0:
            raise ValueError(
                "actor_init_args not supported for function backend.")
    elif not inspect.isclass(func_or_class):
        raise TypeError(
            "Backend must be a function or class, it is {}.".format(
                type(func_or_class)))
    else:
        config["func_or_class"] = func_or_class

    config["actor_init_args"] = list(actor_init_args)

    if not isinstance(max_batch_size, int):
        raise TypeError("max_batch_size must be an integer.")
    elif max_batch_size < 1:
        raise ValueError("max_batch_size must be >= 1.")
    else:
        config["max_batch_size"] = max_batch_size

    if not _callable_accepts_batch(func_or_class) and max_batch_size > 1:
        raise ValueError("Tried to set max_batch_size > 1 but the callable "
                         "isn't marked with @serve.accept_batch.")

    if ray_actor_options is None:
        config["ray_actor_options"] = {}
    elif not isinstance(ray_actor_options, dict):
        raise TypeError("ray_actor_options must be a dictionary.")
    else:
        if ray_actor_options.get("num_cpus", 0) < 0:
            raise ValueError("num_cpus in ray_actor_options must be >= 0.")
        if ray_actor_options.get("num_gpus", 0) < 0:
            raise ValueError("num_gpus in ray_actor_options must be >= 0.")
        if not isinstance(ray_actor_options.get("resources", {}), dict):
            raise TypeError(
                "resources in ray_actor_options must be a dictionary.")
        config["ray_actor_options"] = ray_actor_options

    return config
