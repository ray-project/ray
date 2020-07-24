import asyncio
from functools import singledispatch
import json
import logging
import random
import string
import time
from typing import List
import io
import os

import requests

import ray
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


@ray.remote(num_cpus=0)
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


def format_actor_name(actor_name, instance_name=None):
    if instance_name is None:
        return actor_name
    else:
        return "{}:{}".format(instance_name, actor_name)


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


def try_schedule_resources_on_nodes(
        requirements: List[dict],
        ray_nodes: List = None,
) -> List[bool]:
    """Test given resource requirements can be scheduled on ray nodes.

    Args:
        requirements(List[dict]): The list of resource requirements.
        ray_nodes(Optional[List]): The list of nodes. By default it reads from
            ``ray.nodes()``.
    Returns:
        successfully_scheduled(List[bool]): A list with the same length as
            requirements. Each element indicates whether or not the requirement
            can be satisied.
    """

    if ray_nodes is None:
        ray_nodes = ray.nodes()

    node_to_resources = {
        node["NodeID"]: node["Resources"]
        for node in ray_nodes if node["Alive"]
    }

    successfully_scheduled = []

    for resource_dict in requirements:
        # Filter out zero value
        resource_dict = {k: v for k, v in resource_dict.items() if v > 0}

        for node_id, node_resource in node_to_resources.items():
            # Check if we can schedule on this node
            feasible = True
            for key, count in resource_dict.items():
                if node_resource.get(key, 0) - count < 0:
                    feasible = False

            # If we can, schedule it on this node
            if feasible:
                node_resource = node_to_resources[node_id]
                for key, count in resource_dict.items():
                    node_resource[key] -= count

                successfully_scheduled.append(True)
                break
        else:
            successfully_scheduled.append(False)

    return successfully_scheduled
