import json
import logging
import random
import string
import time
import io
import os

import requests
from pygments import formatters, highlight, lexers
from ray.serve.context import FakeFlaskRequest, TaskContext
from ray.serve.http_util import build_flask_request
import itertools
import numpy as np

try:
    import pydantic
except ImportError:
    pydantic = None


def expand(l):
    """
    Implements a nested flattening of a list.
    Example:
    >>> serve.utils.expand([1,2,[3,4,5],6])
    [1,2,3,4,5,6]
    >>> serve.utils.expand(["a", ["b", "c"], "d", ["e", "f"]])
    ["a", "b", "c", "d", "e", "f"]
    """
    return list(
        itertools.chain.from_iterable(
            [x if isinstance(x, list) else [x] for x in l]))


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
                "HTTP server not ready after {} retries.".format(num_retries))


def get_random_letters(length=6):
    return "".join(random.choices(string.ascii_letters, k=length))
