import json
import logging
import random
import string
import time

import requests
from pygments import formatters, highlight, lexers


def _get_logger():
    logger = logging.getLogger("ray.serve")
    # TODO(simon): Make logging level configurable.
    logger.setLevel(logging.INFO)
    return logger


logger = _get_logger()


class BytesEncoder(json.JSONEncoder):
    """Allow bytes to be part of the JSON document.

    BytesEncoder will walk the JSON tree and decode bytes with utf-8 codec.

    Example:
    >>> json.dumps({b'a': b'c'}, cls=BytesEncoder)
    '{"a":"c"}'
    """

    def default(self, o):  # pylint: disable=E0202
        if isinstance(o, bytes):
            return o.decode("utf-8")
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
