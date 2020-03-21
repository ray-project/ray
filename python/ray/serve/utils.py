import json
import logging
import random
import string
import time
import io
import os
import socket
import array

import requests
from pygments import formatters, highlight, lexers
from ray.serve.context import FakeFlaskRequest, TaskContext
from ray.serve.http_util import build_flask_request
import itertools


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

        # http_body_bytes enclosed in list due to
        # https://github.com/ray-project/ray/issues/6944
        # TODO(alind):  remove list enclosing after issue is fixed
        flask_request = build_flask_request(asgi_scope,
                                            io.BytesIO(body_bytes[0]))
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


class UnixFileDescriptTransport:
    """This class faciliate sending file descriptors among processes."""

    def __init__(self, unix_domain_path):
        self.unix_domain_path = unix_domain_path

    def bind(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.bind(self.unix_domain_path)

    def send(self, fd: int, num_receivers: int):
        self.sock.listen(num_receivers)

        # The code was taken from python official doc
        # https://docs.python.org/3/library/socket.html#socket.socket.sendmsg
        for _ in range(num_receivers):
            conn, addr = self.sock.accept()
            conn.sendmsg([b""], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                  array.array("i", [fd]))])
        self.sock.close()
        os.unlink(self.unix_domain_path)

    def receive(self) -> int:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.unix_domain_path)

        # The following code was taken from python documentation
        # https://docs.python.org/3/library/socket.html#socket.socket.recvmsg
        fds = array.array("i")
        # We know the message size is 0. But still use buffer to receive it
        msg_length = 10
        msg, ancdata, flags, addr = sock.recvmsg(
            msg_length, socket.CMSG_LEN(1 * fds.itemsize))
        for cmsg_level, cmsg_type, cmsg_data in ancdata:
            assert cmsg_level == socket.SOL_SOCKET
            assert cmsg_type == socket.SCM_RIGHTS
            # Append data, ignoring any truncated integers at the end.
            fds.frombytes(
                cmsg_data[:len(cmsg_data) - (len(cmsg_data) % fds.itemsize)])
        fds = list(fds)
        return fds[0]
