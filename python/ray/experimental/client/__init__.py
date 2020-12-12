from ray.experimental.client.api import ClientAPI
from ray.experimental.client.api import APIImpl
from typing import Optional, List, Tuple
from contextlib import contextmanager

import logging

logger = logging.getLogger(__name__)

# About these global variables: Ray 1.0 uses exported module functions to
# provide its API, and we need to match that. However, we want different
# behaviors depending on where, exactly, in the client stack this is running.
#
# The reason for these differences depends on what's being pickled and passed
# to functions, or functions inside functions. So there are three cases to care
# about
#
# (Python Client)-->(Python ClientServer)-->(Internal Raylet Process)
#
# * _client_api should be set if we're inside the client
# * _server_api should be set if we're inside the clientserver
# * Both will be set if we're running both (as in a test)
# * Neither should be set if we're inside the raylet (but we still need to shim
#       from the client API surface to the Ray API)
#
# The job of RayAPIStub (below) delegates to the appropriate one of these
# depending on what's set or not. Then, all users importing the ray object
# from this package get the stub which routes them to the appropriate APIImpl.
_client_api: Optional[APIImpl] = None
_server_api: Optional[APIImpl] = None

# The reason for _is_server is a hack around the above comment while running
# tests. If we have both a client and a server trying to control these static
# variables then we need a way to decide which to use. In this case, both
# _client_api and _server_api are set.
# This boolean flips between the two
_is_server: bool = False


@contextmanager
def stash_api_for_tests(in_test: bool):
    global _is_server
    is_server = _is_server
    if in_test:
        _is_server = True
    yield _server_api
    if in_test:
        _is_server = is_server


def _set_client_api(val: Optional[APIImpl]):
    global _client_api
    global _is_server
    if _client_api is not None:
        raise Exception("Trying to set more than one client API")
    _client_api = val
    _is_server = False


def _set_server_api(val: Optional[APIImpl]):
    global _server_api
    global _is_server
    if _server_api is not None:
        raise Exception("Trying to set more than one server API")
    _server_api = val
    _is_server = True


def reset_api():
    global _client_api
    global _server_api
    global _is_server
    _client_api = None
    _server_api = None
    _is_server = False


def _get_client_api() -> APIImpl:
    global _client_api
    global _server_api
    global _is_server
    api = None
    if _is_server:
        api = _server_api
    else:
        api = _client_api
    if api is None:
        # We're inside a raylet worker
        from ray.experimental.client.server.core_ray_api import CoreRayAPI
        return CoreRayAPI()
    return api


class RayAPIStub:
    def connect(self,
                conn_str: str,
                secure: bool = False,
                metadata: List[Tuple[str, str]] = None,
                stub=None):
        from ray.experimental.client.worker import Worker
        _client_worker = Worker(
            conn_str, secure=secure, metadata=metadata, stub=stub)
        _set_client_api(ClientAPI(_client_worker))

    def disconnect(self):
        global _client_api
        if _client_api is not None:
            _client_api.close()
        _client_api = None

    def __getattr__(self, key: str):
        global _get_client_api
        api = _get_client_api()
        return getattr(api, key)


ray = RayAPIStub()

# Someday we might add methods in this module so that someone who
# tries to `import ray_client as ray` -- as a module, instead of
# `from ray_client import ray` -- as the API stub
# still gets expected functionality. This is the way the ray package
# worked in the past.
#
# This really calls for PEP 562: https://www.python.org/dev/peps/pep-0562/
# But until Python 3.6 is EOL, here we are.
