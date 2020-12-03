from ray.experimental.client.api import ClientAPI
from ray.experimental.client.api import APIImpl
from typing import Optional, List, Tuple
from contextlib import contextmanager

import logging

logger = logging.getLogger(__name__)

# _client_api has to be external to the API stub, below.
# Otherwise, ray.remote() that contains ray.remote()
# contains a reference to the RayAPIStub, therefore a
# reference to the _client_api, and then tries to pickle
# the thing.
_client_api: Optional[APIImpl] = None

_server_api: Optional[APIImpl] = None

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
