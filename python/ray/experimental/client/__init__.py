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


@contextmanager
def stash_api_for_tests(in_test: bool):
    api = None
    if in_test:
        api = stash_api()
    yield api
    if in_test:
        restore_api(api)


def stash_api() -> Optional[APIImpl]:
    global _client_api
    a = _client_api
    _client_api = None
    return a


def restore_api(api: Optional[APIImpl]):
    global _client_api
    _client_api = api


class RayAPIStub:
    def connect(self,
                conn_str: str,
                secure: bool = False,
                metadata: List[Tuple[str, str]] = None,
                stub=None):
        global _client_api
        from ray.experimental.client.worker import Worker
        _client_worker = Worker(
            conn_str, secure=secure, metadata=metadata, stub=stub)
        _client_api = ClientAPI(_client_worker)

    def disconnect(self):
        global _client_api
        if _client_api is not None:
            _client_api.close()
        _client_api = None

    def __getattr__(self, key: str):
        global _client_api
        self.__check_client_api()
        return getattr(_client_api, key)

    def __check_client_api(self):
        global _client_api
        if _client_api is None:
            from ray.experimental.client.server.core_ray_api import CoreRayAPI
            _client_api = CoreRayAPI()


ray = RayAPIStub()

# Someday we might add methods in this module so that someone who
# tries to `import ray_client as ray` -- as a module, instead of
# `from ray_client import ray` -- as the API stub
# still gets expected functionality. This is the way the ray package
# worked in the past.
#
# This really calls for PEP 562: https://www.python.org/dev/peps/pep-0562/
# But until Python 3.6 is EOL, here we are.
