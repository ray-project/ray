from ray.experimental.client.api import ClientAPI
from ray.experimental.client.api import APIImpl
from typing import Optional

import logging

logger = logging.getLogger(__name__)

_client_api: Optional[APIImpl] = None


def get(*args, **kwargs):
    global _client_api
    if _client_api is None:
        from ray.experimental.client.core_ray_api import CoreRayAPI
        logger.info("No client API initialized: probably a worker, using core ray")
        _client_api = CoreRayAPI()
    return _client_api.get(*args, **kwargs)


def put(*args, **kwargs):
    global _client_api
    if _client_api is None:
        from ray.experimental.client.core_ray_api import CoreRayAPI
        logger.info("No client API initialized: probably a worker, using core ray")
        _client_api = CoreRayAPI()
    return _client_api.put(*args, **kwargs)


def remote(*args, **kwargs):
    global _client_api
    if _client_api is None:
        from ray.experimental.client.core_ray_api import CoreRayAPI
        logger.info("No client API initialized: probably a worker, using core ray")
        _client_api = CoreRayAPI()
    return _client_api.remote(*args, **kwargs)


def call_remote(f, *args, **kwargs):
    global _client_api
    if _client_api is None:
        from ray.experimental.client.core_ray_api import CoreRayAPI
        logger.info("No client API initialized: probably a worker, using core ray")
        _client_api = CoreRayAPI()
    return _client_api.call_remote(f, *args, **kwargs)


def connect(conn_str):
    global _client_api
    from ray.experimental.client.worker import Worker
    _client_worker = Worker(conn_str)
    _client_api = ClientAPI(_client_worker)


def disconnect():
    global _client_api
    _client_api.close()
    _client_api = None


def _set_client_api(api: Optional[APIImpl]):
    global _client_api
    _client_api = api
