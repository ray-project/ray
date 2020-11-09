from ray.experimental.client.worker import Worker
from ray.experimental.client.api import ClientAPI
from ray.experimental.client.api import APIImpl
from typing import Optional

_client_api: Optional[APIImpl] = None


def get(*args, **kwargs):
    global _client_api
    if _client_api is None:
        raise Exception("No client API initialized")
    return _client_api.get(*args, **kwargs)


def put(*args, **kwargs):
    global _client_api
    if _client_api is None:
        raise Exception("No client API initialized")
    return _client_api.put(*args, **kwargs)


def remote(*args, **kwargs):
    global _client_api
    if _client_api is None:
        raise Exception("No client API initialized")
    return _client_api.remote(*args, **kwargs)


def call_remote(f, *args, **kwargs):
    global _client_api
    if _client_api is None:
        raise Exception("No client API initialized")
    return _client_api.call_remote(*args, **kwargs)


def connect(conn_str):
    global _client_api
    _client_worker = Worker(conn_str)
    _client_api = ClientAPI(_client_worker)


def disconnect():
    global _client_api
    _client_api.close()
    _client_api = None


def _set_client_api(api: Optional[APIImpl]):
    global _client_api
    _client_api = api
