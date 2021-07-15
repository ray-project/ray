import os
import pytest
from ray.tests.conftest import *  # noqa


@pytest.fixture
def enable_test_module():
    os.environ["RAY_DASHBOARD_MODULE_TEST"] = "true"
    yield
    os.environ.pop("RAY_DASHBOARD_MODULE_TEST", None)


@pytest.fixture
def disable_aiohttp_cache():
    os.environ["RAY_DASHBOARD_NO_CACHE"] = "true"
    yield
    os.environ.pop("RAY_DASHBOARD_NO_CACHE", None)


@pytest.fixture
def set_http_proxy():
    http_proxy = os.environ.get("http_proxy", None)
    https_proxy = os.environ.get("https_proxy", None)

    # set http proxy
    os.environ["http_proxy"] = "www.example.com:990"
    os.environ["https_proxy"] = "www.example.com:990"

    yield

    # reset http proxy
    if http_proxy:
        os.environ["http_proxy"] = http_proxy
    else:
        del os.environ["http_proxy"]

    if https_proxy:
        os.environ["https_proxy"] = https_proxy
    else:
        del os.environ["https_proxy"]
