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
def small_event_line_limit():
    os.environ["EVENT_READ_LINE_LENGTH_LIMIT"] = "1024"
    yield 1024
    os.environ.pop("EVENT_READ_LINE_LENGTH_LIMIT", None)


@pytest.fixture
def fast_gcs_failure_detection():
    # This ensures that these environment vars are not set.
    assert "GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR" not in os.environ
    assert "GCS_CHECK_ALIVE_INTERVAL_SECONDS" not in os.environ
    assert "GCS_RETRY_CONNECT_INTERVAL_SECONDS" not in os.environ

    os.environ["GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR"] = "2"
    os.environ["GCS_CHECK_ALIVE_INTERVAL_SECONDS"] = "1"
    os.environ["GCS_RETRY_CONNECT_INTERVAL_SECONDS"] = "1"
    os.environ["GCS_CHECK_ALIVE_RPC_TIMEOUT"] = "1"
    yield
    os.environ.pop("GCS_CHECK_ALIVE_MAX_COUNT_OF_RPC_ERROR", None)
    os.environ.pop("GCS_CHECK_ALIVE_INTERVAL_SECONDS", None)
    os.environ.pop("GCS_RETRY_CONNECT_INTERVAL_SECONDS", None)
    os.environ.pop("GCS_CHECK_ALIVE_RPC_TIMEOUT", None)


@pytest.fixture
def reduce_actor_cache():
    os.environ["RAY_DASHBOARD_MAX_ACTORS_TO_CACHE"] = "3"
    yield
    os.environ.pop("RAY_DASHBOARD_MAX_ACTORS_TO_CACHE", None)
