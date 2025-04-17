import os
import pathlib

import pytest

import ray.dashboard.modules  # noqa
from ray.tests.conftest import *  # noqa


@pytest.fixture
def enable_test_module():
    os.environ["RAY_DASHBOARD_MODULE_TEST"] = "true"
    import ray.dashboard.tests

    p = pathlib.Path(ray.dashboard.modules.__path__[0]) / "tests" / "__init__.py"
    p.touch()
    yield
    os.environ.pop("RAY_DASHBOARD_MODULE_TEST", None)
    p.unlink()


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
def fast_gcs_failure_detection(monkeypatch):
    monkeypatch.setenv("RAY_gcs_rpc_server_reconnect_timeout_s", "2")
    monkeypatch.setenv("GCS_CHECK_ALIVE_INTERVAL_SECONDS", "1")


@pytest.fixture
def reduce_actor_cache():
    os.environ["RAY_maximum_gcs_destroyed_actor_cached_count"] = "3"
    yield
    os.environ.pop("RAY_maximum_gcs_destroyed_actor_cached_count", None)
