import logging

import pytest

import ray


@pytest.fixture(autouse=True)
def disallow_ray_init(monkeypatch):
    def raise_on_init():
        raise RuntimeError("Unit tests should not depend on Ray being initialized.")

    monkeypatch.setattr(ray, "init", raise_on_init)


@pytest.fixture
def propagate_logs():
    # Mirrors python/ray/tests/conftest.py::propagate_logs. The bazel target
    # for this directory does not pull the parent conftest, so unit tests that
    # use caplog against ray-namespaced loggers need a local copy.
    logging.getLogger("ray").propagate = True
    logging.getLogger("ray.data").propagate = True
    yield


@pytest.fixture
def capture_logger(caplog):
    """Fixture to attach caplog to module loggers and clean up on teardown."""
    from ray.tests.unit.passive_test_utils import attach_logger

    attached = []

    def _attach(logger_name: str, level: int = logging.INFO):
        logger = attach_logger(caplog, logger_name, level)
        attached.append(logger)
        return logger

    yield _attach

    for logger in attached:
        logger.removeHandler(caplog.handler)


@pytest.fixture
def leader_election_on(monkeypatch):
    """Enable GCS leader election for the test."""
    import ray._private.ray_constants as ray_constants

    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", True)


@pytest.fixture
def fast_registration_poll(monkeypatch):
    """Speed up GCS retry intervals across dashboard modules for fast testing."""
    import ray.dashboard.consts as dashboard_consts
    import ray.dashboard.modules.node.node_head as node_head_module

    monkeypatch.setattr(dashboard_consts, "GCS_REGISTER_RETRY_INTERVAL_S", 0.01)
    monkeypatch.setattr(node_head_module, "GCS_REGISTER_RETRY_INTERVAL_S", 0.01)


@pytest.fixture
def enable_passive_gcs(leader_election_on, fast_registration_poll):
    """Convenience fixture enabling leader election and fast registration polling."""
    pass


@pytest.fixture(autouse=True)
def reset_internal_kv():
    """Clean up internal_kv and usage_lib global state before and after each unit test."""
    import ray._common.usage.usage_lib as ray_usage_lib
    import ray.experimental.internal_kv as internal_kv

    ray_usage_lib.reset_global_state()
    yield
    internal_kv._internal_kv_reset()
    ray_usage_lib.reset_global_state()
