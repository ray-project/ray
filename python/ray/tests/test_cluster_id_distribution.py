"""Tests that node-spawned processes receive the cluster ID through their
environment (RAY_CLUSTER_ID) instead of each fetching it from the GCS — an
extra RPC that can time out while a large cluster brings up."""

import sys
from unittest import mock

import pytest

import ray._private.ray_constants as ray_constants
import ray._private.services as services

CLUSTER_ID_HEX = "a" * 56


def _spawn_env(func, *args, **kwargs):
    with mock.patch.object(services, "start_ray_process") as start_ray_process:
        func(*args, **kwargs)
    return start_ray_process.call_args.kwargs.get("env_updates")


def test_start_log_monitor_exports_cluster_id():
    env = _spawn_env(
        services.start_log_monitor,
        "/tmp/session",
        "/tmp/session/logs",
        "127.0.0.1:6379",
        "127.0.0.1",
        cluster_id_hex=CLUSTER_ID_HEX,
    )
    assert env == {ray_constants.RAY_CLUSTER_ID_ENVIRONMENT_VARIABLE: CLUSTER_ID_HEX}


def test_start_log_monitor_without_cluster_id():
    env = _spawn_env(
        services.start_log_monitor,
        "/tmp/session",
        "/tmp/session/logs",
        "127.0.0.1:6379",
        "127.0.0.1",
    )
    assert not env


@pytest.mark.parametrize("autoscaler_v2", [False, True])
def test_start_monitor_exports_cluster_id(autoscaler_v2):
    env = _spawn_env(
        services.start_monitor,
        "127.0.0.1:6379",
        "/tmp/session/logs",
        autoscaler_v2=autoscaler_v2,
        cluster_id_hex=CLUSTER_ID_HEX,
    )
    assert env == {ray_constants.RAY_CLUSTER_ID_ENVIRONMENT_VARIABLE: CLUSTER_ID_HEX}


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
