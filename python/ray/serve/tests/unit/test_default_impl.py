"""Tests for `ray.serve._private.default_impl`.

These exercise the node-id/availability-zone lookup that runs when a handle
creates its router. Under Ray Client that lookup can't reach the cluster's GCS
server, so it must be skipped rather than blocking until timeout.
See https://github.com/ray-project/ray/issues/54601.
"""

from unittest.mock import MagicMock

import pytest

import ray
from ray.serve._private import default_impl


@pytest.fixture
def runtime_context(monkeypatch):
    """Stand-in for `ray.get_runtime_context()` outside of a live cluster."""
    context = MagicMock()
    context.get_node_id.return_value = "node-id-under-test"
    context.gcs_address = "127.0.0.1:6379"
    monkeypatch.setattr(ray, "get_runtime_context", lambda: context)
    return context


def test_get_node_id_and_az_skips_gcs_lookup_under_ray_client(
    monkeypatch, runtime_context
):
    """Under Ray Client the GCS-backed AZ lookup must not run.

    The client process is outside the cluster, so contacting the head node's
    GCS server would block for RAY_GCS_RPC_TIMEOUT_S on every new handle before
    falling back to `None` anyway.
    """
    monkeypatch.setattr(default_impl, "inside_ray_client_context", lambda: True)
    monkeypatch.setattr(default_impl, "GcsClient", MagicMock())

    create_cache = MagicMock(
        side_effect=AssertionError("GCS must not be contacted under Ray Client")
    )
    monkeypatch.setattr(default_impl, "create_cluster_node_info_cache", create_cache)

    node_id, az = default_impl._get_node_id_and_az()

    create_cache.assert_not_called()
    assert node_id == "node-id-under-test"
    assert az is None


def test_get_node_id_and_az_queries_gcs_without_ray_client(
    monkeypatch, runtime_context
):
    """Outside Ray Client the AZ is resolved from the GCS-backed cache."""
    monkeypatch.setattr(default_impl, "inside_ray_client_context", lambda: False)
    monkeypatch.setattr(default_impl, "GcsClient", MagicMock())

    cache = MagicMock()
    cache.get_node_az.return_value = "us-west-2a"
    create_cache = MagicMock(return_value=cache)
    monkeypatch.setattr(default_impl, "create_cluster_node_info_cache", create_cache)

    node_id, az = default_impl._get_node_id_and_az()

    create_cache.assert_called_once()
    cache.update.assert_called_once()
    cache.get_node_az.assert_called_once_with("node-id-under-test")
    assert node_id == "node-id-under-test"
    assert az == "us-west-2a"


def test_get_node_id_and_az_falls_back_on_gcs_error(monkeypatch, runtime_context):
    """A failing GCS lookup falls back to `az=None` instead of raising."""
    monkeypatch.setattr(default_impl, "inside_ray_client_context", lambda: False)
    monkeypatch.setattr(default_impl, "GcsClient", MagicMock())
    monkeypatch.setattr(
        default_impl,
        "create_cluster_node_info_cache",
        MagicMock(side_effect=RuntimeError("GCS unavailable")),
    )

    node_id, az = default_impl._get_node_id_and_az()

    assert node_id == "node-id-under-test"
    assert az is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
