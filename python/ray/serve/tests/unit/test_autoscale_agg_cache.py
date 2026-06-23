"""Unit tests for the get_total_num_requests aggregation cache
(RAY_SERVE_AUTOSCALE_AGG_CACHE).

Default (TTL > 0, currently 1s): bounded-staleness cross-loop caching. A changed
``_metrics_version`` may still return the cached aggregation until the TTL elapses.
This keeps the cache warm under streaming reports (the version bumps on every
ingested report), which is where the control-loop perf win comes from -- at the cost
of an autoscaling total up to TTL seconds stale.

TTL == 0: a version-only cache -- reused only while ``_metrics_version`` is unchanged
(captures the >=2 within-loop reads: the policy + the decision log) and recomputed
the instant any metric changes, so the total is never stale. Used where an exact
fresh total is required (e.g. the frozen-time autoscaling logic tests).
"""
import pytest

import ray.serve._private.autoscaling_state as A
from ray.serve._private.autoscaling_state import DeploymentAutoscalingState
from ray.serve._private.common import DeploymentID


@pytest.fixture
def state(monkeypatch):
    monkeypatch.setattr(A, "RAY_SERVE_AUTOSCALE_AGG_CACHE", True)
    st = DeploymentAutoscalingState(DeploymentID(name="d", app_name="app"))
    calls = {"n": 0}

    def fake_compute():
        calls["n"] += 1
        return float(calls["n"])  # a distinct value per real recompute

    monkeypatch.setattr(st, "_compute_total_num_requests", fake_compute)
    st._calls = calls
    return st


def test_reuse_while_version_stable(state, monkeypatch):
    """No metric change -> computed exactly once, same value returned."""
    monkeypatch.setattr(A, "RAY_SERVE_AUTOSCALE_AGG_CACHE_TTL_S", 0.0)
    state._metrics_version = 7
    v = [state.get_total_num_requests() for _ in range(3)]
    assert v[0] == v[1] == v[2]
    assert state._calls["n"] == 1


def test_recomputes_on_version_change(state, monkeypatch):
    """A metric change bumps _metrics_version -> fresh recompute, never stale."""
    monkeypatch.setattr(A, "RAY_SERVE_AUTOSCALE_AGG_CACHE_TTL_S", 0.0)
    state._metrics_version = 1
    first = state.get_total_num_requests()
    state._metrics_version = 2  # e.g. record_request_metrics_* / on_replica_stopped
    second = state.get_total_num_requests()
    assert second != first
    assert state._calls["n"] == 2


def test_ttl_opt_in_serves_bounded_stale(state, monkeypatch):
    """TTL>0 may return a changed-version value until the window elapses."""
    monkeypatch.setattr(A, "RAY_SERVE_AUTOSCALE_AGG_CACHE_TTL_S", 1.0)
    clock = {"t": 1000.0}
    monkeypatch.setattr(A.time, "time", lambda: clock["t"])
    state._metrics_version = 1
    first = state.get_total_num_requests()  # compute @ t=1000
    state._metrics_version = 2  # version changed...
    clock["t"] = 1000.5  # ...still inside the TTL window
    assert state.get_total_num_requests() == first  # bounded-stale value served
    assert state._calls["n"] == 1
    clock["t"] = 1001.5  # TTL elapsed
    assert state.get_total_num_requests() != first  # fresh recompute
    assert state._calls["n"] == 2


def test_disabled_always_recomputes(monkeypatch):
    """Flag off -> no caching, every call recomputes."""
    monkeypatch.setattr(A, "RAY_SERVE_AUTOSCALE_AGG_CACHE", False)
    st = DeploymentAutoscalingState(DeploymentID(name="d", app_name="app"))
    calls = {"n": 0}
    monkeypatch.setattr(
        st,
        "_compute_total_num_requests",
        lambda: calls.__setitem__("n", calls["n"] + 1) or float(calls["n"]),
    )
    st._metrics_version = 5
    st.get_total_num_requests()
    st.get_total_num_requests()
    assert calls["n"] == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
