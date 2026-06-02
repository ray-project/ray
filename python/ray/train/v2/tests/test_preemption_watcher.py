"""Unit tests for the preemption watcher.

These exercise :class:`PreemptionWatcher` directly, without standing up a Ray
cluster. The watcher is used as a plain Python class (not a Ray actor); its
poll loop runs in a background thread.
"""
import time
from typing import Callable, Dict
from unittest.mock import patch

import pytest

from ray.train.v2._internal.execution.preemption import PreemptionWatcher

_PREEMPTION_MOD = "ray.train.v2._internal.execution.preemption"


def _wait_for(predicate: Callable[[], bool], timeout: float = 2.0) -> bool:
    """Poll ``predicate`` until true or ``timeout`` elapses."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


class _FakeDrainSource:
    """Synchronous drain source that counts how many times it's been polled."""

    def __init__(self, value: Dict[str, int]):
        self._value = value
        self.calls = 0

    def __call__(self) -> Dict[str, int]:
        self.calls += 1
        return dict(self._value)


def _make_watcher(
    node_to_ranks: Dict[str, list],
    fd_map: Dict[str, list],
    drain_source: Callable[[], Dict[str, int]],
    poll_interval_s: float = 0.01,
) -> PreemptionWatcher:
    """Construct a watcher with a fixed failure-domain map.

    The poll loop starts in ``__init__``, so the FD map must be in place
    before construction. We patch ``_build_failure_domain_map`` (which would
    otherwise hit ``ray.nodes()``) for the duration of construction only.
    """
    fd = {nid: sorted(ranks) for nid, ranks in fd_map.items()}
    with patch.object(PreemptionWatcher, "_build_failure_domain_map", return_value=fd):
        return PreemptionWatcher(
            node_to_ranks=node_to_ranks,
            poll_interval_s=poll_interval_s,
            drain_source=drain_source,
        )


# ---------------------------------------------------------------------------
# PreemptionWatcher poll loop
# ---------------------------------------------------------------------------


class TestPreemptionWatcher:
    def test_no_drain_means_no_info(self):
        source = _FakeDrainSource({})
        watcher = _make_watcher(
            node_to_ranks={"node-a": [0], "node-b": [1]},
            fd_map={"node-a": [0], "node-b": [1]},
            drain_source=source,
        )
        assert _wait_for(lambda: source.calls >= 2)
        watcher.stop()

        assert watcher.get_latest_info() is None

    def test_single_node_drain_captures_info(self):
        now_ms = int(time.time() * 1000)
        source = _FakeDrainSource({"node-a": now_ms + 30_000})
        watcher = _make_watcher(
            node_to_ranks={"node-a": [0, 1], "node-b": [2, 3]},
            fd_map={"node-a": [0, 1], "node-b": [2, 3]},
            drain_source=source,
        )
        assert _wait_for(lambda: watcher.get_latest_info() is not None)
        watcher.stop()

        info = watcher.get_latest_info()
        assert info.preempted_node_ids == ["node-a"]
        assert info.preempted_ranks == [0, 1]
        assert info.deadline == pytest.approx((now_ms + 30_000) / 1000.0)

    def test_failure_domain_expansion(self):
        """A drain on one node expands to all ranks in the same FD."""
        now_ms = int(time.time() * 1000)
        source = _FakeDrainSource({"slice-a-host-0": now_ms + 30_000})
        # Simulate slice-a fate-sharing across host-0 and host-1.
        watcher = _make_watcher(
            node_to_ranks={
                "slice-a-host-0": [0, 1],
                "slice-a-host-1": [2, 3],
                "slice-b-host-0": [4, 5],
            },
            fd_map={
                "slice-a-host-0": [0, 1, 2, 3],
                "slice-a-host-1": [0, 1, 2, 3],
                "slice-b-host-0": [4, 5],
            },
            drain_source=source,
        )
        assert _wait_for(lambda: watcher.get_latest_info() is not None)
        watcher.stop()

        info = watcher.get_latest_info()
        assert info.preempted_ranks == [0, 1, 2, 3]
        assert info.preempted_node_ids == ["slice-a-host-0"]

    def test_drain_on_slice_mate_we_dont_host(self):
        """Staggered drain: a slice-mate not in our WG drains first.

        TPU slices are reclaimed atomically, so a drain on a slice host we
        don't own must still register.
        """
        now_ms = int(time.time() * 1000)
        source = _FakeDrainSource({"slice-a-host-1": now_ms + 30_000})
        watcher = _make_watcher(
            node_to_ranks={"slice-a-host-0": [0, 1]},
            fd_map={
                "slice-a-host-0": [0, 1],
                "slice-a-host-1": [0, 1],
            },
            drain_source=source,
        )
        assert _wait_for(lambda: watcher.get_latest_info() is not None)
        watcher.stop()

        info = watcher.get_latest_info()
        assert info.preempted_node_ids == ["slice-a-host-1"]
        assert info.preempted_ranks == [0, 1]

    def test_drain_on_unrelated_node_ignored(self):
        """A drain on a node we don't have workers on is a no-op."""
        now_ms = int(time.time() * 1000)
        source = _FakeDrainSource({"some-other-cluster-node": now_ms + 30_000})
        watcher = _make_watcher(
            node_to_ranks={"node-a": [0, 1]},
            fd_map={"node-a": [0, 1]},
            drain_source=source,
        )
        assert _wait_for(lambda: source.calls >= 2)
        watcher.stop()

        assert watcher.get_latest_info() is None

    def test_multiple_nodes_drain_aggregates(self):
        """Drain on multiple of our nodes aggregates ranks + earliest deadline."""
        now_ms = int(time.time() * 1000)
        d_early = now_ms + 20_000
        d_late = now_ms + 45_000
        source = _FakeDrainSource({"node-a": d_late, "node-b": d_early})
        watcher = _make_watcher(
            node_to_ranks={"node-a": [0, 1], "node-b": [2, 3]},
            fd_map={"node-a": [0, 1], "node-b": [2, 3]},
            drain_source=source,
        )
        assert _wait_for(lambda: watcher.get_latest_info() is not None)
        watcher.stop()

        info = watcher.get_latest_info()
        assert info.preempted_node_ids == ["node-a", "node-b"]
        assert info.preempted_ranks == [0, 1, 2, 3]
        # Earliest deadline wins.
        assert info.deadline == pytest.approx(d_early / 1000.0)

    def test_zero_deadline_resolves_to_unknown(self):
        """Ray Core deadline_ms=0 surfaces as 0.0 (unknown)."""
        source = _FakeDrainSource({"node-a": 0})
        watcher = _make_watcher(
            node_to_ranks={"node-a": [0]},
            fd_map={"node-a": [0]},
            drain_source=source,
        )
        assert _wait_for(lambda: watcher.get_latest_info() is not None)
        watcher.stop()

        info = watcher.get_latest_info()
        assert info.deadline == 0.0

    def test_stop_terminates_loop_promptly(self):
        source = _FakeDrainSource({})
        # Long poll interval: stop() must interrupt the inter-poll wait
        # rather than blocking on it.
        watcher = _make_watcher(
            node_to_ranks={"node-a": [0]},
            fd_map={"node-a": [0]},
            drain_source=source,
            poll_interval_s=10.0,
        )
        assert _wait_for(lambda: source.calls >= 1)
        thread = watcher._monitor_thread
        watcher.stop()
        assert thread is not None and not thread.is_alive()

    def test_drain_source_exception_does_not_kill_loop(self):
        """A failing drain source should not bring down the watcher."""
        attempts = {"n": 0}

        def flaky_source() -> Dict[str, int]:
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError("transient")
            return {}

        watcher = _make_watcher(
            node_to_ranks={"node-a": [0]},
            fd_map={"node-a": [0]},
            drain_source=flaky_source,
        )
        assert _wait_for(lambda: attempts["n"] >= 3)
        watcher.stop()

        # Recovered after failures; no drain ever observed.
        assert watcher.get_latest_info() is None

    def test_idempotent_on_unchanged_drain_set(self):
        """A stable drain set is processed exactly once.

        Verified via object identity: a new ``PreemptionInfo`` is constructed
        only when the drained set changes, so the cached info stays the same
        instance across repeated polls of an unchanged drain.
        """
        now_ms = int(time.time() * 1000)
        source = _FakeDrainSource({"node-a": now_ms + 30_000})
        watcher = _make_watcher(
            node_to_ranks={"node-a": [0, 1]},
            fd_map={"node-a": [0, 1]},
            drain_source=source,
        )
        assert _wait_for(lambda: watcher.get_latest_info() is not None)
        info1 = watcher.get_latest_info()

        # Let several more polls observe the same drain.
        calls_at_detection = source.calls
        assert _wait_for(lambda: source.calls >= calls_at_detection + 3)
        watcher.stop()

        assert watcher.get_latest_info() is info1


# ---------------------------------------------------------------------------
# _build_failure_domain_map
# ---------------------------------------------------------------------------


class TestBuildFailureDomainMap:
    def test_falls_back_on_ray_nodes_error(self, monkeypatch):
        """If ray.nodes() raises, fall back to per-node domains."""
        import ray

        def raise_runtime():
            raise RuntimeError("no ray runtime")

        monkeypatch.setattr(ray, "nodes", raise_runtime)
        result = PreemptionWatcher._build_failure_domain_map(
            {"node-a": [0, 1], "node-b": [2, 3]}
        )
        assert result == {"node-a": [0, 1], "node-b": [2, 3]}

    def test_non_tpu_cluster_is_per_node(self):
        """When no node carries a slice label, domains collapse to per-node."""
        import ray

        fake_nodes = [{"NodeID": "node-a"}, {"NodeID": "node-b"}]
        with patch.object(ray, "nodes", return_value=fake_nodes), patch(
            f"{_PREEMPTION_MOD}.get_tpu_slice_name_from_node", return_value=None
        ):
            result = PreemptionWatcher._build_failure_domain_map(
                {"node-a": [0, 1], "node-b": [2, 3]}
            )
        assert result == {"node-a": [0, 1], "node-b": [2, 3]}

    def test_tpu_slice_unions_within_wg_nodes(self):
        """WG nodes that share a slice label union their ranks."""
        import ray

        fake_nodes = [
            {"NodeID": "slice-a-host-0"},
            {"NodeID": "slice-a-host-1"},
            {"NodeID": "non-tpu"},
        ]

        def fake_slice_label(node):
            return {
                "slice-a-host-0": "slice-a",
                "slice-a-host-1": "slice-a",
            }.get(node["NodeID"])

        with patch.object(ray, "nodes", return_value=fake_nodes), patch(
            f"{_PREEMPTION_MOD}.get_tpu_slice_name_from_node",
            side_effect=fake_slice_label,
        ):
            result = PreemptionWatcher._build_failure_domain_map(
                {
                    "slice-a-host-0": [0, 1],
                    "slice-a-host-1": [2, 3],
                    "non-tpu": [4],
                }
            )

        assert result["slice-a-host-0"] == [0, 1, 2, 3]
        assert result["slice-a-host-1"] == [0, 1, 2, 3]
        assert result["non-tpu"] == [4]

    def test_tpu_slice_includes_unhosted_slice_mates(self):
        """Slice-mates we don't host must appear in the FD map."""
        import ray

        fake_nodes = [
            {"NodeID": "slice-a-host-0"},  # ours
            {"NodeID": "slice-a-host-1"},  # someone else's
            {"NodeID": "slice-a-host-2"},  # someone else's
            {"NodeID": "slice-b-host-0"},  # unrelated slice
        ]

        def fake_slice_label(node):
            return {
                "slice-a-host-0": "slice-a",
                "slice-a-host-1": "slice-a",
                "slice-a-host-2": "slice-a",
                "slice-b-host-0": "slice-b",
            }.get(node["NodeID"])

        with patch.object(ray, "nodes", return_value=fake_nodes), patch(
            f"{_PREEMPTION_MOD}.get_tpu_slice_name_from_node",
            side_effect=fake_slice_label,
        ):
            result = PreemptionWatcher._build_failure_domain_map(
                {"slice-a-host-0": [0, 1]}
            )

        assert result["slice-a-host-0"] == [0, 1]
        assert result["slice-a-host-1"] == [0, 1]
        assert result["slice-a-host-2"] == [0, 1]
        assert "slice-b-host-0" not in result


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
