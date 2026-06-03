"""Unit tests for the preemption watcher."""
from typing import Callable, Dict
from unittest.mock import Mock, patch

import pytest

from ray.train.v2._internal.execution.preemption import PreemptionWatcher

_PREEMPTION_MOD = "ray.train.v2._internal.execution.preemption"


def _make_watcher(
    node_to_ranks: Dict[str, list],
    fd_map: Dict[str, list],
    drain_source: Callable[[], Dict[str, int]],
) -> PreemptionWatcher:
    """Construct a watcher with a fixed failure-domain map, then halt its loop.

    The background poll thread is stopped so tests can drive ``_poll_once()``
    synchronously and deterministically.
    """
    fd = {nid: sorted(ranks) for nid, ranks in fd_map.items()}
    with patch.object(PreemptionWatcher, "_build_failure_domain_map", return_value=fd):
        watcher = PreemptionWatcher(
            node_to_ranks=node_to_ranks,
            drain_source=drain_source,
        )
    # Halt the background poll thread so the test can drive _poll_once() itself.
    watcher._stop_event.set()
    watcher._monitor_thread.join(timeout=5)
    return watcher


class TestPreemptionWatcher:
    @pytest.mark.parametrize(
        "fd_map, drained, exp_nodes, exp_ranks, exp_deadline_ms",
        [
            # Single drained node.
            ({"node-a": [0, 1]}, {"node-a": 30_000}, ["node-a"], [0, 1], 30_000),
            # Drains on nodes outside our failure domains are filtered out.
            (
                {"node-a": [0, 1]},
                {"node-a": 30_000, "other": 30_000},
                ["node-a"],
                [0, 1],
                30_000,
            ),
            # Multiple of our nodes: ranks aggregated, earliest deadline wins.
            (
                {"node-a": [0, 1], "node-b": [2, 3]},
                {"node-a": 45_000, "node-b": 20_000},
                ["node-a", "node-b"],
                [0, 1, 2, 3],
                20_000,
            ),
            # Failure-domain-expanded map: a drain on one host flags the whole
            # fate-shared set (e.g. a TPU slice).
            (
                {"host-0": [0, 1, 2, 3], "host-1": [0, 1, 2, 3]},
                {"host-0": 30_000},
                ["host-0"],
                [0, 1, 2, 3],
                30_000,
            ),
            # deadline_ms=0 is preserved as 0 (unknown), per Ray Core's convention.
            ({"node-a": [0]}, {"node-a": 0}, ["node-a"], [0], 0),
            # A None deadline is treated as unknown (0) rather than raising.
            ({"node-a": [0]}, {"node-a": None}, ["node-a"], [0], 0),
        ],
    )
    def test_drain_produces_info(
        self, fd_map, drained, exp_nodes, exp_ranks, exp_deadline_ms
    ):
        watcher = _make_watcher(
            node_to_ranks=fd_map, fd_map=fd_map, drain_source=lambda: dict(drained)
        )
        watcher._poll_once()

        info = watcher.get_latest_info()
        assert info.preempted_node_ids == exp_nodes
        assert info.preempted_ranks == exp_ranks
        assert info.deadline_ms == exp_deadline_ms

    def test_no_drain_means_no_info(self):
        watcher = _make_watcher(
            node_to_ranks={"node-a": [0]},
            fd_map={"node-a": [0]},
            drain_source=lambda: {},
        )
        watcher._poll_once()
        assert watcher.get_latest_info() is None

    def test_poll_swallows_drain_source_errors(self):
        """A raising drain source must not propagate out of a poll."""
        watcher = _make_watcher(
            node_to_ranks={"node-a": [0]},
            fd_map={"node-a": [0]},
            drain_source=Mock(side_effect=RuntimeError("transient")),
        )
        watcher._poll_once()  # must not raise
        assert watcher.get_latest_info() is None

    def test_none_drain_source_is_safe(self):
        """A drain source returning None is treated as no drains."""
        watcher = _make_watcher(
            node_to_ranks={"node-a": [0]},
            fd_map={"node-a": [0]},
            drain_source=lambda: None,
        )
        watcher._poll_once()  # must not raise
        assert watcher.get_latest_info() is None


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

    def test_falls_back_on_slice_lookup_error(self):
        """If the TPU slice lookup raises, fall back to per-node domains."""
        import ray

        with patch.object(ray, "nodes", return_value=[{"NodeID": "node-a"}]), patch(
            f"{_PREEMPTION_MOD}.get_tpu_slice_name_from_node",
            side_effect=RuntimeError("boom"),
        ):
            result = PreemptionWatcher._build_failure_domain_map({"node-a": [0, 1]})
        assert result == {"node-a": [0, 1]}

    @pytest.mark.parametrize(
        "node_to_ranks, slice_labels, cluster_node_ids, expected",
        [
            # GPU cluster (no slice labels): per-node domains. node-1 may be
            # shared with another workload, but only our ranks are in
            # node_to_ranks, so a drain on node-1 flags only our rank there.
            (
                {"node-0": [0, 1, 2, 3], "node-1": [4]},
                {},
                ["node-0", "node-1"],
                {"node-0": [0, 1, 2, 3], "node-1": [4]},
            ),
            # TPU slice fully occupied by this job: a drain on any host is
            # fate-shared, so every host maps to the union of the slice's ranks.
            (
                {"sa-0": [0, 1], "sa-1": [2, 3]},
                {"sa-0": "A", "sa-1": "A"},
                ["sa-0", "sa-1"],
                {"sa-0": [0, 1, 2, 3], "sa-1": [0, 1, 2, 3]},
            ),
        ],
    )
    def test_build_failure_domain_map(
        self, node_to_ranks, slice_labels, cluster_node_ids, expected
    ):
        import ray

        fake_nodes = [{"NodeID": nid} for nid in cluster_node_ids]

        def fake_slice_label(node):
            return slice_labels.get(node["NodeID"])

        with patch.object(ray, "nodes", return_value=fake_nodes), patch(
            f"{_PREEMPTION_MOD}.get_tpu_slice_name_from_node",
            side_effect=fake_slice_label,
        ):
            result = PreemptionWatcher._build_failure_domain_map(node_to_ranks)

        assert result == expected


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
