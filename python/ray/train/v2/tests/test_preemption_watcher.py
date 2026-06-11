"""Unit tests for the preemption watcher."""
from typing import Dict
from unittest.mock import patch

import pytest

import ray
from ray.train.v2._internal.execution.preemption import PreemptionWatcher

_PREEMPTION_MOD = "ray.train.v2._internal.execution.preemption"


def _make_watcher(
    node_to_ranks: Dict[str, list],
    fd_map: Dict[str, list],
) -> PreemptionWatcher:
    """Construct a watcher with a fixed failure-domain map, then halt its loop.

    The background poll thread is stopped (while the GCS drain call is mocked
    out) so tests can drive ``_poll_once()`` synchronously and deterministically.
    """
    fd = {nid: sorted(ranks) for nid, ranks in fd_map.items()}
    with patch.object(
        PreemptionWatcher, "_build_failure_domain_map", return_value=fd
    ), patch(f"{_PREEMPTION_MOD}._get_draining_nodes", return_value={}):
        watcher = PreemptionWatcher(node_to_ranks=node_to_ranks)
        watcher._stop_event.set()
        watcher._monitor_thread.join(timeout=5)
    return watcher


def _poll_once_with(watcher: PreemptionWatcher, **patch_kwargs) -> None:
    """Drive one poll with the GCS drain call mocked (``return_value``/``side_effect``)."""
    with patch(f"{_PREEMPTION_MOD}._get_draining_nodes", **patch_kwargs):
        watcher._poll_once()


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
            # deadline_ms=0 from Ray Core means "no deadline" -> surfaced as None.
            ({"node-a": [0]}, {"node-a": 0}, ["node-a"], [0], None),
            # A None deadline is also surfaced as None rather than raising.
            ({"node-a": [0]}, {"node-a": None}, ["node-a"], [0], None),
        ],
    )
    def test_drain_produces_info(
        self, fd_map, drained, exp_nodes, exp_ranks, exp_deadline_ms
    ):
        watcher = _make_watcher(node_to_ranks=fd_map, fd_map=fd_map)
        _poll_once_with(watcher, return_value=dict(drained))

        info = watcher.get_latest_preemption_info()
        assert info.preempted_node_ids == exp_nodes
        assert info.preempted_ranks == exp_ranks
        assert info.deadline_ms == exp_deadline_ms
        # The node->ranks map keys are the preempted nodes; each maps to its
        # failure domain. The flat getters above derive from it.
        assert info.preempted_node_to_ranks == {n: sorted(fd_map[n]) for n in exp_nodes}

    def test_no_drain_means_no_info(self):
        watcher = _make_watcher(node_to_ranks={"node-a": [0]}, fd_map={"node-a": [0]})
        _poll_once_with(watcher, return_value={})
        assert watcher.get_latest_preemption_info() is None

    def test_poll_swallows_drain_errors(self):
        """A raising drain call must not propagate out of a poll."""
        watcher = _make_watcher(node_to_ranks={"node-a": [0]}, fd_map={"node-a": [0]})
        _poll_once_with(
            watcher, side_effect=RuntimeError("transient")
        )  # must not raise
        assert watcher.get_latest_preemption_info() is None

    def test_none_drain_result_is_safe(self):
        """A drain call returning None is treated as no drains."""
        watcher = _make_watcher(node_to_ranks={"node-a": [0]}, fd_map={"node-a": [0]})
        _poll_once_with(watcher, return_value=None)  # must not raise
        assert watcher.get_latest_preemption_info() is None


class TestBuildFailureDomainMap:
    def test_falls_back_on_ray_nodes_error(self, monkeypatch):
        """If ray.nodes() raises, fall back to per-node domains."""

        def raise_runtime():
            raise RuntimeError("no ray runtime")

        monkeypatch.setattr(ray, "nodes", raise_runtime)
        result = PreemptionWatcher._build_failure_domain_map(
            {"node-a": [0, 1], "node-b": [2, 3]}
        )
        assert result == {"node-a": [0, 1], "node-b": [2, 3]}

    def test_falls_back_on_slice_lookup_error(self):
        """If the TPU slice lookup raises, fall back to per-node domains."""
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
