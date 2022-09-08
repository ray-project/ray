from unittest.mock import patch

import pytest

from ray.serve.config import DeploymentMode, HTTPOptions
from ray.serve._private.http_state import HTTPState


def test_node_selection():
    head_node_id = "node_id-index-head"

    def _make_http_state(http_options):
        return HTTPState(
            "mock_controller_name",
            detached=True,
            config=http_options,
            head_node_id=head_node_id,
            gcs_client=None,
            _start_proxies_on_init=False,
        )

    all_nodes = [(head_node_id, "fake-head-ip")] + [
        (f"worker-node-id-{i}", f"fake-worker-ip-{i}") for i in range(100)
    ]

    with patch("ray.serve._private.http_state.get_all_node_ids") as func:
        func.return_value = all_nodes

        # Test NoServer
        state = _make_http_state(HTTPOptions(location=DeploymentMode.NoServer))
        assert state._get_target_nodes() == []

        # Test HeadOnly
        state = _make_http_state(HTTPOptions(location=DeploymentMode.HeadOnly))
        assert state._get_target_nodes() == all_nodes[:1]

        # Test EveryNode
        state = _make_http_state(HTTPOptions(location=DeploymentMode.EveryNode))
        assert state._get_target_nodes() == all_nodes

        # Test FixedReplica
        state = _make_http_state(
            HTTPOptions(location=DeploymentMode.FixedNumber, fixed_number_replicas=5)
        )
        selected_nodes = state._get_target_nodes()

        # it should have selection a subset of 5 nodes.
        assert len(selected_nodes) == 5
        assert set(all_nodes).issuperset(set(selected_nodes))

        for _ in range(5):
            # The selection should be deterministic.
            assert selected_nodes == state._get_target_nodes()

        another_seed = _make_http_state(
            HTTPOptions(
                location=DeploymentMode.FixedNumber,
                fixed_number_replicas=5,
                fixed_number_selection_seed=42,
            )
        )._get_target_nodes()
        assert len(another_seed) == 5
        assert set(all_nodes).issuperset(set(another_seed))
        assert set(another_seed) != set(selected_nodes)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
