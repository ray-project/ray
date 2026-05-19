import os
import sys

import pytest

import ray
from ray.util.placement_group import placement_group, placement_group_table

NODE_ID_LABEL = "ray.io/node-id"
RACK_LABEL = "rack_id"
ONE = "1"
TWO = "2"
rack1_labels = {RACK_LABEL: ONE}
rack2_labels = {RACK_LABEL: TWO}


def test_topology_strategy_feasible_after_rack_kill(ray_start_cluster):
    """Verify topology-aware rescheduling after total rack failure.

    Creates a PG on rack 1 (the only available rack at the time). After
    removing one rack 1 node, the PG enters RESCHEDULING but stays pinned to
    rack 1, so it remains infeasible even though rack 2 has capacity. Once
    all rack 1 nodes are removed (total failure), the topology assignment
    is cleared and the PG reschedules onto rack 2.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)

    rack1_nodes = [cluster.add_node(num_cpus=1, labels=rack1_labels) for _ in range(4)]

    def assert_pg_nodes_label_value(cluster_nodes, pg, label, value):
        node_id_to_labels = {node["NodeID"]: node["Labels"] for node in cluster_nodes}
        for node_id in placement_group_table(pg)["bundles_to_node_id"].values():
            assert node_id_to_labels[node_id].get(label) == value

    bundles = [{"CPU": 1}] * 4

    pg = placement_group(
        bundles=bundles,
        topology_strategy=[{RACK_LABEL: "STRICT_PACK"}],
    )
    ray.get(pg.ready(), timeout=30)
    assert placement_group_table(pg)["state"] == "CREATED"
    assert_pg_nodes_label_value(ray.nodes(), pg, RACK_LABEL, ONE)

    # Bring rack 2 online; PG should still be pinned to rack 1.
    for _ in range(4):
        cluster.add_node(num_cpus=1, labels=rack2_labels)

    # Drop one rack 1 node -> partial failure -> RESCHEDULING + infeasible.
    cluster.remove_node(rack1_nodes[0])
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(pg.ready(), timeout=5)
    assert placement_group_table(pg)["state"] == "RESCHEDULING"

    # Total rack 1 failure -> clears assignment -> reschedules onto rack 2.
    for node in rack1_nodes[1:]:
        cluster.remove_node(node)

    ray.get(pg.ready(), timeout=30)
    assert placement_group_table(pg)["state"] == "CREATED"
    assert_pg_nodes_label_value(ray.nodes(), pg, RACK_LABEL, TWO)


def test_topology_strategy_strict_pack(ray_start_cluster):
    """Testing STRICT_PACK on the node level and STRICT_PACK on the rack level"""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)

    cluster.add_node(num_cpus=4, labels=rack1_labels)

    def assert_pg_nodes_label_value(cluster_nodes, pg, label, value):
        node_id_to_labels = {node["NodeID"]: node["Labels"] for node in cluster_nodes}
        for node_id in placement_group_table(pg)["bundles_to_node_id"].values():
            assert node_id_to_labels[node_id].get(label) == value

    bundles = [{"CPU": 1}] * 4

    pg = placement_group(
        bundles=bundles,
        topology_strategy=[{NODE_ID_LABEL: "STRICT_PACK", RACK_LABEL: "STRICT_PACK"}],
    )
    ray.get(pg.ready(), timeout=30)
    assert placement_group_table(pg)["state"] == "CREATED"
    assert_pg_nodes_label_value(ray.nodes(), pg, RACK_LABEL, ONE)


def test_topology_strategy_strict_spread(ray_start_cluster):
    """Testing STRICT_SPREAD on the node level and STRICT_PACK on the rack level"""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)

    for _ in range(4):
        cluster.add_node(num_cpus=1, labels=rack1_labels)

    def assert_pg_nodes_label_value(cluster_nodes, pg, label, value):
        node_id_to_labels = {node["NodeID"]: node["Labels"] for node in cluster_nodes}
        for node_id in placement_group_table(pg)["bundles_to_node_id"].values():
            assert node_id_to_labels[node_id].get(label) == value

    bundles = [{"CPU": 1}] * 4

    pg = placement_group(
        bundles=bundles,
        topology_strategy=[{NODE_ID_LABEL: "STRICT_SPREAD", RACK_LABEL: "STRICT_PACK"}],
    )
    ray.get(pg.ready(), timeout=30)
    assert placement_group_table(pg)["state"] == "CREATED"
    assert_pg_nodes_label_value(ray.nodes(), pg, RACK_LABEL, ONE)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
