import os
import sys

import pytest

import ray
from ray.util.placement_group import placement_group, placement_group_table

NODE_ID_LABEL = "ray.io/node-id"
RACK_LABEL = "ray.io/gpu-domain"
ONE = "rack-1"
TWO = "rack-2"
rack1_labels = {RACK_LABEL: ONE}
rack2_labels = {RACK_LABEL: TWO}


def assert_pg_nodes_label_value(cluster_nodes, pg, label, value):
    node_id_to_labels = {node["NodeID"]: node["Labels"] for node in cluster_nodes}
    for node_id in placement_group_table(pg)["bundles_to_node_id"].values():
        assert node_id_to_labels[node_id].get(label) == value


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
    """Testing STRICT_PACK on the node level and STRICT_PACK on the rack level.

    Provides two candidate 4-CPU nodes on rack 1 so STRICT_PACK at the node
    level has a real choice to make; asserts that all bundles end up on a
    single node (validating the node-level packing) and on rack 1
    (validating the rack-level packing).
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)

    # Two candidate nodes — STRICT_PACK at the node level must pick one.
    cluster.add_node(num_cpus=4, labels=rack1_labels)
    cluster.add_node(num_cpus=4, labels=rack1_labels)

    bundles = [{"CPU": 1}] * 4

    pg = placement_group(
        bundles=bundles,
        topology_strategy=[{NODE_ID_LABEL: "STRICT_PACK", RACK_LABEL: "STRICT_PACK"}],
    )
    ray.get(pg.ready(), timeout=30)
    assert placement_group_table(pg)["state"] == "CREATED"
    assert_pg_nodes_label_value(ray.nodes(), pg, RACK_LABEL, ONE)

    # Verify STRICT_PACK at the node level: all bundles on the same node.
    bundle_nodes = set(placement_group_table(pg)["bundles_to_node_id"].values())
    assert len(bundle_nodes) == 1


def test_topology_strategy_strict_spread(ray_start_cluster):
    """Testing STRICT_SPREAD on the node level and STRICT_PACK on the rack level.

    Provides six rack-1 nodes for four bundles so STRICT_SPREAD at the node
    level has slack to choose from; asserts that each bundle lands on a
    distinct node (validating node-level spreading) and that all bundles
    share rack 1 (validating rack-level packing).
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)

    # Six rack-1 nodes for four bundles — STRICT_SPREAD has slack.
    for _ in range(6):
        cluster.add_node(num_cpus=1, labels=rack1_labels)

    bundles = [{"CPU": 1}] * 4

    pg = placement_group(
        bundles=bundles,
        topology_strategy=[{NODE_ID_LABEL: "STRICT_SPREAD", RACK_LABEL: "STRICT_PACK"}],
    )
    ray.get(pg.ready(), timeout=30)
    assert placement_group_table(pg)["state"] == "CREATED"
    assert_pg_nodes_label_value(ray.nodes(), pg, RACK_LABEL, ONE)

    # Verify STRICT_SPREAD at the node level: each bundle on a distinct node.
    bundle_nodes = list(placement_group_table(pg)["bundles_to_node_id"].values())
    assert len(bundle_nodes) == len(set(bundle_nodes)) == 4


def test_topology_strategy_reschedule_on_node_failure(ray_start_cluster):
    """Verify rescheduling stays within the same topology level on partial failure.

    Provides 6 rack-1 nodes for a 4-bundle PG. Kills 2 nodes holding bundles;
    asserts the PG re-creates with all bundles still on rack-1 rather than
    leaking onto a different domain.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)

    rack_nodes = [cluster.add_node(num_cpus=1, labels=rack1_labels) for _ in range(6)]

    bundles = [{"CPU": 1}] * 4
    pg = placement_group(
        bundles=bundles,
        topology_strategy=[{NODE_ID_LABEL: "PACK", RACK_LABEL: "STRICT_PACK"}],
    )
    ray.get(pg.ready(), timeout=30)
    assert placement_group_table(pg)["state"] == "CREATED"

    cluster.remove_node(rack_nodes[0])
    cluster.remove_node(rack_nodes[1])

    ray.get(pg.ready(), timeout=30)
    assert placement_group_table(pg)["state"] == "CREATED"
    assert_pg_nodes_label_value(ray.nodes(), pg, RACK_LABEL, ONE)


def test_topology_strategy_infeasible_after_node_kill(ray_start_cluster):
    """Verify PG enters RESCHEDULING when no surviving rack-1 node fits the orphan.

    Two rack-1 nodes, each with 1 CPU and one bundle. Kill one; the surviving
    node has no spare CPU for the orphan. The PG should stay in RESCHEDULING.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)

    cluster.add_node(num_cpus=1, labels=rack1_labels)
    node_b = cluster.add_node(num_cpus=1, labels=rack1_labels)

    bundles = [{"CPU": 1}] * 2
    pg = placement_group(
        bundles=bundles,
        topology_strategy=[{NODE_ID_LABEL: "PACK", RACK_LABEL: "STRICT_PACK"}],
    )
    ray.get(pg.ready(), timeout=10)
    assert placement_group_table(pg)["state"] == "CREATED"

    cluster.remove_node(node_b)

    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(pg.ready(), timeout=5)

    state = placement_group_table(pg)["state"]
    assert state == "RESCHEDULING", f"Expected RESCHEDULING, got {state}"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
