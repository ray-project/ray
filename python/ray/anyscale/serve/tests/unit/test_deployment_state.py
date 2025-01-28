import sys

import pytest

from ray.serve._private.common import DeploymentID, DeploymentStatus, ReplicaState
from ray.serve._private.config import ReplicaConfig
from ray.serve._private.deployment_state import DeploymentStateManager
from ray.serve.tests.unit.test_deployment_state import (  # noqa: F401
    check_counts,
    deployment_info,
    mock_deployment_state_manager,
)

TEST_DEPLOYMENT_ID = DeploymentID(name="test_deployment", app_name="test_app")


def dummy():
    pass


def rconfig(**config_opts):
    return ReplicaConfig.create(dummy, **config_opts)


def test_compact_node(mock_deployment_state_manager):  # noqa: F811
    create_dsm, _, cluster_node_info_cache, _ = mock_deployment_state_manager
    cluster_node_info_cache.add_node("node1", {"CPU": 9})
    cluster_node_info_cache.add_node("node2", {"CPU": 4})
    cluster_node_info_cache.add_node("node3", {"CPU": 5})

    dsm: DeploymentStateManager = create_dsm()
    dA = DeploymentID("a", "app")
    dB = DeploymentID("b", "app")
    dC = DeploymentID("c", "app")

    infoA, _ = deployment_info(
        num_replicas=2,
        replica_config=rconfig(ray_actor_options={"num_cpus": 1}),
    )
    infoB, _ = deployment_info(
        num_replicas=1,
        replica_config=rconfig(ray_actor_options={"num_cpus": 2}),
    )
    infoC, _ = deployment_info(
        num_replicas=2,
        replica_config=rconfig(ray_actor_options={"num_cpus": 3}),
    )
    dsm.deploy(dA, infoA)
    dsm.deploy(dB, infoB)
    dsm.deploy(dC, infoC)
    dsA = dsm._deployment_states[dA]
    dsB = dsm._deployment_states[dB]
    dsC = dsm._deployment_states[dC]

    # Node 1: (C3) (C3) -> 6/9 CPUs used
    # Node 2: (A1) (A1) -> 2/4 CPUs used
    # Node 3: (B2) -> 2/5 CPUs used -> should get compacted
    dsm.update()
    dsA._replicas.get()[0]._actor.set_node_id("node2")
    dsA._replicas.get()[0]._actor.set_ready()
    dsA._replicas.get()[1]._actor.set_node_id("node2")
    dsA._replicas.get()[1]._actor.set_ready()

    dsB._replicas.get()[0]._actor.set_node_id("node3")
    dsB._replicas.get()[0]._actor.set_ready()

    dsC._replicas.get()[0]._actor.set_node_id("node1")
    dsC._replicas.get()[0]._actor.set_ready()
    dsC._replicas.get()[1]._actor.set_node_id("node1")
    dsC._replicas.get()[1]._actor.set_ready()

    # Node 3 should be compacted
    dsm.update()
    check_counts(dsA, total=2, by_state=[(ReplicaState.RUNNING, 2, None)])
    check_counts(dsC, total=2, by_state=[(ReplicaState.RUNNING, 2, None)])
    check_counts(
        dsB,
        total=2,
        by_state=[
            (ReplicaState.STARTING, 1, None),
            (ReplicaState.PENDING_MIGRATION, 1, None),
        ],
    )

    dsB._replicas.get([ReplicaState.STARTING])[0]._actor.set_node_id("node2")
    dsB._replicas.get([ReplicaState.STARTING])[0]._actor.set_ready()
    dsm.update()
    check_counts(
        dsB,
        total=2,
        by_state=[
            (ReplicaState.RUNNING, 1, None),
            (ReplicaState.STOPPING, 1, None),
        ],
    )

    dsB._replicas.get([ReplicaState.STOPPING])[0]._actor.set_done_stopping()
    dsm.update()
    check_counts(dsA, total=2, by_state=[(ReplicaState.RUNNING, 2, None)])
    check_counts(dsB, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])
    check_counts(dsC, total=2, by_state=[(ReplicaState.RUNNING, 2, None)])

    for _ in range(3):
        dsm.update()
        check_counts(dsA, total=2, by_state=[(ReplicaState.RUNNING, 2, None)])
        check_counts(dsB, total=1, by_state=[(ReplicaState.RUNNING, 1, None)])
        check_counts(dsC, total=2, by_state=[(ReplicaState.RUNNING, 2, None)])

    assert dsA.curr_status_info.status == DeploymentStatus.HEALTHY
    assert dsB.curr_status_info.status == DeploymentStatus.HEALTHY
    assert dsC.curr_status_info.status == DeploymentStatus.HEALTHY


def test_compaction_cancelled(mock_deployment_state_manager):  # noqa: F811
    create_dsm, _, cluster_node_info_cache, _ = mock_deployment_state_manager
    cluster_node_info_cache.add_node("node1", {"CPU": 3})
    cluster_node_info_cache.add_node("node2", {"CPU": 3})

    dsm: DeploymentStateManager = create_dsm()

    info1, _ = deployment_info(num_replicas=3, version="1")
    dsm.deploy(TEST_DEPLOYMENT_ID, info1)
    ds = dsm._deployment_states[TEST_DEPLOYMENT_ID]

    # Node 1: (1) (1) -> 2/3 CPUs
    # Node 2: (1) -> 1/3 CPUs
    dsm.update()
    ds._replicas.get()[0]._actor.set_node_id("node1")
    ds._replicas.get()[0]._actor.set_ready()
    ds._replicas.get()[1]._actor.set_node_id("node1")
    ds._replicas.get()[1]._actor.set_ready()
    ds._replicas.get()[2]._actor.set_node_id("node2")
    ds._replicas.get()[2]._actor.set_ready()

    # Node 2 should be compacted
    dsm.update()
    check_counts(
        ds,
        total=4,
        by_state=[
            (ReplicaState.RUNNING, 2, None),
            (ReplicaState.PENDING_MIGRATION, 1, None),
            (ReplicaState.STARTING, 1, None),
        ],
    )

    # Upscale from 3 to 4 replicas
    info2, _ = deployment_info(num_replicas=4, version="1")
    dsm.deploy(TEST_DEPLOYMENT_ID, info2)
    dsm.update()
    check_counts(
        ds,
        total=5,
        by_state=[
            (ReplicaState.RUNNING, 2, None),
            (ReplicaState.PENDING_MIGRATION, 1, None),
            (ReplicaState.STARTING, 2, None),
        ],
    )

    # Since there is no more space on node 1, the 4th replica must be
    # scheduled on node 2. This should cancel the ongoing compaction.
    ds._replicas.get([ReplicaState.STARTING])[0]._actor.set_node_id("node2")
    ds._replicas.get([ReplicaState.STARTING])[0]._actor.set_ready()
    dsm.update()
    check_counts(
        ds,
        total=5,
        by_state=[
            # 2 original running replicas
            # + 1 new replica that got started
            # + the pending migration replica is reverted to running
            # -> 4 RUNNING
            (ReplicaState.RUNNING, 4, None),
            # The other starting replica should get stopped to match
            # target_num_replicas=4 -> 1 STOPPING
            (ReplicaState.STOPPING, 1, None),
        ],
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
