import random
import sys
from typing import Any, Dict, List

import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.common import (
    DeploymentID,
    DeploymentStatus,
    ReplicaState,
)
from ray.serve._private.constants import (
    SERVE_CONTROLLER_NAME,
    SERVE_DEFAULT_APP_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.controller import ServeController
from ray.serve._private.test_utils import (
    check_deployment_status,
    check_num_replicas_eq,
)
from ray.serve.schema import ReplicaRank


def get_controller() -> ServeController:
    """Get the current ServeController actor."""
    return ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE)


def get_replica_ranks(deployment_name: str) -> Dict[str, ReplicaRank]:
    """Get the current rank mapping for all replicas in a deployment.

    Args:
        deployment_name: Name of the deployment to get ranks for

    Returns:
        Dict mapping replica_id to ReplicaRank object
    """
    controller = get_controller()
    deployment_id = DeploymentID(name=deployment_name, app_name=SERVE_DEFAULT_APP_NAME)

    # Use the public API method on the controller
    return ray.get(controller._get_replica_ranks_mapping.remote(deployment_id))


def get_running_replica_ids(deployment_name: str) -> List[str]:
    """Get the replica IDs of running replicas for given deployment."""
    controller = get_controller()
    deployment_id = DeploymentID(name=deployment_name, app_name=SERVE_DEFAULT_APP_NAME)

    replicas = ray.get(
        controller._dump_replica_states_for_testing.remote(deployment_id)
    )
    running_replicas = replicas.get([ReplicaState.RUNNING])
    return [replica.replica_id.unique_id for replica in running_replicas]


def check_rank_contiguity(ranks: Dict[str, ReplicaRank]) -> bool:
    """Check that all rank types form contiguous sequences from 0 to N-1.

    Args:
        ranks: Dict mapping replica_id to ReplicaRank object

    Returns:
        True if all rank types (global, node, local) are contiguous
    """
    if not ranks:
        return True

    # Check global ranks are contiguous
    global_ranks = sorted([r.rank for r in ranks.values()])
    expected_global = list(range(len(global_ranks)))
    if global_ranks != expected_global:
        print(
            f"Global ranks not contiguous. Expected {expected_global}, got {global_ranks}"
        )
        return False

    # Group by node_rank and check local ranks are contiguous per node
    replicas_by_node = {}
    node_ranks_set = set()
    for replica_id, rank_obj in ranks.items():
        node_rank = rank_obj.node_rank
        node_ranks_set.add(node_rank)
        if node_rank not in replicas_by_node:
            replicas_by_node[node_rank] = []
        replicas_by_node[node_rank].append(rank_obj.local_rank)

    # Check node ranks are contiguous
    node_ranks_sorted = sorted(node_ranks_set)
    expected_node_ranks = list(range(len(node_ranks_sorted)))
    if node_ranks_sorted != expected_node_ranks:
        print(
            f"Node ranks not contiguous. Expected {expected_node_ranks}, got {node_ranks_sorted}"
        )
        return False

    # Check local ranks are contiguous per node
    for node_rank, local_ranks in replicas_by_node.items():
        local_ranks_sorted = sorted(local_ranks)
        expected_local = list(range(len(local_ranks_sorted)))
        if local_ranks_sorted != expected_local:
            print(
                f"Local ranks not contiguous on node {node_rank}. Expected {expected_local}, got {local_ranks_sorted}"
            )
            return False

    return True


def check_rank_assignment_complete(deployment_name: str, expected_count: int) -> bool:
    """Check that all replicas have been assigned ranks and they are contiguous.

    This validates global ranks, node ranks, and local ranks for all running replicas.
    """
    try:
        replica_ids = get_running_replica_ids(deployment_name)
        ranks = get_replica_ranks(deployment_name)

        # Check all running replicas have ranks
        for replica_id in replica_ids:
            if replica_id not in ranks:
                print(f"Replica {replica_id} not found in ranks: {ranks.keys()}")
                return False

        # Check we have expected number of ranks
        if len(ranks) != expected_count:
            print(
                f"Expected {expected_count} ranks, got {len(ranks)}: {list(ranks.keys())}"
            )
            return False

        # Check all rank types are contiguous (global, node, local)
        return check_rank_contiguity(ranks)
    except Exception as e:
        print(f"Error checking rank assignment: {e}")
        import traceback

        traceback.print_exc()
        return False


@pytest.mark.parametrize("num_replicas", [1, 3, 5])
def test_basic_rank_assignment(serve_instance, num_replicas):
    """Test basic rank assignment for different numbers of replicas."""

    @serve.deployment(num_replicas=num_replicas)
    class RankTracker:
        def __init__(self):
            self.replica_rank = None
            self.world_size = None

        def __call__(self):
            context = serve.get_replica_context()
            self.replica_rank = context.rank.rank if context.rank else None
            self.world_size = context.world_size
            return {
                "rank": self.replica_rank,
                "world_size": self.world_size,
            }

    handle = serve.run(RankTracker.bind())

    # Wait for all replicas to be running and have ranks assigned
    wait_for_condition(
        lambda: check_rank_assignment_complete("RankTracker", num_replicas),
    )

    # Verify ranks are correctly assigned
    ranks = get_replica_ranks("RankTracker")
    assert len(ranks) == num_replicas
    assert check_rank_contiguity(ranks)

    # Verify replicas can access their ranks via API
    responses = []
    for _ in range(10):  # Make multiple requests to hit different replicas
        response = handle.remote().result()
        responses.append(response)

    # Check that we got responses from all replicas
    seen_ranks = set()
    for response in responses:
        assert response["world_size"] == num_replicas
        if response["rank"] is not None:
            seen_ranks.add(response["rank"])

    # We should eventually see all ranks (though it might take multiple requests)
    assert len(seen_ranks) <= num_replicas
    for rank in seen_ranks:
        assert 0 <= rank < num_replicas


def test_node_and_local_rank_assignment(serve_instance):
    """Test node_rank and local_rank assignment in addition to global rank."""

    @serve.deployment(num_replicas=4)
    class NodeRankTracker:
        def __call__(self):
            context = serve.get_replica_context()
            if context.rank:
                return {
                    "rank": context.rank.rank,
                    "node_rank": context.rank.node_rank,
                    "local_rank": context.rank.local_rank,
                    "world_size": context.world_size,
                }
            return None

    handle = serve.run(NodeRankTracker.bind())

    # Wait for all replicas to be running
    wait_for_condition(
        lambda: check_rank_assignment_complete("NodeRankTracker", 4),
    )

    # Collect responses from all replicas
    responses = []
    max_attempts = 50
    for _ in range(max_attempts):
        response = handle.remote().result()
        if response and response not in responses:
            responses.append(response)
        if len(responses) == 4:
            break

    assert len(responses) == 4, f"Expected 4 unique responses, got {len(responses)}"

    # Verify all responses have valid ranks
    global_ranks = set()
    node_ranks = set()
    replicas_by_node = {}

    for response in responses:
        assert response["world_size"] == 4

        # Check global rank
        global_rank = response["rank"]
        assert 0 <= global_rank < 4
        assert global_rank not in global_ranks, "Duplicate global rank found"
        global_ranks.add(global_rank)

        # Check node_rank and local_rank
        node_rank = response["node_rank"]
        local_rank = response["local_rank"]
        assert node_rank >= 0
        assert local_rank >= 0
        node_ranks.add(node_rank)

        # Track replicas by node for local rank verification
        if node_rank not in replicas_by_node:
            replicas_by_node[node_rank] = []
        replicas_by_node[node_rank].append(local_rank)

    # Verify global ranks are contiguous 0..3
    assert global_ranks == {0, 1, 2, 3}

    # Verify node ranks are contiguous starting from 0
    assert min(node_ranks) == 0
    assert max(node_ranks) == len(node_ranks) - 1

    # Verify local ranks within each node are contiguous starting from 0
    for node_rank, local_ranks_list in replicas_by_node.items():
        local_ranks_set = set(local_ranks_list)
        expected_local_ranks = set(range(len(local_ranks_list)))
        assert local_ranks_set == expected_local_ranks, (
            f"Node {node_rank} has non-contiguous local ranks: {local_ranks_set}, "
            f"expected {expected_local_ranks}"
        )


def test_local_rank_contiguity_within_node(serve_instance):
    """Test that local ranks are contiguous within each node."""

    @serve.deployment(num_replicas=3)
    class LocalRankTracker:
        def __call__(self):
            context = serve.get_replica_context()
            if context.rank:
                return {
                    "rank": context.rank.rank,
                    "node_rank": context.rank.node_rank,
                    "local_rank": context.rank.local_rank,
                }
            return None

    handle = serve.run(LocalRankTracker.bind())

    # Wait for all replicas to be running
    wait_for_condition(
        lambda: check_rank_assignment_complete("LocalRankTracker", 3),
    )

    # Collect all responses
    responses = []
    for _ in range(30):
        response = handle.remote().result()
        if response and response not in responses:
            responses.append(response)
        if len(responses) == 3:
            break

    assert len(responses) == 3

    # Group by node_rank and check local_rank contiguity
    by_node = {}
    for r in responses:
        node_rank = r["node_rank"]
        if node_rank not in by_node:
            by_node[node_rank] = []
        by_node[node_rank].append(r["local_rank"])

    # Within each node, local ranks should start at 0 and be contiguous
    for node_rank, local_ranks in by_node.items():
        local_ranks_sorted = sorted(local_ranks)
        expected = list(range(len(local_ranks)))
        assert local_ranks_sorted == expected, (
            f"Node {node_rank} has non-contiguous local ranks: "
            f"{local_ranks_sorted}, expected {expected}"
        )


def test_rank_assignment_with_autoscaling(serve_instance):
    """Test rank assignment and reassignment during autoscaling."""
    signal_actor = SignalActor.remote()

    @serve.deployment(
        autoscaling_config={
            "target_ongoing_requests": 1,
            "metrics_interval_s": 0.1,
            "min_replicas": 2,
            "max_replicas": 4,
            "upscale_delay_s": 1,
            "downscale_delay_s": 1,
            "look_back_period_s": 10,
        },
        max_ongoing_requests=10,
    )
    class AutoscalingRankTracker:
        async def __call__(self):
            await signal_actor.wait.remote()
            context = serve.get_replica_context()
            return {
                "rank": context.rank.rank if context.rank else None,
                "node_rank": context.rank.node_rank if context.rank else None,
                "local_rank": context.rank.local_rank if context.rank else None,
                "world_size": context.world_size,
            }

    handle = serve.run(AutoscalingRankTracker.bind())

    # Wait for initial replicas
    wait_for_condition(
        lambda: check_rank_assignment_complete("AutoscalingRankTracker", 2),
    )

    initial_ranks = get_replica_ranks("AutoscalingRankTracker")
    assert len(initial_ranks) == 2
    assert check_rank_contiguity(initial_ranks)

    # Send concurrent requests to trigger autoscaling
    _ = [handle.remote() for _ in range(10)]

    # Wait for scale-up to happen and ranks to be reassigned
    wait_for_condition(
        lambda: check_num_replicas_eq("AutoscalingRankTracker", 4, use_controller=True),
        timeout=20,
    )

    # Check that ranks are still contiguous after scale-up
    wait_for_condition(
        lambda: check_rank_assignment_complete("AutoscalingRankTracker", 4),
    )

    scaled_ranks = get_replica_ranks("AutoscalingRankTracker")
    assert len(scaled_ranks) == 4
    assert check_rank_contiguity(scaled_ranks)

    signal_actor.send.remote()

    # Wait for scale-down (no more load)
    wait_for_condition(
        lambda: check_num_replicas_eq("AutoscalingRankTracker", 2, use_controller=True),
    )

    # Check that ranks are reassigned and contiguous after scale-down
    wait_for_condition(
        lambda: check_rank_assignment_complete("AutoscalingRankTracker", 2),
    )

    final_ranks = get_replica_ranks("AutoscalingRankTracker")
    assert len(final_ranks) == 2
    assert check_rank_contiguity(final_ranks)


def test_rank_persistence_across_controller_restart(serve_instance):
    """Test that ranks are preserved across controller failures."""

    @serve.deployment(num_replicas=3)
    class PersistentRankTracker:
        def __call__(self):
            context = serve.get_replica_context()
            return {
                "rank": context.rank.rank if context.rank else None,
                "world_size": context.world_size,
            }

    serve.run(PersistentRankTracker.bind())

    # Wait for all replicas to be running
    wait_for_condition(
        lambda: check_rank_assignment_complete("PersistentRankTracker", 3),
    )

    # Record initial ranks
    initial_ranks = get_replica_ranks("PersistentRankTracker")

    assert len(initial_ranks) == 3
    assert check_rank_contiguity(initial_ranks)

    # Kill the controller to simulate failure
    controller = get_controller()
    ray.kill(controller, no_restart=False)

    # Wait for controller to be restarted and deployment to be recovered
    wait_for_condition(
        lambda: check_deployment_status(
            "PersistentRankTracker", DeploymentStatus.HEALTHY
        ),
    )

    # Wait for rank assignment to be restored
    wait_for_condition(
        lambda: check_rank_assignment_complete("PersistentRankTracker", 3),
    )

    # Check that ranks are preserved for surviving replicas
    recovered_ranks = get_replica_ranks("PersistentRankTracker")

    assert len(recovered_ranks) == 3
    assert check_rank_contiguity(recovered_ranks)

    # Check that the recovered ranks are the same as the initial ranks
    assert recovered_ranks == initial_ranks


def test_single_replica_deployment(serve_instance):
    """Test rank assignment for single replica deployment."""

    @serve.deployment(num_replicas=1)
    class SingleReplicaTracker:
        def __call__(self):
            context = serve.get_replica_context()
            return {
                "rank": context.rank.rank if context.rank else None,
                "node_rank": context.rank.node_rank if context.rank else None,
                "local_rank": context.rank.local_rank if context.rank else None,
                "world_size": context.world_size,
            }

    handle = serve.run(SingleReplicaTracker.bind())

    # Wait for deployment
    wait_for_condition(
        lambda: check_rank_assignment_complete("SingleReplicaTracker", 1),
    )

    # Verify single replica has rank 0
    ranks = get_replica_ranks("SingleReplicaTracker")
    assert len(ranks) == 1
    rank_obj = list(ranks.values())[0]
    assert rank_obj.rank == 0
    assert rank_obj.node_rank == 0
    assert rank_obj.local_rank == 0

    # Verify API returns correct values for all rank types
    response = handle.remote().result()
    assert response["rank"] == 0
    assert response["node_rank"] == 0
    assert response["local_rank"] == 0
    assert response["world_size"] == 1


def test_multiple_deployments_independent_ranks(serve_instance):
    """Test that different deployments have independent rank spaces."""

    @serve.deployment(name="deployment1", num_replicas=2)
    class RankTracker1:
        def __call__(self):
            context = serve.get_replica_context()
            return {
                "deployment": "deployment1",
                "rank": context.rank.rank if context.rank else None,
                "world_size": context.world_size,
            }

    @serve.deployment(name="deployment2", num_replicas=3)
    class RankTracker2:
        def __init__(self, rank_tracker1):
            self.rank_tracker1 = rank_tracker1

        def __call__(self):
            context = serve.get_replica_context()
            return {
                "deployment": "deployment2",
                "rank": context.rank.rank if context.rank else None,
                "world_size": context.world_size,
            }

    serve.run(RankTracker2.bind(RankTracker1.bind()))
    # Wait for both deployments
    wait_for_condition(
        lambda: check_rank_assignment_complete("deployment1", 2),
    )
    wait_for_condition(
        lambda: check_rank_assignment_complete("deployment2", 3),
    )

    # Check ranks are independent
    ranks1 = get_replica_ranks("deployment1")
    ranks2 = get_replica_ranks("deployment2")

    assert len(ranks1) == 2
    assert len(ranks2) == 3
    assert check_rank_contiguity(ranks1)
    assert check_rank_contiguity(ranks2)

    # Both should have rank 0 (in their own space)
    ranks1_global = {r.rank for r in ranks1.values()}
    ranks2_global = {r.rank for r in ranks2.values()}
    assert 0 in ranks1_global
    assert 0 in ranks2_global
    assert 1 in ranks1_global
    assert 1 in ranks2_global
    assert 2 in ranks2_global  # Only deployment2 should have rank 2

    handle1 = serve.get_deployment_handle("deployment1", SERVE_DEFAULT_APP_NAME)
    handle2 = serve.get_deployment_handle("deployment2", SERVE_DEFAULT_APP_NAME)

    response1 = handle1.remote().result()
    response2 = handle2.remote().result()
    assert response1["world_size"] == 2
    assert response2["world_size"] == 3


def test_rank_stability_on_replica_death(serve_instance):
    """Test that when one replica dies, other replicas keep their ranks."""

    @serve.deployment(num_replicas=4)
    class StableRankTracker:
        def __call__(self):
            return "hello"

    serve.run(StableRankTracker.bind())

    # Wait for all replicas to be running and have ranks
    wait_for_condition(
        lambda: check_rank_assignment_complete("StableRankTracker", 4),
    )

    # get_replica_ranks
    initial_ranks = get_replica_ranks("StableRankTracker")
    initial_replica_ids = get_running_replica_ids("StableRankTracker")
    assert len(initial_ranks) == 4
    assert check_rank_contiguity(initial_ranks)

    # kill the replica with rank 1
    random_replica_id_idx = random.choice(range(len(initial_replica_ids)))
    killed_replica_id = initial_replica_ids[random_replica_id_idx]
    replica_handle = ray.get_actor(
        f"SERVE_REPLICA::default#StableRankTracker#{killed_replica_id}",
        namespace=SERVE_NAMESPACE,
    )
    ray.kill(replica_handle, no_restart=False)

    def _check():
        new_running_replica_ids = get_running_replica_ids("StableRankTracker")
        assert len(new_running_replica_ids) == 4
        assert new_running_replica_ids != initial_replica_ids
        return True

    wait_for_condition(_check, timeout=20)

    # get_replica_ranks
    final_ranks = get_replica_ranks("StableRankTracker")
    assert len(final_ranks) == 4
    assert check_rank_contiguity(final_ranks)
    # for all replicas that is not killed, their ranks should be the same as before
    for replica_id in initial_replica_ids:
        if replica_id != killed_replica_id:
            assert final_ranks[replica_id] == initial_ranks[replica_id]


def test_node_rank_stability_on_replica_death(serve_instance):
    """Test that node_rank and local_rank are correctly maintained when replicas die."""

    @serve.deployment(num_replicas=4)
    class NodeRankStabilityTracker:
        def __call__(self):
            context = serve.get_replica_context()
            if context.rank:
                return {
                    "rank": context.rank.rank,
                    "node_rank": context.rank.node_rank,
                    "local_rank": context.rank.local_rank,
                    "replica_id": context.replica_id.unique_id,
                }
            return None

    handle = serve.run(NodeRankStabilityTracker.bind())

    # Wait for all replicas to be running
    wait_for_condition(
        lambda: check_rank_assignment_complete("NodeRankStabilityTracker", 4),
    )

    # Collect initial rank information
    initial_responses = []
    for _ in range(50):
        response = handle.remote().result()
        if response and response not in initial_responses:
            initial_responses.append(response)
        if len(initial_responses) == 4:
            break

    assert len(initial_responses) == 4

    # Kill a random replica
    random_replica = random.choice(initial_responses)
    killed_replica_id = random_replica["replica_id"]

    replica_handle = ray.get_actor(
        f"SERVE_REPLICA::default#NodeRankStabilityTracker#{killed_replica_id}",
        namespace=SERVE_NAMESPACE,
    )
    ray.kill(replica_handle, no_restart=False)

    # Wait for the replica to be restarted
    def _check_replica_restarted():
        replica_ids = get_running_replica_ids("NodeRankStabilityTracker")
        return len(replica_ids) == 4 and killed_replica_id not in replica_ids

    wait_for_condition(_check_replica_restarted, timeout=20)

    # Wait for rank assignment to be complete
    wait_for_condition(
        lambda: check_rank_assignment_complete("NodeRankStabilityTracker", 4),
    )

    # Collect final rank information
    final_responses = []
    for _ in range(50):
        response = handle.remote().result()
        if response and response not in final_responses:
            final_responses.append(response)
        if len(final_responses) == 4:
            break

    assert len(final_responses) == 4

    # Create mappings for comparison
    initial_by_replica_id = {r["replica_id"]: r for r in initial_responses}
    final_by_replica_id = {r["replica_id"]: r for r in final_responses}

    # Verify that surviving replicas kept their ranks
    for replica_id in initial_by_replica_id:
        if replica_id != killed_replica_id and replica_id in final_by_replica_id:
            initial = initial_by_replica_id[replica_id]
            final = final_by_replica_id[replica_id]

            # All rank values should be preserved
            assert (
                initial["rank"] == final["rank"]
            ), f"Global rank changed for replica {replica_id}"
            assert (
                initial["node_rank"] == final["node_rank"]
            ), f"Node rank changed for replica {replica_id}"
            assert (
                initial["local_rank"] == final["local_rank"]
            ), f"Local rank changed for replica {replica_id}"

    # Verify all global ranks are still contiguous
    global_ranks = sorted([r["rank"] for r in final_responses])
    assert global_ranks == [0, 1, 2, 3]


def test_user_reconfigure_rank(serve_instance):
    """Test that user can reconfigure the rank of a deployment."""
    signal_actor = SignalActor.remote()

    @serve.deployment(
        num_replicas=4, user_config={"name": "Bob"}, max_ongoing_requests=1
    )
    class ReconfigureRankTracker:
        def __init__(self):
            self.my_rank = "Bob"

        async def __call__(self):
            await signal_actor.wait.remote()
            return self.my_rank

        async def reconfigure(self, user_config: Any, rank: ReplicaRank):
            # rank parameter is actually a ReplicaRank object, extract the integer value
            self.my_rank = rank.rank

    handle = serve.run(ReconfigureRankTracker.bind())
    wait_for_condition(
        lambda: check_rank_assignment_complete("ReconfigureRankTracker", 4),
    )

    f = [handle.remote() for _ in range(4)]

    wait_for_condition(
        lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 4,
    )

    signal_actor.send.remote(clear=True)

    def _check():
        assert {f.result() for f in f} == {0, 1, 2, 3}
        return True

    wait_for_condition(_check)

    serve.run(ReconfigureRankTracker.options(user_config={"name": "Alice"}).bind())
    wait_for_condition(
        lambda: check_rank_assignment_complete("ReconfigureRankTracker", 4),
    )

    f = [handle.remote() for _ in range(4)]
    wait_for_condition(
        lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 4,
    )
    signal_actor.send.remote()

    def _check():
        assert {f.result() for f in f} == {0, 1, 2, 3}
        return True

    wait_for_condition(_check)


def test_user_reconfigure_with_all_rank_fields(serve_instance):
    """Test that reconfigure receives all rank fields (rank, node_rank, local_rank)."""
    signal_actor = SignalActor.remote()

    @serve.deployment(num_replicas=3, max_ongoing_requests=1)
    class AllRanksTracker:
        def __init__(self):
            self.rank_info = None

        async def __call__(self):
            await signal_actor.wait.remote()
            return self.rank_info

        async def reconfigure(self, user_config: Any, rank: ReplicaRank):
            # Store all rank information
            self.rank_info = {
                "rank": rank.rank,
                "node_rank": rank.node_rank,
                "local_rank": rank.local_rank,
            }

    handle = serve.run(AllRanksTracker.bind())
    wait_for_condition(
        lambda: check_rank_assignment_complete("AllRanksTracker", 3),
    )

    # Send requests to all replicas
    futures = [handle.remote() for _ in range(3)]

    wait_for_condition(
        lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 3,
    )

    signal_actor.send.remote()

    # Collect results
    results = [f.result() for f in futures]

    # Verify all replicas received their rank information
    global_ranks = []
    node_ranks = []
    local_ranks = []

    for result in results:
        assert result is not None, "Replica did not receive rank information"
        assert "rank" in result
        assert "node_rank" in result
        assert "local_rank" in result

        # Validate rank values are in expected range
        assert result["rank"] in {0, 1, 2}, f"Invalid global rank: {result['rank']}"

        global_ranks.append(result["rank"])
        node_ranks.append(result["node_rank"])
        local_ranks.append(result["local_rank"])

    # Verify global ranks are unique and complete
    assert set(global_ranks) == {0, 1, 2}

    # Verify node ranks form contiguous sequence starting from 0
    node_ranks_sorted = sorted(set(node_ranks))
    expected_node_ranks = list(range(len(node_ranks_sorted)))
    assert (
        node_ranks_sorted == expected_node_ranks
    ), f"Node ranks not contiguous from 0: {node_ranks_sorted}"

    # Verify local ranks are valid (non-negative and reasonable)
    for local_rank in local_ranks:
        assert local_rank in range(3), f"Invalid local rank: {local_rank}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
