import random
import sys
from typing import Dict, List

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


def get_controller() -> ServeController:
    """Get the current ServeController actor."""
    return ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE)


def get_replica_ranks(deployment_name: str) -> Dict[str, int]:
    """Get the current rank mapping for all replicas in a deployment."""
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


def check_rank_contiguity(ranks: Dict[str, int]) -> bool:
    """Check that ranks form a contiguous sequence from 0 to N-1."""
    if not ranks:
        return True

    rank_values = sorted(ranks.values())
    expected = list(range(len(rank_values)))
    assert rank_values == expected, f"Expected {expected}, got {rank_values}"
    return True


def check_rank_assignment_complete(deployment_name: str, expected_count: int) -> bool:
    """Check that all replicas have been assigned ranks and they are contiguous."""
    try:
        replica_ids = get_running_replica_ids(deployment_name)
        ranks = get_replica_ranks(deployment_name)

        # Check all running replicas have ranks
        for replica_id in replica_ids:
            if replica_id not in ranks:
                print(f"Replica {replica_id} not found in ranks: {ranks}")
                return False

        # Check we have expected number of ranks
        if len(ranks) != expected_count:
            print(f"Expected {expected_count} ranks, got {len(ranks)}: {ranks}")
            return False

        # Check ranks are contiguous
        return check_rank_contiguity(ranks)
    except Exception as e:
        print(f"Error checking rank assignment: {e}")
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
            self.replica_rank = context.rank
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
                "rank": context.rank,
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
                "rank": context.rank,
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
                "rank": context.rank,
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
    assert 0 in ranks.values()

    # Verify API returns correct values
    response = handle.remote().result()
    assert response["rank"] == 0
    assert response["world_size"] == 1


def test_multiple_deployments_independent_ranks(serve_instance):
    """Test that different deployments have independent rank spaces."""

    @serve.deployment(name="deployment1", num_replicas=2)
    class RankTracker1:
        def __call__(self):
            context = serve.get_replica_context()
            return {
                "deployment": "deployment1",
                "rank": context.rank,
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
                "rank": context.rank,
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
    assert 0 in ranks1.values()
    assert 0 in ranks2.values()
    assert 1 in ranks1.values()
    assert 1 in ranks2.values()
    assert 2 in ranks2.values()  # Only deployment2 should have rank 2

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
