import pytest

from ray.anyscale.serve.centralized_scheduler import (
    SchedulerState,
    SyncCentralScheduler,
)


def test_state():
    t0 = 1000
    state = SchedulerState()
    assert state.debug_dict() == {}
    router_id = "router_1"

    # Empty replica.
    state.update_replicas(["r1"])
    assert state.debug_dict() == {"r1": {}}
    assert state.find_schedulable_replica("m1", 1) is None
    assert state.find_evictable_replica("m1", 1, 1, t0) == ("r1", None)
    assert state.count_replicas_for_model("m1") == 0

    # Assign request for m1.
    t1 = 1001
    state.assign_request("req_1", "m1", "r1", router_id, t1)
    assert state.debug_dict() == {"r1": {"m1": (t1, 1)}}
    assert state.find_schedulable_replica("m1", 1) is None
    assert state.find_schedulable_replica("m1", 2) == "r1"
    assert state.find_evictable_replica("m1", 1, 0, t1) == (None, None)
    assert state.count_replicas_for_model("m1") == 1

    # Assign request 2 for m1.
    t2 = 1002
    state.assign_request("req_2", "m1", "r1", router_id, t2)
    assert state.debug_dict() == {"r1": {"m1": (t1, 2)}}

    # Finish request.
    state.complete_request("req_1")
    assert state.debug_dict() == {"r1": {"m1": (t1, 1)}}

    # Change replicas.
    state.update_replicas(["r2", "r3"])
    assert state.debug_dict() == {"r2": {}, "r3": {}}

    # Test eviction policy.
    assert state.find_schedulable_replica("m1", 1) is None
    t3 = 2000
    assert state.find_evictable_replica("m2", 1, 10, t3) == ("r2", None)
    state.assign_request("req_3", "m2", "r2", router_id, t3)
    assert state.find_evictable_replica("m3", 1, 10, t3) == ("r3", None)
    state.assign_request("req_4", "m3", "r3", router_id, t3 + 5)
    assert state.debug_dict() == {"r2": {"m2": (t3, 1)}, "r3": {"m3": (t3 + 5, 1)}}
    assert state.find_evictable_replica("m4", 1, 10, t3) == (None, None)
    assert state.find_evictable_replica("m4", 1, 10, t3 + 9) == (None, None)
    assert state.find_evictable_replica("m4", 1, 10, t3 + 11) == ("r2", "m2")
    state.assign_request("req_5", "m4", "r2", router_id, t3 + 11, model_to_evict="m2")
    assert state.debug_dict() == {"r2": {"m4": (t3 + 11, 1)}, "r3": {"m3": (t3 + 5, 1)}}


def test_sync_scheduler_enforces_limits():
    now = 1000
    router_id = "router_1"

    def clock():
        return now

    sched = SyncCentralScheduler(
        max_replicas_per_model=1,
        max_models_per_replica=1,
        max_concurrent_requests_per_replica=2,
        model_keepalive_s=10,
        clock=clock,
    )
    assert not sched.try_schedule("req1", "m1", router_id)
    sched.update_running_replicas(["r1"])
    assert sched.try_schedule("req1", "m1", router_id) == "r1"
    assert sched.try_schedule("req2", "m1", router_id) == "r1"
    assert not sched.try_schedule("req3", "m1", router_id)
    assert not sched.try_schedule("req3", "m2", router_id)
    sched.notify_completed("req1")
    assert sched.try_schedule("req3", "m1", router_id) == "r1"
    sched.update_running_replicas(["r1", "r2"])
    assert not sched.try_schedule("req4", "m1", router_id)
    assert not sched.try_schedule("req4", "m1", router_id)
    assert sched.try_schedule("req5", "m2", router_id) == "r2"
    assert sched.try_schedule("req6", "m2", router_id) == "r2"
    assert not sched.try_schedule("req7", "m2", router_id)
    sched.notify_completed("req5")
    assert sched.try_schedule("req7", "m2", router_id) == "r2"


def test_sync_scheduler_eviction_policy():
    now = 1000
    router_id = "router_1"

    def clock():
        return now

    sched = SyncCentralScheduler(
        max_replicas_per_model=1,
        max_models_per_replica=1,
        max_concurrent_requests_per_replica=2,
        model_keepalive_s=10,
        clock=clock,
    )
    assert not sched.try_schedule("req1", "m1", router_id)
    sched.update_running_replicas(["r1"])
    assert sched.try_schedule("req1", "m1", router_id) == "r1"
    assert sched.try_schedule("req2", "m1", router_id) == "r1"
    assert not sched.try_schedule("req3", "m1", router_id)
    assert not sched.try_schedule("req4", "m2", router_id)

    now += 15

    assert not sched.try_schedule("req5", "m1", router_id)  # Cannot evict self
    assert sched.try_schedule("req6", "m2", router_id) == "r1"  # But can evict others.
    # Can complete evicted requests.
    sched.notify_completed("req1")
    sched.notify_completed("req2")


def test_clean_up_state_by_router():
    """Test pending requests cleaned up when router is deleted."""

    now = 1000
    router_id = "router_1"

    def clock():
        return now

    sched = SyncCentralScheduler(
        max_replicas_per_model=1,
        max_models_per_replica=1,
        max_concurrent_requests_per_replica=2,
        model_keepalive_s=10,
        clock=clock,
    )
    sched.update_running_replicas(["r1"])
    assert sched.try_schedule("req1", "m1", router_id) == "r1"
    assert sched.state.debug_dict() == {"r1": {"m1": (now, 1)}}
    assert sched.state.router_requests == {"router_1": ["req1"]}
    sched.clean_up_scheduler_state([router_id])
    assert sched.state.debug_dict() == {"r1": {"m1": (now, 0)}}
    assert sched.state.router_requests == {}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
