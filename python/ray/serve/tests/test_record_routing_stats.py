import asyncio
from typing import Any, Dict, Optional

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.common import ReplicaID
from ray.serve.config import RequestRouterConfig
from ray.serve.context import _get_internal_replica_context
from ray.serve.handle import DeploymentHandle


@serve.deployment(
    request_router_config=RequestRouterConfig(
        request_routing_stats_period_s=0.1, request_routing_stats_timeout_s=0.1
    )
)
class Patient:
    def __init__(self):
        self.routing_stats: Dict[str, Any] = {}
        self.should_hang: Optional[asyncio.Event] = None
        self.should_fail: bool = False
        context = _get_internal_replica_context()
        self.replica_id: ReplicaID = context.replica_id

    async def record_routing_stats(self):
        if self.should_hang:
            await self.should_hang.wait()

        if self.should_fail:
            raise Exception("intended to fail")

        return self.routing_stats

    def __call__(self, *args) -> ReplicaID:
        return self.replica_id

    def set_routing_stats(self, routing_stats: Dict[str, Any]) -> ReplicaID:
        self.routing_stats = routing_stats
        return self.replica_id

    def set_should_fail(self):
        self.should_fail = True

    def set_should_hang(self):
        self.should_hang = asyncio.Event()


def check_routing_stats_recorded(
    handle: DeploymentHandle,
    expected_stats: Dict[str, Any],
    replica_id: Optional[ReplicaID] = None,
) -> bool:
    running_replicas = handle._router._asyncio_router.request_router._replicas
    if replica_id:
        target_running_replica = running_replicas[replica_id]
    else:
        target_running_replica = next(iter(running_replicas.values()))
    assert (
        target_running_replica.routing_stats == expected_stats
    ), f"{target_running_replica.routing_stats=} != {expected_stats=}"
    return True


@pytest.mark.parametrize("use_class", [True, False])
def test_no_user_defined_method(serve_instance, use_class):
    """Check the default behavior."""
    if use_class:

        @serve.deployment
        class A:
            def __call__(self, *args):
                return ray.get_runtime_context().current_actor

    else:

        @serve.deployment
        def A(*args):
            return ray.get_runtime_context().current_actor

    h = serve.run(A.bind())
    _ = h.remote().result()
    replicas = list(h._router._asyncio_router.request_router._replicas.values())
    assert len(replicas) == 1
    assert replicas[0].routing_stats == {}


@pytest.mark.asyncio
async def test_user_defined_method_fails(serve_instance):
    """Check the behavior when a user-defined method fails."""
    expected_stats = {"foo": "bar"}
    h = serve.run(Patient.bind())
    await h.set_routing_stats.remote(expected_stats)
    replica_id = await h.remote()

    # Ensure the routing stats are recorded correctly before the failure
    wait_for_condition(
        check_routing_stats_recorded,
        handle=h,
        expected_stats=expected_stats,
        replica_id=replica_id,
    )

    await h.set_should_fail.remote()
    await asyncio.gather(*[h.remote() for _ in range(100)])

    # After the failure, the previous routing stats should still accessible
    wait_for_condition(
        check_routing_stats_recorded,
        handle=h,
        expected_stats=expected_stats,
        replica_id=replica_id,
    )


@pytest.mark.asyncio
async def test_user_defined_method_hangs(serve_instance):
    """Check the behavior when a user-defined method hangs."""
    expected_stats = {"foo": "bar"}
    h = serve.run(Patient.bind())
    await h.set_routing_stats.remote(expected_stats)
    replica_id = await h.remote()

    # Ensure the routing stats are recorded correctly before the failure
    wait_for_condition(
        check_routing_stats_recorded,
        handle=h,
        expected_stats=expected_stats,
        replica_id=replica_id,
    )

    await h.set_should_hang.remote()
    await asyncio.gather(*[h.remote() for _ in range(100)])

    # After the hang, the previous routing stats should still accessible
    wait_for_condition(
        check_routing_stats_recorded,
        handle=h,
        expected_stats=expected_stats,
        replica_id=replica_id,
    )


@pytest.mark.asyncio
async def test_multiple_replicas(serve_instance):
    """Check the behavior with multiple replicas."""
    h = serve.run(Patient.options(num_replicas=2).bind())
    replica_ids = set(await asyncio.gather(*[h.remote() for _ in range(100)]))

    assert len(replica_ids) == 2

    # Ensure that the routing stats is set for one of the replicas.
    expected_stats = {"foo": "bar"}
    updated_stats_replica_id = await h.set_routing_stats.remote(expected_stats)
    wait_for_condition(
        check_routing_stats_recorded,
        handle=h,
        expected_stats=expected_stats,
        replica_id=updated_stats_replica_id,
    )

    # Ensure that the routing stats is not set for the other replica.
    replica_ids.remove(updated_stats_replica_id)
    unupdated_stats_replica_id = replica_ids.pop()
    wait_for_condition(
        check_routing_stats_recorded,
        handle=h,
        expected_stats={},
        replica_id=unupdated_stats_replica_id,
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
