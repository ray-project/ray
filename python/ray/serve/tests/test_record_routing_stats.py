import asyncio
from typing import Any, Dict, Optional

import pytest

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve._private.common import ReplicaID
from ray.serve.context import _get_internal_replica_context
from ray.serve.handle import DeploymentHandle


@serve.deployment(
    request_routing_stats_period_s=0.1, request_routing_stats_timeout_s=0.1
)
class Patient:
    def __init__(self):
        self.routing_stats = {}
        self.should_hang = False
        self.should_fail = False
        context = _get_internal_replica_context()
        self.replica_id: ReplicaID = context.replica_id

    async def record_routing_stats(self):
        if self.should_hang:
            import time

            time.sleep(10000)

        if self.should_fail:
            raise Exception("intended to fail")

        return self.routing_stats

    def __call__(self, *args) -> ReplicaID:
        return self.replica_id

    def set_routing_stats(self, routing_stats: Dict[str, Any]):
        print(f"set_routing_stats {routing_stats=}")
        self.routing_stats = routing_stats

    def set_should_fail(self):
        self.should_fail = True

    def set_should_hang(self):
        self.should_hang = True


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

    # After the failure the previous routing stats should still accessible
    wait_for_condition(
        check_routing_stats_recorded,
        handle=h,
        expected_stats=expected_stats,
        replica_id=replica_id,
    )


# @pytest.mark.asyncio
# async def test_user_defined_method_hangs(serve_instance):
#     """Check the behavior when a user-defined method hangs."""
#     expected_stats = {"foo": "bar"}
#     h = serve.run(Patient.bind())
#     await h.set_routing_stats.remote(expected_stats)
#     replica_id = await h.remote()
#
#     # Ensure the routing stats are recorded correctly before the failure
#     wait_for_condition(check_routing_stats_recorded, handle=h, expected_stats=expected_stats, replica_id=replica_id)
#
#     print("A")
#     await h.set_should_hang.remote()
#     print("B")
#     await asyncio.gather(*[h.remote() for _ in range(100)])
#     print("C")
#     # After the failure the previous routing stats should still accessible
#     wait_for_condition(check_routing_stats_recorded, handle=h, expected_stats=expected_stats, replica_id=replica_id)
#
#
# @pytest.mark.asyncio
# async def test_multiple_replicas(serve_instance):
#     h = serve.run(Patient.options(num_replicas=2).bind())
#     actors = {
#         a._actor_id for a in await asyncio.gather(*[h.remote() for _ in range(100)])
#     }
#     assert len(actors) == 2
#
#     await h.set_should_fail.remote()
#
#     await async_wait_for_condition(
#         check_new_actor_started, handle=h, original_actors=actors
#     )
#
#     new_actors = {
#         a._actor_id for a in await asyncio.gather(*[h.remote() for _ in range(100)])
#     }
#     assert len(new_actors) == 2
#     assert len(new_actors.intersection(actors)) == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
