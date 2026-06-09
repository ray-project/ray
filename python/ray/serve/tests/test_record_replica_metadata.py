import asyncio
from typing import Any, Dict, Optional

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.common import ReplicaID
from ray.serve.context import _get_internal_replica_context
from ray.serve.handle import DeploymentHandle


@serve.deployment
class Coordinator:
    def __init__(self):
        context = _get_internal_replica_context()
        self.replica_id: ReplicaID = context.replica_id
        # Mutable counter we bump every time the hook is invoked, so the test
        # can assert the hook is captured exactly ONCE (not polled).
        self._metadata_calls = 0

    async def record_replica_metadata(self) -> Dict[str, Any]:
        self._metadata_calls += 1
        return {
            "transfer_address": f"tcp://{self.replica_id.unique_id}:1234",
            "calls": self._metadata_calls,
        }

    def __call__(self, *args) -> ReplicaID:
        return self.replica_id

    def get_metadata_calls(self, *args) -> int:
        return self._metadata_calls


def _running_replicas(handle: DeploymentHandle):
    return handle._router._asyncio_router.request_router._replicas


def check_replica_metadata_recorded(
    handle: DeploymentHandle,
    expected_metadata: Dict[str, Any],
    replica_id: Optional[ReplicaID] = None,
) -> bool:
    running_replicas = _running_replicas(handle)
    if replica_id:
        target_running_replica = running_replicas[replica_id]
    else:
        target_running_replica = next(iter(running_replicas.values()))
    assert (
        target_running_replica.replica_metadata == expected_metadata
    ), f"{target_running_replica.replica_metadata=} != {expected_metadata=}"
    return True


@pytest.mark.parametrize("use_class", [True, False])
def test_no_user_defined_method(serve_instance, use_class):
    """Replicas without the hook expose an empty metadata dict."""
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
    replicas = list(_running_replicas(h).values())
    assert len(replicas) == 1
    assert replicas[0].replica_metadata == {}


@pytest.mark.asyncio
async def test_metadata_captured_once(serve_instance):
    """Metadata is captured once at init and visible on the RunningReplica."""
    h = serve.run(Coordinator.bind())
    replica_id = await h.remote()

    expected_metadata = {
        "transfer_address": f"tcp://{replica_id.unique_id}:1234",
        "calls": 1,
    }
    wait_for_condition(
        check_replica_metadata_recorded,
        handle=h,
        expected_metadata=expected_metadata,
        replica_id=replica_id,
    )

    # The hook must NOT be polled: even after many requests, it was called
    # exactly once (at replica init time).
    await asyncio.gather(*[h.remote() for _ in range(50)])
    assert await h.get_metadata_calls.remote() == 1

    # And the router-side view is unchanged (still calls == 1).
    check_replica_metadata_recorded(
        h, expected_metadata=expected_metadata, replica_id=replica_id
    )


@pytest.mark.asyncio
async def test_metadata_visible_on_replica_selection(serve_instance):
    """The selected replica's metadata is exposed on ReplicaSelection."""
    h = serve.run(Coordinator.bind())
    replica_id = await h.remote()

    expected_metadata = {
        "transfer_address": f"tcp://{replica_id.unique_id}:1234",
        "calls": 1,
    }
    wait_for_condition(
        check_replica_metadata_recorded,
        handle=h,
        expected_metadata=expected_metadata,
        replica_id=replica_id,
    )

    # RunningReplica property mirrors the metadata.
    running_replica = _running_replicas(h)[replica_id]
    assert running_replica.replica_metadata == expected_metadata


@pytest.mark.asyncio
async def test_multiple_replicas(serve_instance):
    """Each replica reports its own static metadata."""
    h = serve.run(Coordinator.options(num_replicas=2).bind())
    replica_ids = set(await asyncio.gather(*[h.remote() for _ in range(100)]))
    assert len(replica_ids) == 2

    for replica_id in replica_ids:
        expected_metadata = {
            "transfer_address": f"tcp://{replica_id.unique_id}:1234",
            "calls": 1,
        }
        wait_for_condition(
            check_replica_metadata_recorded,
            handle=h,
            expected_metadata=expected_metadata,
            replica_id=replica_id,
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
