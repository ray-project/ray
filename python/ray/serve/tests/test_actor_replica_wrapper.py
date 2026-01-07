import asyncio
import pickle
import sys
from typing import Union

import pytest

import ray
from ray import ObjectRef, ObjectRefGenerator
from ray._common.test_utils import SignalActor
from ray._common.utils import get_or_create_event_loop
from ray.exceptions import TaskCancelledError
from ray.serve._private.common import (
    DeploymentID,
    ReplicaID,
    ReplicaQueueLengthInfo,
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.serve._private.test_utils import send_signal_on_cancellation


@ray.remote(num_cpus=0)
class FakeReplicaActor:
    def __init__(self):
        self._replica_queue_length_info = None

    def set_replica_queue_length_info(self, info: ReplicaQueueLengthInfo):
        self._replica_queue_length_info = info

    async def handle_request(
        self,
        request_metadata: Union[bytes, RequestMetadata],
        message: str,
        *,
        is_streaming: bool,
    ):
        if isinstance(request_metadata, bytes):
            request_metadata = pickle.loads(request_metadata)

        assert not is_streaming and not request_metadata.is_streaming
        return message

    async def handle_request_streaming(
        self,
        request_metadata: Union[bytes, RequestMetadata],
        message: str,
        *,
        is_streaming: bool,
    ):
        if isinstance(request_metadata, bytes):
            request_metadata = pickle.loads(request_metadata)

        assert is_streaming and request_metadata.is_streaming
        for i in range(5):
            yield f"{message}-{i}"

    async def handle_request_with_rejection(
        self,
        pickled_request_metadata: bytes,
        *args,
        **kwargs,
    ):
        cancelled_signal_actor = kwargs.pop("cancelled_signal_actor", None)
        if cancelled_signal_actor is not None:
            executing_signal_actor = kwargs.pop("executing_signal_actor")
            async with send_signal_on_cancellation(cancelled_signal_actor):
                await executing_signal_actor.send.remote()

            return

        # Special case: if "raise_task_cancelled_error" is in kwargs, raise TaskCancelledError
        # This simulates the scenario where the underlying Ray task gets cancelled
        if kwargs.pop("raise_task_cancelled_error", False):
            raise TaskCancelledError()

        yield pickle.dumps(self._replica_queue_length_info)
        if not self._replica_queue_length_info.accepted:
            return

        request_metadata = pickle.loads(pickled_request_metadata)
        if request_metadata.is_streaming:
            async for result in self.handle_request_streaming(
                request_metadata, *args, **kwargs
            ):
                yield result
        else:
            yield await self.handle_request(request_metadata, *args, **kwargs)


@pytest.fixture
def setup_fake_replica(ray_instance) -> RunningReplica:
    replica_id = ReplicaID(
        "fake_replica", deployment_id=DeploymentID(name="fake_deployment")
    )
    actor_name = replica_id.to_full_id_str()
    # Create actor with a name so it can be retrieved by get_actor_handle()
    _ = FakeReplicaActor.options(
        name=actor_name, namespace=SERVE_NAMESPACE, lifetime="detached"
    ).remote()
    return RunningReplicaInfo(
        replica_id=replica_id,
        node_id=None,
        node_ip=None,
        availability_zone=None,
        actor_name=actor_name,
        max_ongoing_requests=10,
        is_cross_language=False,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("is_streaming", [False, True])
async def test_send_request_without_rejection(setup_fake_replica, is_streaming: bool):
    replica = RunningReplica(setup_fake_replica)

    pr = PendingRequest(
        args=["Hello"],
        kwargs={"is_streaming": is_streaming},
        metadata=RequestMetadata(
            request_id="abc",
            internal_request_id="def",
            is_streaming=is_streaming,
        ),
    )
    replica_result = replica.try_send_request(pr, with_rejection=False)
    if is_streaming:
        assert isinstance(replica_result.to_object_ref_gen(), ObjectRefGenerator)
        for i in range(5):
            assert await replica_result.__anext__() == f"Hello-{i}"
    else:
        assert isinstance(replica_result.to_object_ref(), ObjectRef)
        assert isinstance(await replica_result.to_object_ref_async(), ObjectRef)
        assert await replica_result.get_async() == "Hello"


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [False, True])
@pytest.mark.parametrize("is_streaming", [False, True])
async def test_send_request_with_rejection(
    setup_fake_replica, accepted: bool, is_streaming: bool
):
    actor_handle = setup_fake_replica.get_actor_handle()
    replica = RunningReplica(setup_fake_replica)
    ray.get(
        actor_handle.set_replica_queue_length_info.remote(
            ReplicaQueueLengthInfo(accepted=accepted, num_ongoing_requests=10),
        )
    )

    pr = PendingRequest(
        args=["Hello"],
        kwargs={"is_streaming": is_streaming},
        metadata=RequestMetadata(
            request_id="abc",
            internal_request_id="def",
            is_streaming=is_streaming,
        ),
    )
    replica_result = replica.try_send_request(pr, with_rejection=True)
    info = await replica_result.get_rejection_response()
    assert info.accepted == accepted
    assert info.num_ongoing_requests == 10
    if not accepted:
        pass
    elif is_streaming:
        assert isinstance(replica_result.to_object_ref_gen(), ObjectRefGenerator)
        for i in range(5):
            assert await replica_result.__anext__() == f"Hello-{i}"
    else:
        assert isinstance(replica_result.to_object_ref(), ObjectRef)
        assert isinstance(await replica_result.to_object_ref_async(), ObjectRef)
        assert await replica_result.get_async() == "Hello"


@pytest.mark.asyncio
async def test_send_request_with_rejection_cancellation(setup_fake_replica):
    """
    Verify that the downstream actor method call is cancelled if the call to send the
    request to the replica is cancelled.
    """
    replica = RunningReplica(setup_fake_replica)

    executing_signal_actor = SignalActor.remote()
    cancelled_signal_actor = SignalActor.remote()

    pr = PendingRequest(
        args=["Hello"],
        kwargs={
            "cancelled_signal_actor": cancelled_signal_actor,
            "executing_signal_actor": executing_signal_actor,
        },
        metadata=RequestMetadata(
            request_id="abc",
            internal_request_id="def",
        ),
    )

    # Send request should hang because the downstream actor method call blocks
    # before sending the system message.
    replica_result = replica.try_send_request(pr, with_rejection=True)
    request_task = get_or_create_event_loop().create_task(
        replica_result.get_rejection_response()
    )

    # Check that the downstream actor method call has started.
    await executing_signal_actor.wait.remote()

    _, pending = await asyncio.wait([request_task], timeout=0.001)
    assert len(pending) == 1

    # Cancel the task. This should cause the downstream actor method call to
    # be cancelled (verified via signal actor).
    request_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request_task

    await cancelled_signal_actor.wait.remote()


@pytest.mark.asyncio
async def test_send_request_with_rejection_task_cancelled_error(setup_fake_replica):
    """
    Test that TaskCancelledError from the underlying Ray task gets converted to
    asyncio.CancelledError when sending request with rejection.
    """
    actor_handle = setup_fake_replica.get_actor_handle()
    replica = RunningReplica(setup_fake_replica)

    # Set up the replica to accept the request
    ray.get(
        actor_handle.set_replica_queue_length_info.remote(
            ReplicaQueueLengthInfo(accepted=True, num_ongoing_requests=5),
        )
    )

    pr = PendingRequest(
        args=["Hello"],
        kwargs={
            "raise_task_cancelled_error": True
        },  # This will trigger TaskCancelledError
        metadata=RequestMetadata(
            request_id="abc",
            internal_request_id="def",
        ),
    )

    # The TaskCancelledError should be caught and converted to asyncio.CancelledError
    replica_result = replica.try_send_request(pr, with_rejection=True)
    with pytest.raises(asyncio.CancelledError):
        await replica_result.get_rejection_response()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
