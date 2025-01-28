import asyncio
import pickle
import sys

import grpc
import pytest

import ray
from ray import cloudpickle
from ray._private.test_utils import SignalActor
from ray._private.utils import get_or_create_event_loop
from ray.anyscale.serve._private.replica_scheduler.replica_wrapper import (
    AnyscaleRunningReplica,
)
from ray.serve._private.common import (
    DeploymentID,
    ReplicaID,
    ReplicaQueueLengthInfo,
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.replica_scheduler.common import PendingRequest
from ray.serve._private.test_utils import send_signal_on_cancellation
from ray.serve.generated import serve_proprietary_pb2, serve_proprietary_pb2_grpc
from ray.serve.tests.conftest import *  # noqa


@ray.remote(num_cpus=0)
class FakeReplicaActor:
    def __init__(self):
        self._replica_queue_length_info = None
        self._server = grpc.aio.server()

    async def start(self):
        serve_proprietary_pb2_grpc.add_ASGIServiceServicer_to_server(self, self._server)
        self._port = self._server.add_insecure_port("[::]:0")
        await self._server.start()

        return self._port

    def set_replica_queue_length_info(self, info: ReplicaQueueLengthInfo):
        self._replica_queue_length_info = info

    async def HandleRequest(
        self,
        request: serve_proprietary_pb2.ASGIRequest,
        context: grpc.aio.ServicerContext,
    ):
        args = cloudpickle.loads(request.request_args)
        return serve_proprietary_pb2.ASGIResponse(
            serialized_message=cloudpickle.dumps(args[0])
        )

    async def HandleRequestStreaming(
        self,
        request: serve_proprietary_pb2.ASGIRequest,
        context: grpc.aio.ServicerContext,
    ):
        request_metadata = pickle.loads(request.pickled_request_metadata)
        args = cloudpickle.loads(request.request_args)
        message = args[0]

        for i in range(5):
            if request_metadata.is_http_request:
                yield serve_proprietary_pb2.ASGIResponse(
                    serialized_message=f"{message}-{i}"
                )
            else:
                yield serve_proprietary_pb2.ASGIResponse(
                    serialized_message=cloudpickle.dumps(f"{message}-{i}")
                )

    async def HandleRequestWithRejection(
        self,
        request: serve_proprietary_pb2.ASGIRequest,
        context: grpc.aio.ServicerContext,
    ):
        request_metadata = pickle.loads(request.pickled_request_metadata)
        args = cloudpickle.loads(request.request_args)
        kwargs = cloudpickle.loads(request.request_kwargs)

        cancelled_signal_actor = kwargs.pop("cancelled_signal_actor", None)
        if cancelled_signal_actor is not None:
            executing_signal_actor = kwargs.pop("executing_signal_actor")
            async with send_signal_on_cancellation(cancelled_signal_actor):
                await executing_signal_actor.send.remote()

            return

        await context.send_initial_metadata(
            [
                ("accepted", str(int(self._replica_queue_length_info.accepted))),
                (
                    "num_ongoing_requests",
                    str(self._replica_queue_length_info.num_ongoing_requests),
                ),
            ]
        )
        if not self._replica_queue_length_info.accepted:
            return

        message = args[0]
        if request_metadata.is_streaming:
            for i in range(5):
                if request_metadata.is_http_request:
                    yield serve_proprietary_pb2.ASGIResponse(
                        serialized_message=pickle.dumps(f"{message}-{i}")
                    )
                else:
                    yield serve_proprietary_pb2.ASGIResponse(
                        serialized_message=cloudpickle.dumps(f"{message}-{i}")
                    )
        else:
            yield serve_proprietary_pb2.ASGIResponse(
                serialized_message=cloudpickle.dumps(message)
            )


@pytest.fixture
def setup_fake_replica(ray_instance, request) -> RunningReplicaInfo:
    actor_handle = FakeReplicaActor.remote()
    port = ray.get(actor_handle.start.remote())

    return RunningReplicaInfo(
        ReplicaID(
            "fake_replica",
            deployment_id=DeploymentID(name="fake_deployment"),
        ),
        node_id=None,
        # Just use local node IP
        node_ip="127.0.0.1",
        availability_zone=None,
        actor_handle=actor_handle,
        max_ongoing_requests=10,
        is_cross_language=False,
        # Get grpc port from FakeReplicaActor
        port=port,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("is_streaming", [False, True])
@pytest.mark.parametrize("on_separate_loop", [True, False])
async def test_to_object_ref_not_supported(
    setup_fake_replica: RunningReplicaInfo, is_streaming: bool, on_separate_loop: bool
):
    replica = AnyscaleRunningReplica(setup_fake_replica)
    pr = PendingRequest(
        args=["Hello"],
        kwargs={"is_streaming": is_streaming},
        metadata=RequestMetadata(
            request_id="abc",
            internal_request_id="def",
            is_streaming=is_streaming,
            _by_reference=False,  # use gRPC transport
            _on_separate_loop=on_separate_loop,
        ),
    )
    err_msg = "Converting by-value DeploymentResponses to ObjectRefs is not supported."
    replica_result, _ = await replica.send_request(pr, with_rejection=False)
    if is_streaming:
        with pytest.raises(RuntimeError, match=err_msg):
            replica_result.to_object_ref_gen()
    else:
        with pytest.raises(RuntimeError, match=err_msg):
            await replica_result.to_object_ref_async()

        with pytest.raises(RuntimeError, match=err_msg):
            replica_result.to_object_ref(timeout_s=0.0)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_streaming", [False, True])
@pytest.mark.parametrize("on_separate_loop", [True, False])
async def test_send_request(
    setup_fake_replica: RunningReplicaInfo, is_streaming: bool, on_separate_loop: bool
):
    replica = AnyscaleRunningReplica(setup_fake_replica)
    pr = PendingRequest(
        args=["Hello"],
        kwargs={"is_streaming": is_streaming},
        metadata=RequestMetadata(
            request_id="abc",
            internal_request_id="def",
            is_streaming=is_streaming,
            _by_reference=False,  # use gRPC transport
            _on_separate_loop=on_separate_loop,
        ),
    )
    replica_result, _ = await replica.send_request(pr, with_rejection=False)
    if is_streaming:
        for i in range(5):
            assert await replica_result.__anext__() == f"Hello-{i}"
    else:
        assert await replica_result.get_async() == "Hello"


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [False, True])
@pytest.mark.parametrize("is_streaming", [False, True])
@pytest.mark.parametrize("on_separate_loop", [True, False])
async def test_send_request_with_rejection(
    setup_fake_replica: RunningReplicaInfo,
    accepted: bool,
    is_streaming: bool,
    on_separate_loop: bool,
):
    actor_handle = setup_fake_replica.actor_handle
    replica = AnyscaleRunningReplica(setup_fake_replica)
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
            _by_reference=False,  # use gRPC transport
            _on_separate_loop=on_separate_loop,
        ),
    )
    replica_result, info = await replica.send_request(pr, with_rejection=True)
    assert info.accepted == accepted
    assert info.num_ongoing_requests == 10
    if not accepted:
        assert replica_result is None
    elif is_streaming:
        for i in range(5):
            assert await replica_result.__anext__() == f"Hello-{i}"
    else:
        assert await replica_result.__anext__() == "Hello"


@pytest.mark.asyncio
@pytest.mark.parametrize("on_separate_loop", [True, False])
async def test_send_request_with_rejection_cancellation(
    setup_fake_replica: RunningReplicaInfo, on_separate_loop: bool
):
    """
    Verify that the downstream actor method call is cancelled if the call to send the
    request to the replica is cancelled.
    """
    replica = AnyscaleRunningReplica(setup_fake_replica)
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
            _by_reference=False,  # use gRPC transport
            _on_separate_loop=on_separate_loop,
        ),
    )

    # Send request should hang because the downstream actor method call blocks
    # before sending the system message.
    send_request_task = get_or_create_event_loop().create_task(
        replica.send_request(pr, with_rejection=True)
    )

    # Check that the downstream actor method call has started.
    await executing_signal_actor.wait.remote()

    _, pending = await asyncio.wait([send_request_task], timeout=0.001)
    assert len(pending) == 1

    # Cancel the task. This should cause the downstream actor method call to
    # be cancelled (verified via signal actor).
    send_request_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await send_request_task

    await cancelled_signal_actor.wait.remote()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
