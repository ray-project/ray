import sys
from typing import Tuple

import grpc
import pytest

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.common import RequestProtocol
from ray.serve._private.test_utils import get_application_url
from ray.serve.generated import serve_pb2, serve_pb2_grpc


def test_grpc_backpressure(serve_instance):
    """Requests should return UNAVAILABLE once the limit is reached."""

    signal_actor = SignalActor.remote()

    @serve.deployment(max_ongoing_requests=1, max_queued_requests=1)
    class Deployment:
        async def __call__(self, request: serve_pb2.UserDefinedMessage):
            await signal_actor.wait.remote()
            return serve_pb2.UserDefinedResponse(greeting=request.name)

    serve.run(Deployment.bind())

    @ray.remote(num_cpus=0)
    def do_request(msg: str) -> Tuple[grpc.StatusCode, str]:
        channel = grpc.insecure_channel(
            get_application_url(protocol=RequestProtocol.GRPC)
        )
        stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
        try:
            response, call = stub.__call__.with_call(
                serve_pb2.UserDefinedMessage(name=msg)
            )
            return call.code(), response.greeting
        except grpc.RpcError as e:
            return e.code(), e.details()

    # First response should block. Until the signal is sent, all subsequent requests
    # will be queued in the handle.
    first_ref = do_request.remote("hi-1")
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 1)
    _, pending = ray.wait([first_ref], timeout=0.1)
    assert len(pending) == 1

    # Check that beyond the 1st queued request, others are dropped due to backpressure.
    second_ref = do_request.remote("hi-2")
    _, pending = ray.wait([second_ref], timeout=0.1)
    for _ in range(10):
        status_code, text = ray.get(do_request.remote(("hi-err")))
        assert status_code == grpc.StatusCode.RESOURCE_EXHAUSTED
        assert text.startswith("Request dropped due to backpressure")

    # Send the signal; the first request will be unblocked and the second should
    # subsequently get scheduled and executed.
    ray.get(signal_actor.send.remote())
    assert ray.get(first_ref) == (grpc.StatusCode.OK, "hi-1")
    assert ray.get(second_ref) == (grpc.StatusCode.OK, "hi-2")

    ray.get(signal_actor.send.remote(clear=True))
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 0)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
