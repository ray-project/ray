import sys
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Tuple
from urllib.parse import urljoin

import grpc
import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from starlette.requests import Request

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.common import RequestProtocol
from ray.serve._private.test_utils import get_application_url
from ray.serve.exceptions import BackPressureError
from ray.serve.generated import serve_pb2, serve_pb2_grpc


def test_handle_backpressure(serve_instance):
    """Requests should raise a BackPressureError once the limit is reached."""

    signal_actor = SignalActor.remote()

    @serve.deployment(max_ongoing_requests=1, max_queued_requests=1)
    class Deployment:
        async def __call__(self, msg: str) -> str:
            await signal_actor.wait.remote()
            return msg

    handle = serve.run(Deployment.bind())

    # First response should block. Until the signal is sent, all subsequent requests
    # will be queued in the handle.
    first_response = handle.remote("hi-1")
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 1)

    # Check that beyond the 1st queued request, others are dropped due to backpressure.
    second_response = handle.remote("hi-2")
    for _ in range(10):
        with pytest.raises(BackPressureError):
            handle.remote().result()

    # Send the signal; the first request will be unblocked and the second should
    # subsequently get scheduled and executed.
    ray.get(signal_actor.send.remote())
    assert first_response.result() == "hi-1"
    assert second_response.result() == "hi-2"

    ray.get(signal_actor.send.remote(clear=True))
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 0)


def test_http_backpressure(serve_instance):
    """Requests should return a 503 once the limit is reached."""

    signal_actor = SignalActor.remote()

    @serve.deployment(max_ongoing_requests=1, max_queued_requests=1)
    class Deployment:
        async def __call__(self, request: Request) -> str:
            msg = (await request.json())["msg"]
            await signal_actor.wait.remote()
            return msg

    serve.run(Deployment.bind())

    @ray.remote(num_cpus=0)
    def do_request(msg: str) -> Tuple[int, str]:
        application_url = get_application_url()
        r = httpx.request("GET", application_url, json={"msg": msg}, timeout=30.0)
        return r.status_code, r.text

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
        assert status_code == 503
        assert text.startswith("Request dropped due to backpressure")

    # Send the signal; the first request will be unblocked and the second should
    # subsequently get scheduled and executed.
    ray.get(signal_actor.send.remote())
    assert ray.get(first_ref) == (200, "hi-1")
    assert ray.get(second_ref) == (200, "hi-2")

    ray.get(signal_actor.send.remote(clear=True))
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 0)


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


def test_model_composition_backpressure(serve_instance):
    signal_actor = SignalActor.remote()

    @serve.deployment(max_ongoing_requests=1, max_queued_requests=1)
    class Child:
        async def __call__(self):
            await signal_actor.wait.remote()
            return "ok"

    @serve.deployment
    class Parent:
        def __init__(self, child):
            self.child = child

        async def __call__(self):
            return await self.child.remote()

    def send_request():
        return httpx.get(get_application_url())

    serve.run(Parent.bind(child=Child.bind()))
    with ThreadPoolExecutor(max_workers=3) as exc:
        # Send first request, wait for it to be blocked while executing.
        executing_fut = exc.submit(send_request)
        wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 1)
        done, _ = wait([executing_fut], timeout=0.1, return_when=FIRST_COMPLETED)
        assert len(done) == 0

        # Send second request, it should get queued.
        queued_fut = exc.submit(send_request)
        done, _ = wait(
            [executing_fut, queued_fut], timeout=0.1, return_when=FIRST_COMPLETED
        )
        assert len(done) == 0

        # Send third request, it should get rejected.
        rejected_fut = exc.submit(send_request)
        assert rejected_fut.result().status_code == 503

        # Send signal, check the two requests succeed.
        ray.get(signal_actor.send.remote(clear=False))
        assert executing_fut.result().status_code == 200
        assert executing_fut.result().text == "ok"
        assert queued_fut.result().status_code == 200
        assert queued_fut.result().text == "ok"

    ray.get(signal_actor.send.remote(clear=True))
    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 0)


@pytest.mark.parametrize("request_type", ["async_non_gen", "sync_non_gen"])
def test_model_composition_backpressure_with_fastapi(serve_instance, request_type):
    """Tests backpressure behavior with FastAPI model composition.

    Tests that when a Child deployment with max_ongoing_requests=1 and max_queued_requests=1
    is called through a Parent FastAPI deployment:
    1. First request blocks while executing
    2. Second request gets queued
    3. Third request gets rejected with 503 status code
    4. After unblocking, first two requests complete successfully

    Tests both async and sync non-generator endpoints.
    """
    signal_actor = SignalActor.remote()
    app = FastAPI()

    @serve.deployment(max_ongoing_requests=1, max_queued_requests=1)
    class Child:
        async def __call__(self):
            await signal_actor.wait.remote()
            return "ok"

    @serve.deployment
    @serve.ingress(app)
    class Parent:
        def __init__(self, child):
            self.child = child

        @app.get("/async_non_gen")
        async def async_non_gen(self):
            result = await self.child.remote()
            return PlainTextResponse(result)

        @app.get("/sync_non_gen")
        def sync_non_gen(self):
            result = self.child.remote().result()
            return PlainTextResponse(result)

    def send_request():
        url_map = {
            "async_non_gen": urljoin(get_application_url(), "async_non_gen"),
            "sync_non_gen": urljoin(get_application_url(), "sync_non_gen"),
        }
        resp = httpx.get(url_map[request_type])
        return resp

    serve.run(Parent.bind(child=Child.bind()))

    with ThreadPoolExecutor(max_workers=3) as exc:
        executing_fut = exc.submit(send_request)
        wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 1)
        done, _ = wait([executing_fut], timeout=0.1, return_when=FIRST_COMPLETED)
        assert len(done) == 0

        queued_fut = exc.submit(send_request)
        done, _ = wait(
            [executing_fut, queued_fut], timeout=0.1, return_when=FIRST_COMPLETED
        )
        assert len(done) == 0

        rejected_fut = exc.submit(send_request)
        assert rejected_fut.result().status_code == 503

        # Send signal, let the two requests succeed.
        ray.get(signal_actor.send.remote())
        assert executing_fut.result().status_code == 200
        assert executing_fut.result().text == "ok"
        assert queued_fut.result().status_code == 200
        assert queued_fut.result().text == "ok"

    wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 0)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
