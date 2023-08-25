import asyncio
import os
import pytest
import sys
from typing import AsyncGenerator

from fastapi import FastAPI
import requests
from starlette.requests import Request

import ray
from ray.actor import ActorHandle
from ray._private.test_utils import SignalActor

from ray import serve
from ray.serve.handle import RayServeHandle
from ray.serve._private.constants import RAY_SERVE_ENABLE_NEW_ROUTING


"""
- test client HTTP disconnection during assignment
- test client HTTP disconnection during execution
- test timeout during assignment
- test timeout during execution
- test handle call during assignment
- test unary handle call during execution
- test generator handle call during execution
- test downstream calls are cancelled automatically
"""


async def send_signal_on_cancellation(signal_actor: ActorHandle):
    try:
        await asyncio.sleep(100000)
    except asyncio.CancelledError:
        await signal_actor.send.remote()


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_NEW_ROUTING,
    reason="New routing feature flag is disabled.",
)
@pytest.mark.parametrize("use_fastapi", [False, True])
def test_cancel_on_http_client_disconnect(serve_instance, use_fastapi: bool):
    signal_actor = SignalActor.remote()

    if use_fastapi:
        app = FastAPI()

        @serve.deployment
        @serve.ingress(app)
        class SimpleGenerator:
            @app.get("/")
            async def wait_for_cancellation(self):
                await send_signal_on_cancellation(signal_actor)

    else:

        @serve.deployment
        class SimpleGenerator:
            async def __call__(self, request: Request):
                await send_signal_on_cancellation(signal_actor)

    serve.run(SimpleGenerator.bind())

    with pytest.raises(requests.exceptions.ReadTimeout):
        requests.get("http://localhost:8000", timeout=0.1)
    ray.get(signal_actor.wait.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
