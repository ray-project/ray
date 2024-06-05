import os
import subprocess
import sys

import pytest
from aiohttp import web

import ray
from ray._private.workers import default_worker
from ray.serve._private.utils import get_random_string


async def anyscaled_service():
    """Mock implementation of anyscaled service."""

    request_ctx = {}
    routes = web.RouteTableDef()

    @routes.post("/execute_ray_worker")
    async def execute_ray_worker(req):
        request_ctx.update(await req.json())

        subprocess.Popen(
            [sys.executable, default_worker.__file__] + request_ctx["args"],
            env=request_ctx["envs"],
        )

        return web.Response(text="hi")

    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.UnixSite(runner, "/tmp/stream.sock")
    await site.start()

    os.environ["ANYSCALE_DATAPLANE_SERVICE_SOCKET"] = "/tmp/stream.sock"

    return request_ctx


@pytest.mark.asyncio
async def test_basic():
    if ray.is_initialized():
        ray.shutdown()

    request_ctx = await anyscaled_service()

    image_uri = f"docker.io/rayproject/ray:{get_random_string()}"

    @ray.remote(runtime_env={"image_uri": image_uri})
    def f():
        print("Calling task f()!")
        return "Hello world"

    ref = f.remote()

    assert await ref == "Hello world"

    # Although the mock anyscaled service does not do anything with the
    # image uri, check that it is passed correctly to the http server
    assert request_ctx["image_uri"] == image_uri

    # RAY_RAYLET_PID and RAY_JOB_ID are crucial to the worker process
    # starting correctly
    assert "RAY_RAYLET_PID" in request_ctx["envs"]
    assert "RAY_JOB_ID" in request_ctx["envs"]

    ray.shutdown()


@pytest.mark.asyncio
async def test_ray_env_var():
    if ray.is_initialized():
        ray.shutdown()

    request_ctx = await anyscaled_service()

    os.environ["RAY_TEST_ABC"] = "123"

    image_uri = f"docker.io/rayproject/ray:{get_random_string()}"

    @ray.remote(runtime_env={"image_uri": image_uri})
    def f():
        print("Calling task f()!")
        return "Hello world"

    ref = f.remote()

    assert await ref == "Hello world"
    assert request_ctx["image_uri"] == image_uri
    assert request_ctx["envs"].get("RAY_TEST_ABC") == "123"

    ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
