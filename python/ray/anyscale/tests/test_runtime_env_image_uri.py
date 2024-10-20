import asyncio
import os
import subprocess
import sys

import pytest
from aiohttp import web

import ray
from ray._private.workers import default_worker
from ray.exceptions import RuntimeEnvSetupError
from ray.runtime_env import RuntimeEnvConfig
from ray.serve._private.utils import get_random_string


async def anyscaled_service(pull_delay_s: int = 0):
    """Mock implementation of anyscaled service."""

    request_ctx = {}
    routes = web.RouteTableDef()

    @routes.post("/pull_image")
    async def pull_image(req) -> web.Response:
        req_json = await req.json()
        print("Received pull_image request with", req_json)

        request_ctx.update(req_json)

        await asyncio.sleep(pull_delay_s)
        return web.Response(text="hi")

    @routes.post("/execute_ray_worker")
    async def execute_ray_worker(req) -> web.Response:
        req_json = await req.json()
        print("Received execute_ray_worker request with", req_json)

        # Check request context from pull image request
        assert request_ctx["image_uri"] == req_json["image_uri"]
        assert request_ctx["check_ray_and_python_version"]

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

    return request_ctx, runner


@pytest.mark.asyncio
async def test_basic():
    if ray.is_initialized():
        ray.shutdown()

    request_ctx, runner = await anyscaled_service()

    image_uri = f"docker.io/rayproject/ray:{get_random_string()}"

    @ray.remote(runtime_env={"image_uri": image_uri})
    def f():
        print("Calling task f()!")
        return "Hello world"

    assert request_ctx == {}
    ref = f.remote()

    assert await ref == "Hello world"

    # Although the mock anyscaled service does not do anything with the
    # image uri, check that it is passed correctly to the http server
    assert request_ctx["image_uri"] == image_uri

    # RAY_RAYLET_PID and RAY_JOB_ID are crucial to the worker process
    # starting correctly
    assert "RAY_RAYLET_PID" in request_ctx["envs"]
    assert "RAY_JOB_ID" in request_ctx["envs"]

    await runner.cleanup()


@pytest.mark.asyncio
async def test_user_env_var():
    """Test that user provided environment variables are passed to the worker."""

    if ray.is_initialized():
        ray.shutdown()

    request_ctx, runner = await anyscaled_service()

    image_uri = f"docker.io/rayproject/ray:{get_random_string()}"

    @ray.remote(
        runtime_env={"image_uri": image_uri, "env_vars": {"TEST_ABC": "hello world"}}
    )
    def f():
        print("Calling task f()!")
        return "Hello world"

    assert request_ctx == {}
    ref = f.remote()

    assert await ref == "Hello world"
    assert request_ctx["image_uri"] == image_uri
    assert request_ctx["envs"].get("TEST_ABC") == "hello world"

    await runner.cleanup()


@pytest.mark.asyncio
async def test_ray_env_var():
    """Test that RAY-prefixed environment variables are passed to the worker."""

    if ray.is_initialized():
        ray.shutdown()

    request_ctx, runner = await anyscaled_service()

    os.environ["RAY_TEST_ABC"] = "123"

    image_uri = f"docker.io/rayproject/ray:{get_random_string()}"

    @ray.remote(runtime_env={"image_uri": image_uri})
    def f():
        print("Calling task f()!")
        return "Hello world"

    assert request_ctx == {}
    ref = f.remote()

    assert await ref == "Hello world"
    assert request_ctx["image_uri"] == image_uri
    assert request_ctx["envs"].get("RAY_TEST_ABC") == "123"

    await runner.cleanup()


@pytest.mark.asyncio
async def test_runtime_env_setup_timeout():
    if ray.is_initialized():
        ray.shutdown()

    request_ctx, runner = await anyscaled_service(pull_delay_s=100)
    image_uri = f"docker.io/rayproject/ray:{get_random_string()}"

    @ray.remote(
        runtime_env={
            "image_uri": image_uri,
            "config": RuntimeEnvConfig(setup_timeout_seconds=1),
        }
    )
    def f():
        return "Hello world"

    assert request_ctx == {}
    ref = f.remote()

    with pytest.raises(
        RuntimeEnvSetupError,
        match="Failed to install runtime_env within the timeout of 1 seconds",
    ):
        await ref

    assert request_ctx["image_uri"] == image_uri
    assert "args" not in request_ctx and "envs" not in request_ctx

    await runner.cleanup()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
