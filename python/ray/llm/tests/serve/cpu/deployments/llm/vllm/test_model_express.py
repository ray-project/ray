import asyncio
import importlib
import os
import socket
import sys
import threading
import urllib.error
import urllib.request
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ray.llm._internal.serve.engines.vllm import model_express
from ray.llm._internal.serve.engines.vllm.patches.compile_cache_compat import _rewrite
from ray.llm._internal.serve.serving_patterns.model_express import envs as mx_envs
from ray.llm._internal.serve.serving_patterns.model_express.mx_server import (
    ModelExpressServer,
)
from ray.serve.llm import LLMConfig
from ray.serve.llm.mx import build_mx_app

MX_ADDRESS = "10.0.0.1:41000"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(autouse=True)
def reset_ready_server():
    """Reset the module's process-wide ready server between tests."""
    yield
    if model_express._ready_server is not None:
        model_express._ready_server.shutdown()
        model_express._ready_server.server_close()
    model_express._ready_server = None
    model_express._engine_ready.clear()


def _llm_config() -> LLMConfig:
    return LLMConfig(
        model_loading_config=dict(model_id="Qwen/Qwen3-0.6B"),
        engine_kwargs=dict(load_format="modelexpress", tensor_parallel_size=2),
    )


def _setup(env: dict):
    """Run setup_model_express; returns the env and the app names looked up."""
    looked_up = []

    def get_deployment_handle(deployment_name, app_name):
        looked_up.append((deployment_name, app_name))

        async def address():
            return MX_ADDRESS

        return SimpleNamespace(address=SimpleNamespace(remote=address))

    with patch.dict("os.environ", env, clear=True), patch(
        "ray.serve.get_deployment_handle", get_deployment_handle
    ), patch("ray.util.get_node_ip_address", return_value="127.0.0.1"):
        asyncio.run(model_express.setup_model_express(_llm_config()))
        return dict(os.environ), looked_up


def test_is_model_express():
    assert model_express.is_model_express(_llm_config())
    assert not model_express.is_model_express(
        LLMConfig(model_loading_config=dict(model_id="Qwen/Qwen3-0.6B"))
    )


@pytest.mark.parametrize(
    "extra",
    [
        dict(placement_group_config=dict(bundles=[{"GPU": 0.49}])),
        dict(runtime_env=dict(env_vars={"VLLM_RAY_PER_WORKER_GPUS": "0.5"})),
    ],
)
def test_setup_rejects_fractional_gpus(extra):
    llm_config = LLMConfig(
        model_loading_config=dict(model_id="Qwen/Qwen3-0.6B"),
        engine_kwargs=dict(load_format="modelexpress"),
        **extra,
    )
    with pytest.raises(ValueError, match="fractional GPUs"):
        asyncio.run(model_express.setup_model_express(llm_config))


def test_setup_without_mx_app():
    def get_deployment_handle(deployment_name, app_name):
        raise KeyError(f"{deployment_name} in {app_name} does not exist.")

    with patch.dict("os.environ", {}, clear=True), patch(
        "ray.serve.get_deployment_handle", get_deployment_handle
    ), patch("ray.util.get_node_ip_address", return_value="127.0.0.1"):
        with pytest.raises(RuntimeError, match="build_mx_app"):
            asyncio.run(model_express.setup_model_express(_llm_config()))


def test_setup_sets_client_env():
    env, looked_up = _setup(env={})

    # The server's address comes from the `mx` app's deployment.
    assert looked_up == [("ModelExpressServer", "mx")]
    assert env["MX_SERVER_ADDRESS"] == MX_ADDRESS
    # MX adds each worker's GPU index to the bases.
    assert env["MX_METADATA_PORT"] == "15555"
    assert env["MX_WORKER_GRPC_PORT"] == "16555"
    # Copied to every vLLM worker, which can be on other nodes; MX detects it.
    assert "MX_WORKER_HOST" not in env
    assert env["MX_MODEL_NAME_OVERRIDE"] == "Qwen/Qwen3-0.6B"
    assert env["MX_ARTIFACT_TRANSFER"] == "1"
    # The ready endpoint listens on the port in the URL.
    ready_port = model_express._ready_server.server_address[1]
    assert env["MX_ARTIFACT_READY_URL"] == f"http://127.0.0.1:{ready_port}/ready"
    # vLLM only copies listed env prefixes into its Ray workers.
    prefixes = env["VLLM_RAY_EXTRA_ENV_VAR_PREFIXES_TO_COPY"].split(",")
    assert {"MX_", "MODEL_EXPRESS_"} <= set(prefixes)


def test_setup_uses_configured_env():
    env, looked_up = _setup(
        env={
            "RAY_MX_APP_NAME": "mx-2",
            "RAY_MX_METADATA_PORT_BASE": "25555",
            "RAY_MX_WORKER_GRPC_PORT_BASE": "26555",
        },
    )
    assert looked_up == [("ModelExpressServer", "mx-2")]
    assert env["MX_METADATA_PORT"] == "25555"
    assert env["MX_WORKER_GRPC_PORT"] == "26555"


def test_setup_logs_mx_env():
    with patch.object(model_express.logger, "info") as info:
        env, _ = _setup(env={"OTHER": "x"})
    message = info.call_args.args[0] % info.call_args.args[1:]
    logged = dict(item.split("=", 1) for item in message.split(": ", 1)[1].split())
    assert logged == {
        name: value
        for name, value in env.items()
        if name.startswith(("MX_", "MODEL_EXPRESS_"))
    }
    assert "MX_SERVER_ADDRESS" in logged and "OTHER" not in logged


def test_setup_keeps_user_env():
    env, looked_up = _setup(
        env={
            "MX_SERVER_ADDRESS": "mx:9000",
            "MX_METADATA_PORT": "40000",
            "VLLM_RAY_EXTRA_ENV_VAR_PREFIXES_TO_COPY": "FOO_",
        },
    )
    assert looked_up == []
    assert env["MX_SERVER_ADDRESS"] == "mx:9000"
    assert env["MX_METADATA_PORT"] == "40000"
    assert env["VLLM_RAY_EXTRA_ENV_VAR_PREFIXES_TO_COPY"].split(",") == [
        "FOO_",
        "MX_",
        "MODEL_EXPRESS_",
    ]


def test_ready_endpoint_reports_engine_start():
    env, _ = _setup(env={})
    url = env["MX_ARTIFACT_READY_URL"]

    with pytest.raises(urllib.error.HTTPError) as e:
        urllib.request.urlopen(url, timeout=5)
    assert e.value.code == 503

    model_express.mark_engine_ready()
    assert urllib.request.urlopen(url, timeout=5).status == 200


def test_mx_server_envs():
    with patch.dict("os.environ", {}, clear=True):
        assert mx_envs.get_mx_server_binary() == "/usr/local/bin/modelexpress-server"
        assert mx_envs.get_mx_app_name() == "mx"
        assert mx_envs.get_mx_metadata_port_base() == 15555
        assert mx_envs.get_mx_worker_grpc_port_base() == 16555
        assert mx_envs.get_mx_server_port() == 8931
        assert importlib.reload(mx_envs).MX_SERVER_HEALTH_CHECK_PERIOD_S == 10
    with patch.dict(
        "os.environ",
        {
            "RAY_MX_SERVER_BINARY_PATH": "/opt/mx/server",
            "RAY_MX_SERVER_PORT": "9100",
            "RAY_MX_SERVER_HEALTH_CHECK_PERIOD_S": "2.5",
        },
    ):
        assert mx_envs.get_mx_server_binary() == "/opt/mx/server"
        assert mx_envs.get_mx_server_port() == 9100
        assert importlib.reload(mx_envs).MX_SERVER_HEALTH_CHECK_PERIOD_S == 2.5
    importlib.reload(mx_envs)


def test_build_mx_app():
    deployment = build_mx_app({"metadata_backend": "redis"})._bound_deployment
    assert deployment.name == "ModelExpressServer"
    assert deployment.init_kwargs == {"metadata_backend": "redis"}
    assert build_mx_app()._bound_deployment.init_kwargs == {}


def test_mx_server_missing_binary():
    with patch.dict("os.environ", {"RAY_MX_SERVER_BINARY_PATH": "/nonexistent/mx"}):
        with pytest.raises(FileNotFoundError, match="RAY_MX_SERVER_BINARY_PATH"):
            ModelExpressServer.func_or_class()


@pytest.mark.skipif(
    not os.access(mx_envs.get_mx_server_binary(), os.X_OK),
    reason="MX server binary not installed",
)
def test_mx_server_listens_on_configured_port(tmp_path, monkeypatch):
    # The server's default cache directory is relative to its working directory.
    monkeypatch.chdir(tmp_path)
    port = _free_port()
    with patch.dict("os.environ", {"RAY_MX_SERVER_PORT": str(port)}), patch(
        "ray.util.get_node_ip_address", return_value="127.0.0.1"
    ):
        server = ModelExpressServer.func_or_class()
    try:
        assert server.address() == f"127.0.0.1:{port}"
        socket.create_connection(("127.0.0.1", port), timeout=5).close()
        server.check_health()
    finally:
        server._proc.terminate()
        server._proc.wait(timeout=10)


@pytest.mark.skipif(
    not os.access(mx_envs.get_mx_server_binary(), os.X_OK),
    reason="MX server binary not installed",
)
def test_mx_server_waits_for_port_from_replaced_replica(tmp_path, monkeypatch):
    # The replica being replaced still listens on the port for a moment.
    monkeypatch.chdir(tmp_path)
    port = _free_port()
    old = socket.socket()
    old.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    old.bind(("127.0.0.1", port))
    old.listen()
    threading.Timer(2, old.close).start()
    with patch.dict("os.environ", {"RAY_MX_SERVER_PORT": str(port)}), patch(
        "ray.util.get_node_ip_address", return_value="127.0.0.1"
    ):
        server = ModelExpressServer.func_or_class()
    try:
        server.check_health()
    finally:
        server._proc.terminate()
        server._proc.wait(timeout=10)


def test_mx_server_port_never_released():
    port = _free_port()
    with socket.socket() as old:
        old.bind(("127.0.0.1", port))
        old.listen()
        with patch.dict(
            "os.environ",
            {"RAY_MX_SERVER_PORT": str(port), "RAY_MX_SERVER_BINARY_PATH": "/bin/true"},
        ), patch("subprocess.Popen") as popen:
            with pytest.raises(TimeoutError, match="still in use"):
                ModelExpressServer.func_or_class(startup_timeout_s=1)
        popen.assert_not_called()


def test_compile_cache_compat_rewrites_device_literals():
    code = (
        "with torch.cuda._DeviceGuard(1):\n"
        "    torch.cuda.set_device(1)\n"
        "    stream1 = get_raw_stream(1)\n"
        "    x = tl.program_id(0)\n"
        "    y = other.get_raw_stream(0)\n"
    )
    current = "__import__('torch').cuda.current_device()"
    assert _rewrite(code) == (
        f"with torch.cuda._DeviceGuard({current}):\n"
        f"    torch.cuda.set_device({current})\n"
        f"    stream1 = get_raw_stream({current})\n"
        "    x = tl.program_id(0)\n"
        "    y = other.get_raw_stream(0)\n"
    )


@pytest.mark.parametrize(
    "vllm_has_flag,user_env,expected_env,expected_torch_flag",
    [
        # vLLM supports the mode but leaves it off: turn it on through vLLM.
        (True, None, "1", False),
        # A user's choice wins.
        (True, "0", "0", False),
        # Older vLLM on torch >= 2.14: set torch's mode directly.
        (False, None, None, True),
    ],
)
def test_compile_cache_compat_on_torch_with_compile_on_one_rank(
    vllm_has_flag, user_env, expected_env, expected_torch_flag
):
    import torch.compiler
    import vllm.envs

    from ray.llm._internal.serve.engines.vllm.patches import compile_cache_compat

    if not vllm_has_flag and hasattr(vllm.envs, "VLLM_COMPILE_ON_ONE_RANK"):
        pytest.skip("installed vLLM already has VLLM_COMPILE_ON_ONE_RANK")
    env = {"VLLM_COMPILE_ON_ONE_RANK": user_env} if user_env else {}
    # Stands in for torch >= 2.14's compiler config.
    compiler_config = SimpleNamespace(compile_on_one_rank=False)
    with patch.dict("os.environ", env, clear=True), patch.object(
        torch.compiler, "config", compiler_config
    ), patch.object(compile_cache_compat, "_add_compile_cache_factor"):
        if vllm_has_flag:
            with patch.object(
                vllm.envs, "VLLM_COMPILE_ON_ONE_RANK", False, create=True
            ):
                compile_cache_compat.apply()
        else:
            compile_cache_compat.apply()
        assert os.environ.get("VLLM_COMPILE_ON_ONE_RANK") == expected_env
        assert compiler_config.compile_on_one_rank is expected_torch_flag


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
