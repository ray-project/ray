import asyncio
import importlib
import os
import socket
import sys
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
from ray.serve.schema import ReplicaRank

MX_ADDRESS = "10.0.0.1:41000"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def ready_port_base():
    """Reset the module's process-wide ready server between tests."""
    base = _free_port()
    yield base
    if model_express._ready_server is not None:
        model_express._ready_server.shutdown()
        model_express._ready_server.server_close()
    model_express._ready_server = None
    model_express._engine_ready.clear()


def _llm_config(ready_port_base: int, **experimental) -> LLMConfig:
    return LLMConfig(
        model_loading_config=dict(model_id="Qwen/Qwen3-0.6B"),
        engine_kwargs=dict(load_format="modelexpress", tensor_parallel_size=2),
        experimental_configs={
            model_express.MX_READY_PORT_BASE_KEY: ready_port_base,
            **experimental,
        },
    )


def _setup(llm_config: LLMConfig, local_rank: int, env: dict):
    """Run setup_model_express; returns the env and the app names looked up."""
    looked_up = []

    def get_deployment_handle(deployment_name, app_name):
        looked_up.append((deployment_name, app_name))

        async def address():
            return MX_ADDRESS

        return SimpleNamespace(address=SimpleNamespace(remote=address))

    replica_context = SimpleNamespace(
        rank=ReplicaRank(rank=5, node_rank=0, local_rank=local_rank)
    )
    with patch.dict("os.environ", env, clear=True), patch(
        "ray.serve.get_replica_context", return_value=replica_context
    ), patch("ray.serve.get_deployment_handle", get_deployment_handle), patch(
        "ray.util.get_node_ip_address", return_value="127.0.0.1"
    ):
        asyncio.run(model_express.setup_model_express(llm_config))
        return dict(os.environ), looked_up


def test_is_model_express():
    assert model_express.is_model_express(_llm_config(0))
    assert not model_express.is_model_express(
        LLMConfig(model_loading_config=dict(model_id="Qwen/Qwen3-0.6B"))
    )


def test_setup_sets_client_env(ready_port_base):
    env, looked_up = _setup(_llm_config(ready_port_base), local_rank=1, env={})

    # The server's address comes from the `mx` app's deployment.
    assert looked_up == [("ModelExpressServer", "mx")]
    assert env["MX_SERVER_ADDRESS"] == MX_ADDRESS
    # Colocated replicas of a deployment get disjoint blocks of num_devices ports.
    offset = 1 * 2
    assert env["MX_METADATA_PORT"] == str(15555 + offset)
    assert env["MX_WORKER_GRPC_PORT"] == str(16555 + offset)
    assert env["MX_WORKER_HOST"] == "127.0.0.1"
    assert env["MX_MODEL_NAME_OVERRIDE"] == "Qwen/Qwen3-0.6B"
    assert env["MX_ARTIFACT_TRANSFER"] == "1"
    assert env["MX_ARTIFACT_READY_URL"] == (
        f"http://127.0.0.1:{ready_port_base + offset}/ready"
    )
    # vLLM only copies listed env prefixes into its Ray workers.
    prefixes = env["VLLM_RAY_EXTRA_ENV_VAR_PREFIXES_TO_COPY"].split(",")
    assert {"MX_", "MODEL_EXPRESS_"} <= set(prefixes)


def test_setup_uses_configured_app_name(ready_port_base):
    config = _llm_config(ready_port_base, **{model_express.MX_APP_NAME_KEY: "mx-2"})
    _, looked_up = _setup(config, local_rank=0, env={})
    assert looked_up == [("ModelExpressServer", "mx-2")]


def test_setup_keeps_user_env(ready_port_base):
    env, looked_up = _setup(
        _llm_config(ready_port_base),
        local_rank=0,
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


def test_ready_endpoint_reports_engine_start(ready_port_base):
    env, _ = _setup(_llm_config(ready_port_base), local_rank=0, env={})
    url = env["MX_ARTIFACT_READY_URL"]

    with pytest.raises(urllib.error.HTTPError) as e:
        urllib.request.urlopen(url, timeout=5)
    assert e.value.code == 503

    model_express.mark_engine_ready()
    assert urllib.request.urlopen(url, timeout=5).status == 200


def test_mx_server_envs():
    with patch.dict("os.environ", {}, clear=True):
        assert mx_envs.get_mx_server_binary() == "/usr/local/bin/modelexpress-server"
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
