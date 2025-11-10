import socket
import sys
import urllib.error
import urllib.parse
import urllib.request

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.authentication.http_token_authentication import (
    format_authentication_http_error,
    get_auth_headers_if_auth_enabled,
)
from ray.core.generated import runtime_env_agent_pb2
from ray.tests.authentication_test_utils import (
    reset_auth_token_state,
    set_auth_mode,
    set_env_auth_token,
)


def _agent_url(agent_address: str, path: str) -> str:
    return urllib.parse.urljoin(agent_address, path)


def _make_get_or_create_request() -> runtime_env_agent_pb2.GetOrCreateRuntimeEnvRequest:
    request = runtime_env_agent_pb2.GetOrCreateRuntimeEnvRequest()
    request.job_id = b"ray_client_test"
    request.serialized_runtime_env = "{}"
    request.runtime_env_config.setup_timeout_seconds = 1
    request.source_process = "pytest"
    return request


def _wait_for_runtime_env_agent(agent_address: str) -> None:
    parsed = urllib.parse.urlparse(agent_address)

    def _can_connect() -> bool:
        try:
            with socket.create_connection((parsed.hostname, parsed.port), timeout=1):
                return True
        except OSError:
            return False

    wait_for_condition(_can_connect, timeout=10)


def test_runtime_env_agent_requires_auth_missing_token(setup_cluster_with_token_auth):
    agent_address = ray._private.worker.global_worker.node.runtime_env_agent_address
    _wait_for_runtime_env_agent(agent_address)
    request = _make_get_or_create_request()

    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(  # noqa: S310 - test controlled
            urllib.request.Request(
                _agent_url(agent_address, "/get_or_create_runtime_env"),
                data=request.SerializeToString(),
                headers={"Content-Type": "application/octet-stream"},
                method="POST",
            ),
            timeout=5,
        )

    assert exc_info.value.code == 401
    body = exc_info.value.read().decode("utf-8", "ignore")
    assert "Missing authentication token" in body
    formatted = format_authentication_http_error(401, body)
    assert formatted.startswith("Authentication required")


def test_runtime_env_agent_rejects_invalid_token(setup_cluster_with_token_auth):
    agent_address = ray._private.worker.global_worker.node.runtime_env_agent_address
    _wait_for_runtime_env_agent(agent_address)
    request = _make_get_or_create_request()

    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(  # noqa: S310 - test controlled
            urllib.request.Request(
                _agent_url(agent_address, "/get_or_create_runtime_env"),
                data=request.SerializeToString(),
                headers={
                    "Content-Type": "application/octet-stream",
                    "Authorization": "Bearer wrong_token",
                },
                method="POST",
            ),
            timeout=5,
        )

    assert exc_info.value.code == 403
    body = exc_info.value.read().decode("utf-8", "ignore")
    assert "Invalid authentication token" in body
    formatted = format_authentication_http_error(403, body)
    assert formatted.startswith("Authentication failed")


def test_runtime_env_agent_accepts_valid_token(setup_cluster_with_token_auth):
    agent_address = ray._private.worker.global_worker.node.runtime_env_agent_address
    _wait_for_runtime_env_agent(agent_address)
    token = setup_cluster_with_token_auth["token"]
    request = _make_get_or_create_request()

    with urllib.request.urlopen(  # noqa: S310 - test controlled
        urllib.request.Request(
            _agent_url(agent_address, "/get_or_create_runtime_env"),
            data=request.SerializeToString(),
            headers={
                "Content-Type": "application/octet-stream",
                "Authorization": f"Bearer {token}",
            },
            method="POST",
        ),
        timeout=5,
    ) as response:
        reply = runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply()
        reply.ParseFromString(response.read())
    assert reply.status == runtime_env_agent_pb2.AgentRpcStatus.AGENT_RPC_STATUS_OK


def test_inject_token_if_enabled_adds_header(cleanup_auth_token_env):
    set_auth_mode("token")
    set_env_auth_token("apptoken1234567890")
    reset_auth_token_state()

    headers = {}
    headers_to_add = get_auth_headers_if_auth_enabled(headers)

    assert headers_to_add != {}
    auth_header = headers_to_add["authorization"]
    if isinstance(auth_header, bytes):
        auth_header = auth_header.decode("utf-8")
    assert auth_header == "Bearer apptoken1234567890"


def test_inject_token_if_enabled_respects_existing_header(cleanup_auth_token_env):
    set_auth_mode("token")
    set_env_auth_token("apptoken1234567890")
    reset_auth_token_state()

    headers = {"authorization": "Bearer custom"}
    headers_to_add = get_auth_headers_if_auth_enabled(headers)

    assert headers_to_add == {}


def test_format_authentication_http_error_non_auth_status():
    assert format_authentication_http_error(404, "not found") is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
