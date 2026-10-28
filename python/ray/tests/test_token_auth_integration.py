"""Integration tests for token-based authentication in Ray (part 1).

Cluster lifecycle, connection, and end-to-end operation tests.
"""

import os
import secrets
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple
from unittest.mock import patch

import pytest

import ray
from ray._common.network_utils import find_free_port
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import client_test_enabled
from ray.cluster_utils import Cluster
from ray.exceptions import AuthenticationError

try:
    from ray._raylet import AuthenticationTokenLoader

    _RAYLET_AVAILABLE = True
except ImportError:
    _RAYLET_AVAILABLE = False
    AuthenticationTokenLoader = None

from ray._private.authentication.authentication_constants import (
    TOKEN_AUTH_FIX_MESSAGE,
)
from ray._private.authentication_test_utils import (
    authentication_env_guard,
    clear_auth_token_sources,
    reset_auth_token_state,
    set_auth_mode,
    set_env_auth_token,
)

pytestmark = pytest.mark.skipif(
    not _RAYLET_AVAILABLE,
    reason="Authentication tests require ray._raylet (not available in minimal installs)",
)


def _run_ray_start_and_verify_status(
    args: list, env: dict, expect_success: bool = True, timeout: int = 30
) -> subprocess.CompletedProcess:
    """Helper to run ray start command with proper error handling."""
    result = subprocess.run(
        ["ray", "start"] + args,
        env={"RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER": "1", **env},
        capture_output=True,
        text=True,
        timeout=timeout,
    )

    if expect_success:
        assert result.returncode == 0, (
            f"ray start should have succeeded. "
            f"stdout: {result.stdout}, stderr: {result.stderr}"
        )
    else:
        assert result.returncode != 0, (
            f"ray start should have failed but succeeded. "
            f"stdout: {result.stdout}, stderr: {result.stderr}"
        )
        # Check that error message mentions token
        error_output = result.stdout + result.stderr
        assert (
            "authentication token" in error_output.lower()
            or "token" in error_output.lower()
        ), f"Error message should mention token. Got: {error_output}"

    return result


def _cleanup_ray_start(env: Optional[dict] = None):
    """Helper to clean up ray start processes."""
    # Ensure any ray.init() connection is closed first
    if ray.is_initialized():
        ray.shutdown()

    # Stop with a longer timeout
    subprocess.run(
        ["ray", "stop", "--force"],
        env=env,
        capture_output=True,
        timeout=60,  # Increased timeout for flaky cleanup
        check=False,  # Don't raise on non-zero exit
    )

    # Wait for ray processes to actually stop
    def ray_stopped():
        result = subprocess.run(
            ["ray", "status"],
            capture_output=True,
            check=False,
        )
        # ray status returns non-zero when no cluster is running
        return result.returncode != 0

    try:
        wait_for_condition(ray_stopped, timeout=10, retry_interval_ms=500)
    except Exception:
        # Best effort - don't fail the test if we can't verify it stopped
        pass


@pytest.fixture(autouse=True)
def _auto_clean_token_sources(clean_token_sources):
    """Opt in to the shared clean_token_sources fixture for every test."""
    yield


@pytest.mark.skipif(
    client_test_enabled(),
    reason="This test is for starting a new local cluster, not compatible with client mode",
)
def test_local_cluster_generates_token():
    """Test ray.init() generates token for local cluster when auth_mode=token is set."""
    # Ensure no token exists
    default_token_path = Path.home() / ".ray" / "auth_token"
    assert (
        not default_token_path.exists()
    ), f"Token file already exists at {default_token_path}"

    # Enable token auth via environment variable
    set_auth_mode("token")
    reset_auth_token_state()

    # Initialize Ray with token auth
    ray.init()

    try:
        # Verify token file was created
        assert default_token_path.exists(), (
            f"Token file was not created at {default_token_path}. "
            f"HOME={os.environ.get('HOME')}, "
            f"Files in {default_token_path.parent}: {list(default_token_path.parent.iterdir()) if default_token_path.parent.exists() else 'directory does not exist'}"
        )
        token = default_token_path.read_text().strip()
        assert len(token) == 64
        assert all(c in "0123456789abcdef" for c in token)
        if sys.platform != "win32":
            assert default_token_path.stat().st_mode & 0o777 == 0o600

        # Verify cluster is working
        assert ray.is_initialized()

    finally:
        ray.shutdown()


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Starts a new local cluster; not compatible with client mode",
)
def test_ray_init_generates_token_and_enforces_auth_by_default():
    """ray.init() (no address) enables token auth by default: it generates a
    token and the cluster enforces authentication, even without RAY_AUTH_MODE."""
    import requests

    from ray._raylet import AuthenticationMode, get_authentication_mode

    default_token_path = Path.home() / ".ray" / "auth_token"
    assert not default_token_path.exists()

    # RAY_AUTH_MODE intentionally unset.
    os.environ.pop("RAY_AUTH_MODE", None)
    reset_auth_token_state()

    ctx = ray.init()
    try:
        assert default_token_path.exists()
        token = default_token_path.read_text().strip()
        assert len(token) == 64
        assert get_authentication_mode() == AuthenticationMode.TOKEN

        # Basic ops work for the in-process driver (it holds the token).
        @ray.remote
        def f():
            return "ok"

        assert ray.get(f.remote()) == "ok"

        # The cluster enforces auth: an unauthenticated request is rejected and
        # an authenticated one succeeds.
        dashboard_url = f"http://{ctx.address_info['webui_url']}"
        resp = requests.get(f"{dashboard_url}/api/v0/actors")
        assert resp.status_code == 401, resp.text
        resp = requests.get(
            f"{dashboard_url}/api/v0/actors",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 200, resp.text
    finally:
        ray.shutdown()


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Starts a new local cluster; not compatible with client mode",
)
def test_ray_init_reuses_existing_token_by_default():
    """ray.init() reuses an existing default token instead of regenerating it."""
    from ray._private.authentication_test_utils import set_default_auth_token
    from ray._raylet import AuthenticationMode, get_authentication_mode

    existing_token = "a" * 64
    default_token_path = set_default_auth_token(existing_token)

    os.environ.pop("RAY_AUTH_MODE", None)
    reset_auth_token_state()

    ray.init()
    try:
        assert get_authentication_mode() == AuthenticationMode.TOKEN
        # The pre-existing token is reused, not overwritten.
        assert default_token_path.read_text().strip() == existing_token
    finally:
        ray.shutdown()


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Starts a new local cluster; not compatible with client mode",
)
def test_ray_init_respects_disabled_opt_out():
    """RAY_AUTH_MODE=disabled keeps a new local ray.init() cluster unauthenticated
    and generates no token."""
    from ray._raylet import AuthenticationMode, get_authentication_mode

    default_token_path = Path.home() / ".ray" / "auth_token"
    assert not default_token_path.exists()

    set_auth_mode("disabled")
    reset_auth_token_state()

    ray.init()
    try:
        assert get_authentication_mode() == AuthenticationMode.DISABLED
        assert not default_token_path.exists()
    finally:
        ray.shutdown()


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray start, not compatible with client mode",
)
def test_ray_init_connect_does_not_generate_token():
    """ray.init(address=...) connecting to an existing cluster never auto-enables
    token auth or generates a token, even with RAY_AUTH_MODE unset. Auto-enable is
    scoped to new local clusters only."""
    from ray._raylet import AuthenticationMode, get_authentication_mode

    # Start a non-auth head (RAY_AUTH_MODE/token unset -> starts disabled).
    env = os.environ.copy()
    env.pop("RAY_AUTH_MODE", None)
    env.pop("RAY_AUTH_TOKEN", None)
    env.pop("RAY_AUTH_TOKEN_PATH", None)

    default_token_path = Path.home() / ".ray" / "auth_token"
    assert not default_token_path.exists()

    try:
        _run_ray_start_and_verify_status(
            ["--head", "--port=0"], env, expect_success=True
        )
        # The head itself generated no token (no source, RAY_AUTH_MODE unset).
        assert not default_token_path.exists()

        # Connect in-process with RAY_AUTH_MODE unset -> routes through the
        # connect branch, which must not auto-enable or generate a token.
        os.environ.pop("RAY_AUTH_MODE", None)
        reset_auth_token_state()
        ray.init(address="auto")

        assert get_authentication_mode() == AuthenticationMode.DISABLED
        assert not default_token_path.exists()

        @ray.remote
        def f():
            return "ok"

        assert ray.get(f.remote()) == "ok"
    finally:
        _cleanup_ray_start(env)


def test_connect_without_token_raises_error(setup_cluster_with_token_auth):
    """Test ray.init(address=...) without token fails when auth_mode=token is set."""
    cluster_info = setup_cluster_with_token_auth
    cluster = cluster_info["cluster"]

    # Disconnect the current driver session and drop token state before retrying.
    ray.shutdown()
    set_auth_mode("disabled")
    clear_auth_token_sources(remove_default=True)
    reset_auth_token_state()

    # Ensure no token exists
    token_loader = AuthenticationTokenLoader.instance()
    assert not token_loader.has_token()

    with pytest.raises(ConnectionError) as exc_info:
        ray.init(address=cluster.address)
    assert isinstance(exc_info.value, AuthenticationError)


def test_ray_init_address_without_token_does_not_create_token(
    setup_cluster_with_token_auth: dict,
) -> None:
    """ray.init(address=...) with token auth on and no token fails, and no token
    file gets created."""
    cluster = setup_cluster_with_token_auth["cluster"]
    ray.shutdown()
    clear_auth_token_sources(remove_default=True)
    reset_auth_token_state()
    default_token_path = Path.home() / ".ray" / "auth_token"

    with pytest.raises(AuthenticationError):
        ray.init(address=cluster.address)
    assert not default_token_path.exists()


@pytest.mark.parametrize(
    "token,expected_status",
    [
        (None, 401),  # No token -> Unauthorized
        ("wrong_token", 403),  # Wrong token -> Forbidden
    ],
    ids=["no_token", "wrong_token"],
)
def test_state_api_auth_failure(token, expected_status, setup_cluster_with_token_auth):
    """Test that state API calls fail with missing or incorrect token."""
    import requests

    cluster_info = setup_cluster_with_token_auth
    dashboard_url = cluster_info["dashboard_url"]

    # Make direct HTTP request to state API endpoint
    headers = {}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"

    response = requests.get(f"{dashboard_url}/api/v0/actors", headers=headers)

    assert response.status_code == expected_status, (
        f"State API should return {expected_status}, got {response.status_code}: "
        f"{response.text}"
    )


@pytest.mark.parametrize("tokens_match", [True, False])
def test_cluster_token_authentication(tokens_match, setup_cluster_with_token_auth):
    """Test cluster authentication with matching and non-matching tokens."""
    cluster_info = setup_cluster_with_token_auth
    cluster = cluster_info["cluster"]
    cluster_token = cluster_info["token"]

    # Reconfigure the driver token state to simulate fresh connections.
    ray.shutdown()
    set_auth_mode("token")

    if tokens_match:
        client_token = cluster_token  # Same token - should succeed
    else:
        client_token = "b" * 64  # Different token - should fail

    set_env_auth_token(client_token)
    reset_auth_token_state()

    if tokens_match:
        # Should succeed - test gRPC calls work
        ray.init(address=cluster.address)

        obj_ref = ray.put("test_data")
        result = ray.get(obj_ref)
        assert result == "test_data"

        @ray.remote
        def test_func():
            return "success"

        result = ray.get(test_func.remote())
        assert result == "success"

        ray.shutdown()

    else:
        # Should fail - connection or gRPC calls should fail
        with pytest.raises(ConnectionError) as exc_info:
            ray.init(address=cluster.address)
            try:
                ray.put("test")
            finally:
                ray.shutdown()
        assert isinstance(exc_info.value, AuthenticationError)


def _token_mode_env_without_token() -> dict:
    """This process's env with token auth on and no token source."""
    env = os.environ.copy()
    env["RAY_AUTH_MODE"] = "token"
    env.pop("RAY_AUTH_TOKEN", None)
    env.pop("RAY_AUTH_TOKEN_PATH", None)
    return env


@pytest.fixture
def token_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point HOME at an empty temp dir and return the default token path in it."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    return tmp_path / ".ray" / "auth_token"


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray start, not compatible with client mode",
)
def test_ray_start_head_without_token_fails(tmp_path: Path) -> None:
    """ray start --head with token auth on and no token fails before starting
    any process, tells the user to generate a token, and creates none."""
    env = _token_mode_env_without_token()

    default_token_path = Path.home() / ".ray" / "auth_token"
    assert not default_token_path.exists()

    try:
        temp_dir = tmp_path / "ray"
        result = _run_ray_start_and_verify_status(
            ["--head", "--port=0", f"--temp-dir={temp_dir}"], env, expect_success=False
        )
        output = result.stdout + result.stderr
        assert "ray get-auth-token --generate" in output
        assert TOKEN_AUTH_FIX_MESSAGE not in output
        assert not default_token_path.exists()
        # The head node creates its session directory before starting any process.
        assert not list(temp_dir.glob("session_*"))
    finally:
        _cleanup_ray_start(env)


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray start, not compatible with client mode",
)
def test_ray_start_head_reports_a_missing_token_path_file(tmp_path: Path) -> None:
    """ray start --head with RAY_AUTH_TOKEN_PATH naming a missing file reports
    that file, not the head no-token error."""
    env = _token_mode_env_without_token()
    env["RAY_AUTH_TOKEN_PATH"] = str(tmp_path / "missing_token")

    try:
        result = _run_ray_start_and_verify_status(
            ["--head", "--port=0"], env, expect_success=False
        )
        output = result.stdout + result.stderr
        assert "RAY_AUTH_TOKEN_PATH" in output
        assert "--generate" not in output
    finally:
        _cleanup_ray_start(env)


@pytest.mark.skipif(
    client_test_enabled() or sys.platform == "win32",
    reason="AutoscalingCluster uses subprocess ray start and the fake "
    "multi-node provider, which don't run in client mode or on Windows",
)
def test_autoscaling_cluster_creates_token() -> None:
    """AutoscalingCluster() creates the token before its `ray start --head` and
    runs with auth on in an empty HOME, even when the test process set
    RAY_AUTH_MODE=disabled, which shutdown() then restores."""
    from ray.cluster_utils import AutoscalingCluster

    set_auth_mode("disabled")
    reset_auth_token_state()
    default_token_path = Path.home() / ".ray" / "auth_token"
    assert not default_token_path.exists()

    cluster = AutoscalingCluster(
        head_resources={"CPU": 1},
        worker_node_types={},
    )
    try:
        cluster.start()
        assert os.environ["RAY_AUTH_MODE"] == "token"
        assert default_token_path.exists()
        reset_auth_token_state()
        ray.init(address="auto")

        @ray.remote
        def f():
            return "ok"

        assert ray.get(f.remote()) == "ok"
    finally:
        ray.shutdown()
        cluster.shutdown()
    assert os.environ["RAY_AUTH_MODE"] == "disabled"


@pytest.mark.parametrize("leftover_file_content", [None, b"", b" \t\n\x0b\x0c\r\n"])
def test_create_token_if_missing(
    leftover_file_content: Optional[bytes], token_home: Path
) -> None:
    """Token creation writes an owner-only token, overwriting a leftover token
    file that holds no token."""
    token_path = token_home
    if leftover_file_content is not None:
        token_path.parent.mkdir()
        token_path.write_bytes(leftover_file_content)

    reset_auth_token_state()
    AuthenticationTokenLoader.instance().create_token_if_missing()

    assert len(token_path.read_text()) == 64
    if sys.platform != "win32":
        assert token_path.stat().st_mode & 0o777 == 0o600


@pytest.mark.skipif(
    sys.platform == "win32" or os.geteuid() == 0,
    reason="Needs POSIX permissions that apply to the current user",
)
def test_create_token_if_missing_overwrites_an_unreadable_file(
    token_home: Path,
) -> None:
    """A leftover token file the owner can write but not read is overwritten,
    as before this change, instead of crashing token creation."""
    token_path = token_home
    token_path.parent.mkdir()
    token_path.write_bytes(b"")
    token_path.chmod(0o200)

    reset_auth_token_state()
    AuthenticationTokenLoader.instance().create_token_if_missing()

    assert token_path.stat().st_mode & 0o777 == 0o600
    assert len(token_path.read_text()) == 64


@pytest.mark.skipif(
    sys.platform == "win32" or os.geteuid() == 0,
    reason="Needs POSIX permissions that apply to the current user",
)
def test_create_token_if_missing_reports_an_unwritable_home(
    token_home: Path,
) -> None:
    """A token file that can't be written raises RuntimeError naming the file,
    not an AuthenticationError telling the user to copy a token from a head."""
    token_dir = token_home.parent
    token_dir.mkdir()
    token_dir.chmod(0o500)
    reset_auth_token_state()

    try:
        with pytest.raises(RuntimeError, match=str(token_dir / "auth_token")):
            AuthenticationTokenLoader.instance().create_token_if_missing()
    finally:
        token_dir.chmod(0o700)


@pytest.mark.parametrize(
    "token_file_content, loaded_token",
    [
        (b"c" * 64, "c" * 64),
        (b"\n  \n" + b"c" * 64 + b"\n", "c" * 64),
        # Not UTF-8, but non-blank, so the token loader takes it as the token.
        (b"\xff\xfe", None),
    ],
)
def test_token_creation_keeps_an_existing_token(
    token_file_content: bytes, loaded_token: Optional[str], token_home: Path
) -> None:
    """A token file that holds a token, on its first non-blank line as the token
    loader reads it, is left unchanged by token creation."""
    from ray._private.authentication.authentication_token_setup import (
        ensure_token_if_auth_enabled,
    )

    token_path = token_home
    token_path.parent.mkdir()
    token_path.write_bytes(token_file_content)
    set_auth_mode("token")
    reset_auth_token_state()

    ensure_token_if_auth_enabled(create_token_if_missing=True)
    reset_auth_token_state()
    AuthenticationTokenLoader.instance().create_token_if_missing()
    # The CLI returns at has_token here; this checks only the user-facing path.
    cli = subprocess.run(
        ["ray", "get-auth-token", "--generate"],
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        errors="replace",
    )
    # Printing a non-UTF-8 token fails on its own; only the file matters then.
    if loaded_token is not None:
        assert cli.returncode == 0, cli.stderr
        assert cli.stdout == loaded_token

    if loaded_token is not None and not client_test_enabled():
        from ray.cluster_utils import Cluster

        Cluster(initialize_head=True).shutdown()

    assert token_path.read_bytes() == token_file_content
    if loaded_token is not None:
        reset_auth_token_state()
        assert AuthenticationTokenLoader.instance().get_raw_token() == loaded_token


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Starts a new local cluster; not compatible with client mode",
)
@pytest.mark.parametrize(
    "cluster_kwargs,config_mode",
    [({"enable_token_auth": False}, "token"), ({}, "disabled")],
)
def test_cluster_utils_rejects_auth_mode_in_system_config(
    cluster_kwargs: dict, config_mode: str
) -> None:
    """Cluster() rejects an AUTH_MODE in _system_config that disagrees with its
    mode, as ray.init() does: a test cluster's mode can only be set with
    enable_token_auth. The earlier RAY_AUTH_MODE comes back after the failure."""
    from ray.cluster_utils import Cluster

    set_auth_mode("token")
    reset_auth_token_state()

    with pytest.raises(RuntimeError, match="remove AUTH_MODE from _system_config"):
        Cluster(
            initialize_head=True,
            head_node_args={"_system_config": {"AUTH_MODE": config_mode}},
            **cluster_kwargs,
        )
    assert os.environ["RAY_AUTH_MODE"] == "token"


def test_autoscaling_cluster_enable_token_auth_false_disables_auth() -> None:
    """AutoscalingCluster(enable_token_auth=False) starts its head with
    RAY_AUTH_MODE=disabled even when the test process set token mode, creates no
    token, and restores the earlier mode on shutdown."""
    from ray.cluster_utils import AutoscalingCluster

    set_auth_mode("token")
    reset_auth_token_state()
    cluster = AutoscalingCluster(
        head_resources={"CPU": 1}, worker_node_types={}, enable_token_auth=False
    )

    with patch("subprocess.check_call") as check_call:
        cluster.start()
        head_start_env = check_call.call_args_list[-1].kwargs["env"]
        assert head_start_env["RAY_AUTH_MODE"] == "disabled"
        assert os.environ["RAY_AUTH_MODE"] == "disabled"
        cluster.shutdown()
    assert os.environ["RAY_AUTH_MODE"] == "token"
    assert not (Path.home() / ".ray" / "auth_token").exists()


def test_autoscaling_cluster_rejects_auth_mode_in_system_config() -> None:
    """AutoscalingCluster.start() rejects a disagreeing _system_config AUTH_MODE
    before `ray stop --force` or `ray start --head` runs."""
    from ray.cluster_utils import AutoscalingCluster

    set_auth_mode("token")
    reset_auth_token_state()
    cluster = AutoscalingCluster(head_resources={"CPU": 1}, worker_node_types={})

    with patch("subprocess.check_call") as check_call:
        with pytest.raises(RuntimeError, match="remove AUTH_MODE from _system_config"):
            cluster.start(_system_config={"AUTH_MODE": "disabled"})
    check_call.assert_not_called()
    assert os.environ["RAY_AUTH_MODE"] == "token"


def test_autoscaling_cluster_start_failure_restores_auth_mode() -> None:
    """When AutoscalingCluster.start() fails after setting RAY_AUTH_MODE, it puts
    back the earlier mode."""
    from ray.cluster_utils import AutoscalingCluster

    set_auth_mode("token")
    reset_auth_token_state()
    cluster = AutoscalingCluster(
        head_resources={"CPU": 1}, worker_node_types={}, enable_token_auth=False
    )

    with patch(
        "subprocess.check_call",
        side_effect=subprocess.CalledProcessError(1, "ray start --head"),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            cluster.start()
    assert os.environ["RAY_AUTH_MODE"] == "token"


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Starts a new local cluster; not compatible with client mode",
)
@pytest.mark.parametrize(
    "earlier_mode,enable_token_auth",
    [(None, None), ("disabled", None), (None, True), (None, False), ("token", False)],
)
def test_cluster_utils_enable_token_auth_overrides_auth_mode(
    earlier_mode: Optional[str],
    enable_token_auth: Optional[bool],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cluster()'s enable_token_auth decides the auth mode whatever RAY_AUTH_MODE
    the test process had: True, the default (None here omits the argument),
    creates ~/.ray/auth_token and runs with auth on, False creates none and runs
    with auth off. A driver from the test process connects either way, and
    shutdown() restores the earlier RAY_AUTH_MODE."""
    from ray.cluster_utils import Cluster

    if earlier_mode is None:
        monkeypatch.delenv("RAY_AUTH_MODE", raising=False)
    else:
        set_auth_mode(earlier_mode)
    reset_auth_token_state()
    default_token_path = Path.home() / ".ray" / "auth_token"
    assert not default_token_path.exists()

    cluster_kwargs = (
        {} if enable_token_auth is None else {"enable_token_auth": enable_token_auth}
    )
    expect_token_auth = enable_token_auth is not False
    from ray.cluster_utils import _TestClusterAuthMode

    cluster = Cluster(initialize_head=True, **cluster_kwargs)
    try:
        # Per-test env cleanup in conftest.py skips while this is non-zero.
        assert _TestClusterAuthMode.active_count == 1
        assert os.environ["RAY_AUTH_MODE"] == (
            "token" if expect_token_auth else "disabled"
        )
        assert default_token_path.exists() == expect_token_auth
        ray.init(address=cluster.address)

        @ray.remote
        def f():
            return "ok"

        assert ray.get(f.remote()) == "ok"
    finally:
        ray.shutdown()
        cluster.shutdown()
    assert _TestClusterAuthMode.active_count == 0
    assert os.environ.get("RAY_AUTH_MODE") == earlier_mode


def test_cluster_utils_head_start_failure_unsets_auth_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When starting the head fails, in Cluster(initialize_head=True) or a later
    add_node(), the cluster unsets the RAY_AUTH_MODE it set."""
    from ray.cluster_utils import Cluster

    monkeypatch.delenv("RAY_AUTH_MODE", raising=False)
    reset_auth_token_state()

    with patch("ray._private.node.Node", side_effect=RuntimeError("head failed")):
        with pytest.raises(RuntimeError, match="head failed"):
            Cluster(initialize_head=True)
        cluster = Cluster()
        with pytest.raises(RuntimeError, match="head failed"):
            cluster.add_node()
    assert "RAY_AUTH_MODE" not in os.environ


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Starts a new local cluster; not compatible with client mode",
)
@pytest.mark.parametrize("failing_step", ["_wait_for_node", "connect"])
def test_cluster_utils_failure_after_head_start_unsets_auth_mode(
    failing_step: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When Cluster(initialize_head=True, connect=True) fails after the head
    started, waiting for the head or connecting to it, it unsets the
    RAY_AUTH_MODE it set."""
    from ray.cluster_utils import Cluster

    monkeypatch.delenv("RAY_AUTH_MODE", raising=False)
    reset_auth_token_state()
    started_clusters = []

    def fail(cluster: Cluster, *args, **kwargs) -> None:
        started_clusters.append(cluster)
        raise RuntimeError(f"{failing_step} failed")

    try:
        with patch.object(Cluster, failing_step, autospec=True, side_effect=fail):
            with pytest.raises(RuntimeError, match=f"{failing_step} failed"):
                Cluster(initialize_head=True, connect=True)
        assert "RAY_AUTH_MODE" not in os.environ
    finally:
        for cluster in started_clusters:
            cluster.shutdown()


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray start, not compatible with client mode",
)
def test_ray_start_address_without_token_raises_error(
    setup_cluster_with_token_auth: dict,
) -> None:
    """ray start --address with token auth on and no token fails, and no token
    file gets created."""
    cluster = setup_cluster_with_token_auth["cluster"]
    ray.shutdown()

    env = _token_mode_env_without_token()

    default_token_path = Path.home() / ".ray" / "auth_token"
    assert not default_token_path.exists()

    result = _run_ray_start_and_verify_status(
        [f"--address={cluster.address}"], env, expect_success=False
    )
    assert "ray get-auth-token" in result.stdout + result.stderr
    assert not default_token_path.exists()


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray start, not compatible with client mode",
)
def test_ray_start_head_with_token_succeeds():
    """Test that ray start --head succeeds when token auth is enabled with a valid token."""
    # Set up environment with token auth and a valid token
    test_token = "a" * 64
    env = os.environ.copy()
    env["RAY_AUTH_TOKEN"] = test_token
    env["RAY_AUTH_MODE"] = "token"

    try:
        # Start head node - should succeed
        _run_ray_start_and_verify_status(
            ["--head", "--port=0"], env, expect_success=True
        )

        # Verify we can connect to the cluster with ray.init()
        set_env_auth_token(test_token)
        set_auth_mode("token")
        reset_auth_token_state()

        # Wait for cluster to be ready
        def cluster_ready():
            try:
                ray.init(address="auto")
                return True
            except Exception:
                return False

        wait_for_condition(cluster_ready, timeout=10)
        assert ray.is_initialized()

        # Test basic operations work
        @ray.remote
        def test_func():
            return "success"

        result = ray.get(test_func.remote())
        assert result == "success"

    finally:
        # Cleanup handles ray.shutdown() internally
        _cleanup_ray_start(env)


@pytest.mark.parametrize(
    "token_source, auth_mode_env, expected_enabled",
    [
        # No RAY_AUTH_MODE set + a token available from any source -> auto-enable.
        ("default", None, True),  # ~/.ray/auth_token
        ("env", None, True),  # RAY_AUTH_TOKEN
        ("path", None, True),  # RAY_AUTH_TOKEN_PATH
        # No RAY_AUTH_MODE set + no token available -> stays disabled.
        ("none", None, False),
        # Explicit RAY_AUTH_MODE wins over auto-detection in both directions.
        ("default", "disabled", False),
        ("none", "token", True),
    ],
)
def test_maybe_enable_token_auth_if_token_available(
    token_source, auth_mode_env, expected_enabled, tmp_path
):
    """The helper enables token auth for a local head cluster when a token is
    available from any source and RAY_AUTH_MODE isn't explicitly set. It warns
    whenever RAY_AUTH_MODE is unset (that auth was enabled or left disabled), and
    stays silent when the mode is set explicitly."""
    from unittest import mock

    import ray._private.authentication.authentication_token_setup as auth_setup
    from ray._private.authentication_test_utils import (
        set_auth_token_path,
        set_default_auth_token,
        set_env_auth_token,
    )
    from ray._raylet import AuthenticationMode, get_authentication_mode

    test_token = "a" * 64

    with authentication_env_guard():
        clear_auth_token_sources(remove_default=True)
        os.environ.pop("RAY_AUTH_MODE", None)

        if auth_mode_env is not None:
            os.environ["RAY_AUTH_MODE"] = auth_mode_env

        if token_source == "default":
            set_default_auth_token(test_token)
        elif token_source == "env":
            set_env_auth_token(test_token)
        elif token_source == "path":
            set_auth_token_path(test_token, tmp_path / "auth_token")

        reset_auth_token_state()

        with mock.patch.object(auth_setup.logger, "warning") as mock_warning:
            enabled = auth_setup.maybe_enable_token_auth_if_token_available()

        assert enabled is expected_enabled

        if expected_enabled:
            assert get_authentication_mode() == AuthenticationMode.TOKEN
        else:
            # The explicit setting (or lack thereof) is preserved.
            assert os.environ.get("RAY_AUTH_MODE") == auth_mode_env
            assert get_authentication_mode() == AuthenticationMode.DISABLED

        # Warn only when RAY_AUTH_MODE is unset (that auth was enabled or left
        # disabled); an explicit mode is respected silently.
        if auth_mode_env is None:
            mock_warning.assert_called_once()
            expected_phrase = (
                "Token authentication is enabled"
                if expected_enabled
                else "Token authentication is disabled"
            )
            assert expected_phrase in mock_warning.call_args.args[0]
        else:
            mock_warning.assert_not_called()

    reset_auth_token_state()


def test_maybe_enable_token_auth_if_token_available_fails_closed(tmp_path):
    """With RAY_AUTH_MODE unset, a configured-but-unreadable token source
    (an empty RAY_AUTH_TOKEN_PATH file) fails closed: the helper raises
    AuthenticationError rather than silently starting unauthenticated."""
    import ray._private.authentication.authentication_token_setup as auth_setup
    from ray.exceptions import AuthenticationError

    empty_token_file = tmp_path / "auth_token"
    empty_token_file.write_text("")

    with authentication_env_guard():
        clear_auth_token_sources(remove_default=True)
        os.environ.pop("RAY_AUTH_MODE", None)
        os.environ["RAY_AUTH_TOKEN_PATH"] = str(empty_token_file)
        reset_auth_token_state()

        with pytest.raises(AuthenticationError):
            auth_setup.maybe_enable_token_auth_if_token_available()

    reset_auth_token_state()


@pytest.mark.parametrize(
    "auth_mode_env, token_exists, expected_enabled",
    [
        # No RAY_AUTH_MODE set -> token auth enabled by default, token or not.
        (None, False, True),
        (None, True, True),
        # Explicit RAY_AUTH_MODE wins in both directions.
        ("disabled", True, False),
        ("token", False, True),
    ],
)
def test_enable_token_auth_by_default(auth_mode_env, token_exists, expected_enabled):
    """The helper enables token auth by default for a new local ray.init()
    cluster unless RAY_AUTH_MODE is explicitly set. It warns that auth was
    enabled when RAY_AUTH_MODE is unset, and stays silent when it is set."""
    from unittest import mock

    import ray._private.authentication.authentication_token_setup as auth_setup
    from ray._private.authentication_test_utils import set_default_auth_token
    from ray._raylet import AuthenticationMode, get_authentication_mode

    with authentication_env_guard():
        clear_auth_token_sources(remove_default=True)
        os.environ.pop("RAY_AUTH_MODE", None)

        if auth_mode_env is not None:
            os.environ["RAY_AUTH_MODE"] = auth_mode_env
        if token_exists:
            set_default_auth_token("a" * 64)

        reset_auth_token_state()

        with mock.patch.object(auth_setup.logger, "warning") as mock_warning:
            enabled = auth_setup.enable_token_auth_by_default()

        assert enabled is expected_enabled

        # Warn only when RAY_AUTH_MODE is unset (auth enabled by default); an
        # explicit mode is respected silently.
        if auth_mode_env is None:
            mock_warning.assert_called_once()
            assert "Token authentication is enabled" in mock_warning.call_args.args[0]
        else:
            mock_warning.assert_not_called()

        if expected_enabled:
            assert get_authentication_mode() == AuthenticationMode.TOKEN
        else:
            # An explicit RAY_AUTH_MODE=disabled is preserved.
            assert os.environ.get("RAY_AUTH_MODE") == auth_mode_env
            assert get_authentication_mode() == AuthenticationMode.DISABLED

    reset_auth_token_state()


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray start, not compatible with client mode",
)
def test_ray_start_head_enables_auth_from_existing_token():
    """ray start --head enables token auth automatically when a token exists at
    the default path, even without RAY_AUTH_MODE set."""
    from ray._private.authentication_test_utils import set_default_auth_token

    test_token = "a" * 64
    default_token_path = set_default_auth_token(test_token)
    assert default_token_path.exists()

    # RAY_AUTH_MODE is intentionally not set for the head node.
    env = os.environ.copy()
    env.pop("RAY_AUTH_MODE", None)
    env.pop("RAY_AUTH_TOKEN", None)
    env.pop("RAY_AUTH_TOKEN_PATH", None)

    try:
        _run_ray_start_and_verify_status(
            ["--head", "--port=0"], env, expect_success=True
        )

        # Authentication is enforced: connecting without the token fails.
        set_auth_mode("disabled")
        clear_auth_token_sources(remove_default=False)
        reset_auth_token_state()
        with pytest.raises(ConnectionError) as exc_info:
            ray.init(address="auto")
        assert isinstance(exc_info.value, AuthenticationError)

        # Connecting with the matching token succeeds.
        set_auth_mode("token")
        set_env_auth_token(test_token)
        reset_auth_token_state()

        def cluster_ready():
            try:
                ray.init(address="auto")
                return True
            except Exception:
                return False

        wait_for_condition(cluster_ready, timeout=10)
        assert ray.is_initialized()

    finally:
        _cleanup_ray_start(env)


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray start, not compatible with client mode",
)
@pytest.mark.parametrize("address", [None, "auto", "explicit"])
def test_ray_init_connect_reuses_existing_token(address, propagate_logs, caplog):
    """ray.init() finding a local cluster (no address or "auto") reuses the token
    of a head that enabled auth from it; an explicit address never auto-enables."""
    from ray._private.authentication_test_utils import set_default_auth_token
    from ray._private.services import canonicalize_bootstrap_address
    from ray._raylet import AuthenticationMode, get_authentication_mode

    set_default_auth_token("b" * 64)

    env = os.environ.copy()
    env.pop("RAY_AUTH_MODE", None)
    env.pop("RAY_AUTH_TOKEN", None)
    env.pop("RAY_AUTH_TOKEN_PATH", None)
    env.pop("RAY_ADDRESS", None)

    try:
        _run_ray_start_and_verify_status(
            ["--head", "--port=0"], env, expect_success=True
        )

        os.environ.pop("RAY_AUTH_MODE", None)
        reset_auth_token_state()

        if address == "explicit":
            with pytest.raises(ConnectionError) as exc_info:
                ray.init(address=canonicalize_bootstrap_address("auto"))
            assert isinstance(exc_info.value, AuthenticationError)
            assert get_authentication_mode() == AuthenticationMode.DISABLED
            return

        with caplog.at_level(
            "WARNING", logger="ray._private.authentication.authentication_token_setup"
        ):
            ray.init(address=address)

        assert get_authentication_mode() == AuthenticationMode.TOKEN
        assert "Token authentication is enabled" in caplog.text

        @ray.remote
        def f():
            return "ok"

        assert ray.get(f.remote()) == "ok"
    finally:
        _cleanup_ray_start(env)


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray start, not compatible with client mode",
)
@pytest.mark.parametrize(
    "auth_mode, expect_warn",
    [
        (None, True),  # RAY_AUTH_MODE unset + no token -> disabled + warn.
        ("disabled", False),  # explicit RAY_AUTH_MODE=disabled -> no warn.
    ],
    ids=["unset_no_token", "explicit_disabled"],
)
def test_ray_start_head_warns_when_auth_disabled(auth_mode, expect_warn):
    """ray start --head warns that it runs without token auth only when
    RAY_AUTH_MODE is unset; an explicit RAY_AUTH_MODE=disabled is silent."""
    # No token available anywhere.
    env = os.environ.copy()
    env.pop("RAY_AUTH_MODE", None)
    env.pop("RAY_AUTH_TOKEN", None)
    env.pop("RAY_AUTH_TOKEN_PATH", None)
    if auth_mode is not None:
        env["RAY_AUTH_MODE"] = auth_mode

    default_token_path = Path.home() / ".ray" / "auth_token"
    assert not default_token_path.exists()

    try:
        result = _run_ray_start_and_verify_status(
            ["--head", "--port=0"], env, expect_success=True
        )
        output = result.stdout + result.stderr
        warned = "Token authentication is disabled" in output
        assert (
            warned is expect_warn
        ), f"Expected disabled-warning={expect_warn}. Got: {output}"
    finally:
        _cleanup_ray_start(env)


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray start, not compatible with client mode",
)
@pytest.mark.parametrize("token_match", ["correct", "incorrect"])
def test_ray_start_address_with_token(token_match, setup_cluster_with_token_auth):
    """Test ray start --address=... with correct or incorrect token."""
    cluster_info = setup_cluster_with_token_auth
    cluster = cluster_info["cluster"]
    cluster_token = cluster_info["token"]

    # Reset the driver connection to reuse the fixture-backed cluster.
    ray.shutdown()
    set_auth_mode("token")

    # Set up environment for worker
    env = os.environ.copy()
    env["RAY_AUTH_MODE"] = "token"

    if token_match == "correct":
        env["RAY_AUTH_TOKEN"] = cluster_token
        expect_success = True
    else:
        env["RAY_AUTH_TOKEN"] = "b" * 64
        expect_success = False

    # Start worker node
    _run_ray_start_and_verify_status(
        [f"--address={cluster.address}", "--num-cpus=1"],
        env,
        expect_success=expect_success,
    )

    if token_match == "correct":
        try:
            # Connect and verify the cluster has 2 nodes (head + worker)
            set_env_auth_token(cluster_token)
            reset_auth_token_state()
            ray.init(address=cluster.address)

            def worker_joined():
                return len(ray.nodes()) >= 2

            wait_for_condition(worker_joined, timeout=10)

            nodes = ray.nodes()
            assert (
                len(nodes) >= 2
            ), f"Expected at least 2 nodes, got {len(nodes)}: {nodes}"

        finally:
            if ray.is_initialized():
                ray.shutdown()
            _cleanup_ray_start(env)


def _assert_token_auth_error(output: str):
    from ray._private.authentication.authentication_constants import (
        TOKEN_AUTH_ERROR_MESSAGE,
    )

    assert TOKEN_AUTH_ERROR_MESSAGE in output, output


@pytest.fixture
def disabled_head_with_token_file():
    """A head with RAY_AUTH_MODE=disabled, and a token file in the default path."""
    from ray._common.network_utils import find_free_port
    from ray._private.authentication_test_utils import set_default_auth_token

    set_default_auth_token("c" * 64)
    port = find_free_port()
    client_server_port = find_free_port()
    dashboard_port = find_free_port()
    env = os.environ.copy()
    env.pop("RAY_AUTH_TOKEN", None)
    env.pop("RAY_AUTH_TOKEN_PATH", None)
    # Keep the default GCS connect retries (~20s) so a client that retries a
    # rejection exceeds the 15s timeouts below instead of passing.
    env.pop("RAY_NUM_REDIS_GET_RETRIES", None)
    env["RAY_AUTH_MODE"] = "disabled"
    try:
        _run_ray_start_and_verify_status(
            [
                "--head",
                f"--port={port}",
                f"--ray-client-server-port={client_server_port}",
                f"--dashboard-port={dashboard_port}",
            ],
            env,
            expect_success=True,
        )
        yield (
            env,
            f"127.0.0.1:{port}",
            f"ray://127.0.0.1:{client_server_port}",
            f"http://127.0.0.1:{dashboard_port}",
        )
    finally:
        _cleanup_ray_start(env)


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray start, not compatible with client mode",
)
@pytest.mark.parametrize("driver_auth_mode", ["disabled", "token"])
def test_driver_on_auth_disabled_cluster(
    driver_auth_mode, disabled_head_with_token_file
):
    """A driver with a token file works on a cluster with RAY_AUTH_MODE=disabled
    only if it sets RAY_AUTH_MODE=disabled too."""
    env, address, _, _ = disabled_head_with_token_file
    driver = (
        "import ray\n"
        f"ray.init(address='{address}')\n"
        "print(ray.get(ray.remote(lambda: 'driver ok').remote()))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", driver],
        env={**env, "RAY_AUTH_MODE": driver_auth_mode},
        capture_output=True,
        text=True,
        # The rejection must not wait out the ~20s of GCS connect retries.
        timeout=15 if driver_auth_mode == "token" else 60,
    )
    output = result.stdout + result.stderr

    if driver_auth_mode == "disabled":
        assert result.returncode == 0, output
        assert "driver ok" in output
    else:
        assert result.returncode != 0, output
        _assert_token_auth_error(output)


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray start, not compatible with client mode",
)
def test_ray_start_address_in_token_mode_on_auth_disabled_cluster(
    disabled_head_with_token_file,
):
    """ray start --address in token mode fails fast with the token auth error against a
    head with RAY_AUTH_MODE=disabled."""
    env, address, _, _ = disabled_head_with_token_file
    result = subprocess.run(
        ["ray", "start", f"--address={address}", "--num-cpus=1"],
        env={**env, "RAY_AUTH_MODE": "token"},
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode != 0
    _assert_token_auth_error(result.stdout + result.stderr)


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray start, not compatible with client mode",
)
@pytest.mark.parametrize("client_auth_mode", ["disabled", "token"])
def test_ray_client_on_auth_disabled_cluster(
    client_auth_mode, disabled_head_with_token_file
):
    """A ray:// client with a token file works on a cluster with
    RAY_AUTH_MODE=disabled only if it sets RAY_AUTH_MODE=disabled too."""
    env, _, client_address, _ = disabled_head_with_token_file
    driver = (
        "import ray\n"
        f"ray.init('{client_address}')\n"
        "print(ray.get(ray.remote(lambda: 'client ok').remote()))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", driver],
        env={**env, "RAY_AUTH_MODE": client_auth_mode},
        capture_output=True,
        text=True,
        timeout=15 if client_auth_mode == "token" else 60,
    )
    output = result.stdout + result.stderr

    if client_auth_mode == "disabled":
        assert result.returncode == 0, output
        assert "client ok" in output
    else:
        assert result.returncode != 0, output
        _assert_token_auth_error(output)


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray start, not compatible with client mode",
)
def test_dashboard_agent_grpc_on_auth_disabled_cluster(disabled_head_with_token_file):
    """The dashboard agent's gRPC server rejects a request that carries a token on a
    cluster with RAY_AUTH_MODE=disabled, and accepts one that doesn't."""
    import json

    import grpc

    from ray.core.generated import reporter_pb2, reporter_pb2_grpc
    from ray.dashboard import consts as dashboard_consts

    _, address, _, _ = disabled_head_with_token_file
    set_auth_mode("disabled")
    reset_auth_token_state()
    ray.init(address=address)
    agent_key = (
        f"{dashboard_consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}"
        f"{ray.get_runtime_context().get_node_id()}"
    )

    def get_agent_address():
        return ray.experimental.internal_kv._internal_kv_get(
            agent_key, namespace=ray._private.ray_constants.KV_NAMESPACE_DASHBOARD
        )

    wait_for_condition(lambda: get_agent_address() is not None, timeout=30)
    ip, _, grpc_port = json.loads(get_agent_address())
    stub = reporter_pb2_grpc.ReporterServiceStub(
        grpc.insecure_channel(f"{ip}:{grpc_port}")
    )

    stub.HealthCheck(reporter_pb2.HealthCheckRequest(), timeout=5)
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.HealthCheck(
            reporter_pb2.HealthCheckRequest(),
            metadata=(("authorization", f"Bearer {'c' * 64}"),),
            timeout=5,
        )
    assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED
    _assert_token_auth_error(exc_info.value.details())


@pytest.mark.parametrize(
    "headers, expected_status",
    [
        ([("Authorization", "Bearer t")], 401),
        ([("Authorization", "bearer t")], 401),
        ([("Authorization", "Basic dXNlcjpwYXNz")], 401),
        ([("X-Ray-Authorization", "Bearer t")], 401),
        ([("Authorization", ""), ("Authorization", "Bearer t")], 401),
        ([("Authorization", "")], 200),
        ([("X-Ray-Authorization", "")], 200),
        ([("Cookie", "ray-authentication-token=t")], 200),
        ([], 200),
    ],
)
def test_token_auth_middleware_with_auth_disabled(
    headers: List[Tuple[str, str]],
    expected_status: int,
):
    """With auth disabled, the middleware rejects a request with any auth header,
    and ignores the token cookie."""
    import asyncio
    from unittest import mock

    import aiohttp
    from aiohttp.test_utils import make_mocked_request
    from multidict import CIMultiDict

    from ray._private.authentication import authentication_utils
    from ray._private.authentication.http_token_authentication import (
        get_token_auth_middleware,
    )

    middleware = get_token_auth_middleware(aiohttp)

    async def handler(request):
        return aiohttp.web.Response(status=200)

    request = make_mocked_request("GET", "/api/version", headers=CIMultiDict(headers))
    with mock.patch.object(
        authentication_utils, "is_token_auth_enabled", return_value=False
    ):
        response = asyncio.run(middleware(request, handler))

    assert response.status == expected_status
    if expected_status == 401:
        _assert_token_auth_error(response.text)


_HTTP_CLIENT_CALLS = {
    "jobs": "from ray.job_submission import JobSubmissionClient\n"
    "JobSubmissionClient('{dashboard_url}').list_jobs()\n",
    "state": "from ray.util.state import list_nodes\n"
    "list_nodes(address='{dashboard_url}')\n",
    "serve": "from ray.dashboard.modules.serve.sdk import ServeSubmissionClient\n"
    "ServeSubmissionClient('{dashboard_url}')\n",
}


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray start, not compatible with client mode",
)
def test_dashboard_on_auth_disabled_cluster(disabled_head_with_token_file):
    """A dashboard with RAY_AUTH_MODE=disabled rejects requests that carry an auth
    header, so the jobs, state and Serve clients with a token file work only if they
    set RAY_AUTH_MODE=disabled too."""
    import requests

    from ray._private.authentication.authentication_constants import (
        AUTHENTICATION_TOKEN_COOKIE_NAME,
    )

    env, _, _, dashboard_url = disabled_head_with_token_file
    wait_for_condition(
        lambda: requests.get(f"{dashboard_url}/api/version", timeout=5).ok,
        timeout=30,
    )
    token = "c" * 64
    for path, headers, cookies, expect_mismatch in [
        ("/api/version", {"Authorization": f"Bearer {token}"}, None, True),
        ("/api/version", {"Authorization": f"bearer {token}"}, None, True),
        ("/api/version", {"X-Ray-Authorization": f"Bearer {token}"}, None, True),
        ("/api/authentication_mode", {"Authorization": f"Bearer {token}"}, None, True),
        ("/api/version", None, {AUTHENTICATION_TOKEN_COOKIE_NAME: token}, False),
        ("/api/version", {"Authorization": "Basic dXNlcjpwYXNz"}, None, True),
        ("/api/version", {"X-Ray-Authorization": ""}, None, False),
    ]:
        response = requests.get(
            f"{dashboard_url}{path}", headers=headers, cookies=cookies, timeout=5
        )
        if expect_mismatch:
            assert response.status_code == 401, (path, headers, response.text)
            _assert_token_auth_error(response.text)
        else:
            assert response.status_code == 200, (path, headers, cookies, response.text)
    for client_name, client_call in _HTTP_CLIENT_CALLS.items():
        for client_auth_mode in ["disabled", "token"]:
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    client_call.format(dashboard_url=dashboard_url)
                    + "print('client ok')\n",
                ],
                env={**env, "RAY_AUTH_MODE": client_auth_mode},
                capture_output=True,
                text=True,
                timeout=60,
            )
            output = f"{client_name}, RAY_AUTH_MODE={client_auth_mode}:\n" + (
                result.stdout + result.stderr
            )

            if client_auth_mode == "disabled":
                assert result.returncode == 0, output
                assert "client ok" in output
            else:
                assert result.returncode != 0, output
                assert "AuthenticationError" in output, output
                _assert_token_auth_error(output)


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Uses subprocess ray start, not compatible with client mode",
)
@pytest.mark.parametrize(
    "entry_point",
    ["ray start", "ray start --system-config", "ray.init", "Cluster", "gcs_server"],
)
def test_k8s_token_auth_with_auth_disabled_is_config_error(entry_point, tmp_path):
    """RAY_AUTH_MODE=disabled with ENABLE_K8S_TOKEN_AUTH, from the environment or the
    system config, fails before starting any process, with an error naming
    RAY_AUTH_MODE=token. A GCS started directly exits with the same error."""
    from ray._private.authentication.authentication_constants import (
        K8S_TOKEN_AUTH_REQUIRES_TOKEN_MODE_ERROR_MESSAGE,
    )
    from ray._private.services import GCS_SERVER_EXECUTABLE

    temp_dir = tmp_path / "ray"
    command = {
        "ray start": ["ray", "start", "--head", f"--temp-dir={temp_dir}"],
        "ray start --system-config": [
            "ray",
            "start",
            "--head",
            f"--temp-dir={temp_dir}",
            '--system-config={"ENABLE_K8S_TOKEN_AUTH": true}',
        ],
        "ray.init": [
            sys.executable,
            "-c",
            f"import ray; ray.init(_temp_dir='{temp_dir}')",
        ],
        "Cluster": [
            sys.executable,
            "-c",
            "from ray.cluster_utils import Cluster; "
            "Cluster(initialize_head=True, enable_token_auth=False, "
            f"head_node_args={{'temp_dir': '{temp_dir}'}})",
        ],
        "gcs_server": [
            GCS_SERVER_EXECUTABLE,
            "--gcs_server_port=0",
            f"--log_dir={tmp_path}",
            "--session_name=k8s_check",
        ],
    }[entry_point]
    result = subprocess.run(
        command,
        env={
            **os.environ,
            "RAY_AUTH_MODE": "disabled",
            **(
                {}
                if entry_point == "ray start --system-config"
                else {"RAY_ENABLE_K8S_TOKEN_AUTH": "1"}
            ),
        },
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode != 0
    assert K8S_TOKEN_AUTH_REQUIRES_TOKEN_MODE_ERROR_MESSAGE in (
        result.stdout + result.stderr
    )
    if entry_point != "gcs_server":
        assert not list(temp_dir.glob("session_*"))


def test_kill_raylet_on_auth_disabled_cluster_with_token_file():
    """kill_raylet still kills a raylet on a cluster with RAY_AUTH_MODE=disabled when
    the test process has a token file, which the raylet would reject."""
    from ray._private.authentication_test_utils import set_default_auth_token
    from ray._private.test_utils import kill_raylet

    set_default_auth_token("e" * 64)
    set_auth_mode("disabled")
    reset_auth_token_state()
    cluster = Cluster(
        initialize_head=True, enable_token_auth=False, head_node_args={"num_cpus": 1}
    )
    try:
        worker = cluster.add_node(num_cpus=1)
        ray.init(address=cluster.address)
        kill_raylet(
            next(n for n in ray.nodes() if n["NodeID"] == worker.node_id),
            graceful=False,
        )
        wait_for_condition(
            lambda: not next(
                n["Alive"] for n in ray.nodes() if n["NodeID"] == worker.node_id
            ),
            timeout=60,
        )
    finally:
        ray.shutdown()
        cluster.shutdown()


def test_e2e_operations_with_token_auth(setup_cluster_with_token_auth):
    """Test that e2e operations work with token authentication enabled.

    This verifies that with token auth enabled:
    1. Tasks execute successfully
    2. Actors can be created and called
    3. State API works (list_nodes, list_actors, list_tasks)
    4. Job submission works
    """
    cluster_info = setup_cluster_with_token_auth

    # Test 1: Submit a simple task
    @ray.remote
    def simple_task(x):
        return x + 1

    result = ray.get(simple_task.remote(41))
    assert result == 42, f"Task should return 42, got {result}"

    # Test 2: Create and use an actor
    @ray.remote
    class SimpleActor:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

    actor = SimpleActor.remote()
    result = ray.get(actor.increment.remote())
    assert result == 1, f"Actor method should return 1, got {result}"

    # Test 3: State API operations (uses HTTP with auth headers)
    from ray.util.state import list_actors, list_nodes, list_tasks

    # List nodes - should include at least the head node
    wait_for_condition(lambda: len(list_nodes()) >= 1)

    # List actors - should include our SimpleActor
    def check_actors():
        actors = list_actors()
        if len(actors) < 1:
            return False
        return "SimpleActor" in actors[0].class_name

    wait_for_condition(check_actors)

    # List tasks - should include completed tasks
    wait_for_condition(lambda: len(list_tasks()) >= 1)

    # Test 4: Submit a job and wait for completion
    from ray.job_submission import JobSubmissionClient

    # Create job submission client (uses HTTP with auth headers)
    client = JobSubmissionClient(address=cluster_info["dashboard_url"])

    # Submit a simple job
    job_id = client.submit_job(
        entrypoint="echo 'Hello from job'",
    )

    # Wait for job to complete
    def job_finished():
        status = client.get_job_status(job_id)
        return status in ["SUCCEEDED", "FAILED", "STOPPED"]

    wait_for_condition(job_finished, timeout=30)

    final_status = client.get_job_status(job_id)
    assert (
        final_status == "SUCCEEDED"
    ), f"Job should succeed, got status: {final_status}"


def test_logs_api_with_token_auth(setup_cluster_with_token_auth):
    """Test that log APIs work with token authentication enabled."""
    from ray.util.state import get_log, list_logs

    # Get node ID for log queries
    node_id = ray.nodes()[0]["NodeID"]

    # Test list_logs() with valid auth
    logs = list_logs(node_id=node_id)
    assert isinstance(logs, dict), f"list_logs should return a dict, got {type(logs)}"

    # Test get_log() with valid auth (fetch raylet.out which will always exist)
    chunks_received = 0
    for chunk in get_log(filename="raylet.out", node_id=node_id, tail=10):
        assert isinstance(chunk, str), f"get_log chunk should be str, got {type(chunk)}"
        chunks_received += 1
        break

    assert chunks_received > 0, "Should have received at least one log chunk"


@pytest.fixture
def token_cluster_with_client_server(cleanup_auth_token_env):
    """A token-auth head node that also serves ray:// clients."""
    test_token = secrets.token_hex(32)
    set_auth_mode("token")
    set_env_auth_token(test_token)
    reset_auth_token_state()

    client_server_port = find_free_port()
    cluster = Cluster()
    cluster.add_node(
        dashboard_agent_listen_port=find_free_port(),
        ray_client_server_port=client_server_port,
    )
    try:
        yield {
            "gcs_address": cluster.address,
            "client_address": f"ray://127.0.0.1:{client_server_port}",
        }
    finally:
        cluster.shutdown()


def _connect_command(entry_point: str, cluster_info: dict) -> list:
    if entry_point == "ray_start":
        return ["ray", "start", f"--address={cluster_info['gcs_address']}"]
    address = cluster_info[
        "client_address" if entry_point == "ray_client" else "gcs_address"
    ]
    return [sys.executable, "-c", f"import ray; ray.init(address={address!r})"]


def _assert_connect_fails_naming_fix(
    entry_point: str, cluster_info: dict, env_overrides: dict
):
    """Run a connecting entry point in a subprocess and check how it fails."""
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in ("RAY_AUTH_TOKEN", "RAY_AUTH_TOKEN_PATH")
    }
    # Replace the pre_envs fixture's cap of 2 GCS connect retries with 60, so code
    # that retries a rejection takes about 60s, far above the bound below.
    env["RAY_NUM_REDIS_GET_RETRIES"] = "60"
    env.update(env_overrides)
    start = time.monotonic()
    result = subprocess.run(
        _connect_command(entry_point, cluster_info),
        env={"RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER": "1", **env},
        capture_output=True,
        text=True,
        timeout=120,
    )
    elapsed_s = time.monotonic() - start
    output = result.stdout + result.stderr

    assert result.returncode != 0, f"{entry_point} should have failed: {output}"
    assert "ray get-auth-token" in output, output
    assert "RAY_AUTH_MODE=disabled" in output, output
    assert "--generate" not in output, output
    # Retrying takes about 60s for GCS and 31s for ray://; failing fast takes ~1s.
    assert elapsed_s < 20, f"{entry_point} took {elapsed_s:.0f}s to fail: {output}"


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Runs connecting entry points in subprocesses",
)
@pytest.mark.parametrize(
    "env_overrides",
    [
        {"RAY_AUTH_MODE": "token"},
        {"RAY_AUTH_MODE": "disabled"},
        {"RAY_AUTH_MODE": "token", "RAY_AUTH_TOKEN": "b" * 64},
    ],
    ids=["no_token", "auth_disabled", "wrong_token"],
)
def test_rejected_connect_fails_fast_naming_fix(
    env_overrides, token_cluster_with_client_server
):
    for entry_point in ["ray_start", "ray_init", "ray_client"]:
        _assert_connect_fails_naming_fix(
            entry_point, token_cluster_with_client_server, env_overrides
        )


@pytest.mark.skipif(
    client_test_enabled(),
    reason="Connects its own ray:// client",
)
def test_ray_client_checks_token_before_connecting():
    set_auth_mode("token")
    reset_auth_token_state()

    # Nothing listens on the port, so only a check made before connecting
    # can fail with AuthenticationError instead of a connection timeout.
    start = time.monotonic()
    try:
        with pytest.raises(AuthenticationError, match="ray get-auth-token"):
            ray.init(f"ray://127.0.0.1:{find_free_port()}")
    finally:
        # A failed ray:// connect leaves client mode on; later tests in this
        # process would then route ray.init through the client.
        ray.util.disconnect()
    assert time.monotonic() - start < 5


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
