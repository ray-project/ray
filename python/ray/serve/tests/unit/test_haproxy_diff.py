"""Unit tests for the incremental backend-update diff in HAProxyApi.

The diff helper decides whether a backend-config change can be applied via
HAProxy's runtime API (`add server` / `del server`) instead of regenerating
the full config and triggering a graceful reload. At Ray Serve scales with
many replicas under autoscaling churn, reload-on-every-change blocks the
proxy actor's event loop long enough to fail the controller's health check,
which kills and recreates the actor — leading to cluster-wide proxy
flapping. The runtime-API path keeps the actor responsive for the common
case (replica added/removed, nothing else).

These tests cover the diff classification in isolation. Whether a given
diff actually applies cleanly is HAProxy's responsibility at runtime;
HAProxyApi.try_apply_servers_dynamically falls back to a reload on any
socket-command error.
"""

from unittest.mock import AsyncMock, patch

import pytest

from ray.serve._private.haproxy import (
    BackendConfig,
    HAProxyApi,
    HAProxyConfig,
    ServerConfig,
    _compute_backend_diff,
)
from ray.serve.config import HTTPOptions


def _backend(
    name: str,
    servers: list,
    *,
    path_prefix: str = "/x",
    fallback=None,
    **overrides,
) -> BackendConfig:
    """Make a BackendConfig with sensible defaults; override fields per test."""
    return BackendConfig(
        name=name,
        path_prefix=path_prefix,
        app_name=name,
        servers=servers,
        fallback_server=fallback,
        **overrides,
    )


def _server(name: str, host: str = "10.0.0.1", port: int = 8000) -> ServerConfig:
    return ServerConfig(name=name, host=host, port=port)


class TestComputeBackendDiff:
    """The diff is the safety boundary between fast-path and reload."""

    def test_identical_configs_no_changes(self):
        """No diff → no work, no reload."""
        configs = {"b1": _backend("b1", [_server("s1"), _server("s2")])}
        diff = _compute_backend_diff(configs, configs)
        assert not diff.is_structural
        assert diff.servers_to_add == {}
        assert diff.servers_to_remove == {}

    def test_added_server(self):
        """A new server within an existing backend is the canonical fast-path case."""
        old = {"b1": _backend("b1", [_server("s1")])}
        new = {"b1": _backend("b1", [_server("s1"), _server("s2", port=8001)])}
        diff = _compute_backend_diff(old, new)
        assert not diff.is_structural
        assert "b1" in diff.servers_to_add
        assert [s.name for s in diff.servers_to_add["b1"]] == ["s2"]
        assert diff.servers_to_remove == {}

    def test_removed_server(self):
        """A removed server within an existing backend is also fast-path."""
        old = {"b1": _backend("b1", [_server("s1"), _server("s2")])}
        new = {"b1": _backend("b1", [_server("s1")])}
        diff = _compute_backend_diff(old, new)
        assert not diff.is_structural
        assert diff.servers_to_remove == {"b1": ["s2"]}
        assert diff.servers_to_add == {}

    def test_server_address_change_is_remove_plus_add(self):
        """Same name, different host/port → remove old, add new.

        HAProxy's runtime API doesn't have a clean in-place address-change
        primitive that works the same way across all 2.x releases, so we
        treat this as remove + re-add. Conservative and predictable.
        """
        old = {"b1": _backend("b1", [_server("s1", host="10.0.0.1", port=8000)])}
        new = {"b1": _backend("b1", [_server("s1", host="10.0.0.2", port=8000)])}
        diff = _compute_backend_diff(old, new)
        assert not diff.is_structural
        assert diff.servers_to_remove == {"b1": ["s1"]}
        assert [s.host for s in diff.servers_to_add["b1"]] == ["10.0.0.2"]

    def test_added_backend_is_structural(self):
        """A new backend requires a new ACL → reload."""
        old = {"b1": _backend("b1", [_server("s1")])}
        new = {
            "b1": _backend("b1", [_server("s1")]),
            "b2": _backend("b2", [_server("s3")]),
        }
        diff = _compute_backend_diff(old, new)
        assert diff.is_structural

    def test_removed_backend_is_structural(self):
        """A removed backend → reload (frontend ACLs must be regenerated)."""
        old = {
            "b1": _backend("b1", [_server("s1")]),
            "b2": _backend("b2", [_server("s3")]),
        }
        new = {"b1": _backend("b1", [_server("s1")])}
        diff = _compute_backend_diff(old, new)
        assert diff.is_structural

    def test_path_prefix_change_is_structural(self):
        """Path-prefix changes affect the frontend ACLs → reload."""
        old = {"b1": _backend("b1", [_server("s1")], path_prefix="/x")}
        new = {"b1": _backend("b1", [_server("s1")], path_prefix="/y")}
        diff = _compute_backend_diff(old, new)
        assert diff.is_structural

    def test_fallback_server_change_is_structural(self):
        """Backup-server changes are rendered into the backend block → reload."""
        old = {
            "b1": _backend(
                "b1",
                [_server("s1")],
                fallback=_server("fb", host="127.0.0.1", port=8500),
            )
        }
        new = {
            "b1": _backend(
                "b1",
                [_server("s1")],
                fallback=_server("fb", host="127.0.0.1", port=8501),
            )
        }
        diff = _compute_backend_diff(old, new)
        assert diff.is_structural

    @pytest.mark.parametrize(
        "field,old_val,new_val",
        [
            ("timeout_connect_s", None, 5),
            ("timeout_server_s", 30, 60),
            ("timeout_http_keep_alive_s", None, 60),
            ("timeout_tunnel_s", None, 60),
            ("health_check_fall", None, 3),
            ("health_check_rise", None, 2),
            ("health_check_inter", None, "1s"),
            ("health_check_path", None, "/-/healthz"),
        ],
    )
    def test_backend_level_field_change_is_structural(self, field, old_val, new_val):
        """Any non-server backend field change → reload.

        These all render into the HAProxy backend block (timeouts, health-
        check directives, etc.), so they require regenerating the config.
        """
        old = {"b1": _backend("b1", [_server("s1")], **{field: old_val})}
        new = {"b1": _backend("b1", [_server("s1")], **{field: new_val})}
        diff = _compute_backend_diff(old, new)
        assert diff.is_structural, f"change to {field} should be structural"

    def test_concurrent_adds_and_removes_in_one_diff(self):
        """A real-world case: scale-up adds N replicas while scale-down removes M."""
        old = {"b1": _backend("b1", [_server("s1"), _server("s2"), _server("s3")])}
        new = {"b1": _backend("b1", [_server("s2"), _server("s4"), _server("s5")])}
        diff = _compute_backend_diff(old, new)
        assert not diff.is_structural
        assert sorted(diff.servers_to_remove["b1"]) == ["s1", "s3"]
        assert sorted(s.name for s in diff.servers_to_add["b1"]) == ["s4", "s5"]

    def test_changes_across_multiple_backends(self):
        """Mixed updates across backends still take the fast path if all are server-only."""
        old = {
            "b1": _backend("b1", [_server("s1")]),
            "b2": _backend("b2", [_server("s2")], path_prefix="/y"),
        }
        new = {
            "b1": _backend("b1", [_server("s1"), _server("s1b")]),
            "b2": _backend("b2", [], path_prefix="/y"),
        }
        diff = _compute_backend_diff(old, new)
        assert not diff.is_structural
        assert [s.name for s in diff.servers_to_add["b1"]] == ["s1b"]
        assert diff.servers_to_remove["b2"] == ["s2"]


class TestTryApplyServersDynamically:
    """Behavior of HAProxyApi.try_apply_servers_dynamically.

    The contract: returns True only when the change was applied without a
    reload. Otherwise the caller (HAProxyManager._apply_backend_update)
    will perform a full reload.
    """

    def _make_api(self, backend_configs=None):
        cfg = HAProxyConfig(
            socket_path="/tmp/test.sock",
            http_options=HTTPOptions(host="127.0.0.1", port=8000),
        )
        return HAProxyApi(cfg=cfg, backend_configs=backend_configs or {})

    @pytest.mark.asyncio
    async def test_first_time_setup_falls_back_to_reload(self):
        """Initial broadcast (no prior state) must reload — there's no diff base."""
        api = self._make_api(backend_configs={})
        new_configs = {"b1": _backend("b1", [_server("s1")])}

        applied = await api.try_apply_servers_dynamically(new_configs)
        assert applied is False
        # Internal state should have been updated so the subsequent reload
        # uses the new configs.
        assert api.backend_configs == new_configs

    @pytest.mark.asyncio
    async def test_haproxy_not_running_falls_back_to_reload(self):
        """If HAProxy isn't up yet, the runtime API isn't available."""
        api = self._make_api(backend_configs={"b1": _backend("b1", [_server("s1")])})
        new_configs = {"b1": _backend("b1", [_server("s1"), _server("s2")])}

        with patch.object(api, "is_running", AsyncMock(return_value=False)):
            applied = await api.try_apply_servers_dynamically(new_configs)
        assert applied is False
        assert api.backend_configs == new_configs

    @pytest.mark.asyncio
    async def test_structural_change_falls_back_to_reload(self):
        """New backend → caller must reload; we still update internal state."""
        api = self._make_api(backend_configs={"b1": _backend("b1", [_server("s1")])})
        new_configs = {
            "b1": _backend("b1", [_server("s1")]),
            "b2": _backend("b2", [_server("s3")]),
        }

        with patch.object(api, "is_running", AsyncMock(return_value=True)):
            applied = await api.try_apply_servers_dynamically(new_configs)
        assert applied is False
        assert api.backend_configs == new_configs

    @pytest.mark.asyncio
    async def test_server_add_uses_runtime_api(self):
        """Pure add → `add server` + `enable server` over the socket, no reload."""
        api = self._make_api(backend_configs={"b1": _backend("b1", [_server("s1")])})
        new_configs = {"b1": _backend("b1", [_server("s1"), _server("s2", port=8001)])}

        socket_calls = []

        async def fake_send(cmd):
            socket_calls.append(cmd)
            return ""

        with patch.object(
            api, "is_running", AsyncMock(return_value=True)
        ), patch.object(
            api, "_send_socket_command", side_effect=fake_send
        ), patch.object(
            api, "_generate_config_file_internal"
        ):
            applied = await api.try_apply_servers_dynamically(new_configs)

        assert applied is True
        # The exact commands matter — operators read these in HAProxy's logs.
        assert any("add server b1/s2 10.0.0.1:8001" in c for c in socket_calls)
        assert any("enable server b1/s2" in c for c in socket_calls)
        assert not any("del server" in c for c in socket_calls)

    @pytest.mark.asyncio
    async def test_server_remove_uses_runtime_api(self):
        """Pure remove → `disable server` + `del server` over the socket, no reload."""
        api = self._make_api(
            backend_configs={"b1": _backend("b1", [_server("s1"), _server("s2")])}
        )
        new_configs = {"b1": _backend("b1", [_server("s1")])}

        socket_calls = []

        async def fake_send(cmd):
            socket_calls.append(cmd)
            return ""

        with patch.object(
            api, "is_running", AsyncMock(return_value=True)
        ), patch.object(
            api, "_send_socket_command", side_effect=fake_send
        ), patch.object(
            api, "_generate_config_file_internal"
        ):
            applied = await api.try_apply_servers_dynamically(new_configs)

        assert applied is True
        # `disable` must precede `del` to handle in-flight connections cleanly.
        disable_idx = next(
            i for i, c in enumerate(socket_calls) if "disable server b1/s2" in c
        )
        del_idx = next(i for i, c in enumerate(socket_calls) if "del server b1/s2" in c)
        assert disable_idx < del_idx

    @pytest.mark.asyncio
    async def test_socket_failure_falls_back_to_reload(self):
        """If any runtime command raises, bail and let the caller reload.

        HAProxy can fail at the transport level (socket closed, connection
        refused, timeout). The caller's reload converges live state back
        to self.backend_configs.
        """
        api = self._make_api(backend_configs={"b1": _backend("b1", [_server("s1")])})
        new_configs = {"b1": _backend("b1", [_server("s1"), _server("s2")])}

        with patch.object(
            api, "is_running", AsyncMock(return_value=True)
        ), patch.object(
            api,
            "_send_socket_command",
            AsyncMock(side_effect=RuntimeError("socket closed")),
        ):
            applied = await api.try_apply_servers_dynamically(new_configs)
        assert applied is False
        # Internal state still updated so the fallback reload reflects truth.
        assert api.backend_configs == new_configs

    @pytest.mark.asyncio
    async def test_socket_returns_error_string_falls_back_to_reload(self):
        """HAProxy can reject commands by returning an error string (not raising).

        The runtime API conveys most command-level failures as response
        text (e.g. ``[ALERT] (123) : 'add server' : Backend X not found.``)
        rather than transport-level exceptions. We must treat those
        responses as failures or we'd silently skip a needed reload while
        live HAProxy state diverges from our internal config.
        """
        api = self._make_api(backend_configs={"b1": _backend("b1", [_server("s1")])})
        new_configs = {"b1": _backend("b1", [_server("s1"), _server("s2")])}

        async def fake_send(cmd):
            if cmd.startswith("add server"):
                return "[ALERT] (123) : 'add server' : Backend b1 not found.\n"
            return ""

        with patch.object(
            api, "is_running", AsyncMock(return_value=True)
        ), patch.object(api, "_send_socket_command", side_effect=fake_send):
            applied = await api.try_apply_servers_dynamically(new_configs)
        assert applied is False
        assert api.backend_configs == new_configs

    @pytest.mark.asyncio
    async def test_send_runtime_command_raises_on_error_string(self):
        """Direct test of the response validator that backs the apply path."""
        api = self._make_api()

        async def fake_send(cmd):
            return "[ALERT] command rejected\n"

        with patch.object(api, "_send_socket_command", side_effect=fake_send):
            with pytest.raises(RuntimeError, match="HAProxy rejected"):
                await api._send_runtime_command("add server foo/bar 1.2.3.4:80")

    @pytest.mark.asyncio
    async def test_send_runtime_command_passes_empty_response(self):
        """Successful HAProxy runtime commands return empty/whitespace."""
        api = self._make_api()

        async def fake_send(cmd):
            return ""

        with patch.object(api, "_send_socket_command", side_effect=fake_send):
            result = await api._send_runtime_command("disable server foo/bar")
        assert result == ""


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
