"""Unit tests for DNS-AID integration with Ray Serve.

All tests are self-contained: no Ray cluster is required.  DNS network I/O
is mocked via ``unittest.mock.patch`` so the tests run offline.
"""

import asyncio
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Stub out the optional `dns_aid` package so tests run without it installed
# ---------------------------------------------------------------------------

_dns_aid_stub = types.ModuleType("dns_aid")
_dns_aid_stub.publish = MagicMock(return_value=None)
_dns_aid_stub.delete = MagicMock(return_value=True)

_backends_stub = types.ModuleType("dns_aid.backends")


class _MockBackend:
    def __init__(self, **kwargs):
        pass


_backends_stub.create_backend = MagicMock(return_value=_MockBackend())

sys.modules.setdefault("dns_aid", _dns_aid_stub)
sys.modules.setdefault("dns_aid.backends", _backends_stub)

# Now import the modules under test
from ray.serve.dns_aid import DnsAidConfig, DnsAidManager  # noqa: E402


class TestDnsAidConfig(unittest.TestCase):
    def test_defaults(self):
        cfg = DnsAidConfig()
        self.assertFalse(cfg.enabled)
        self.assertIsNone(cfg.zone)
        self.assertIsNone(cfg.server)
        self.assertEqual(cfg.dns_port, 53)
        self.assertEqual(cfg.ttl, 45)
        self.assertEqual(cfg.backend, "ddns")

    def test_env_var_zone(self):
        with patch.dict(os.environ, {"DNS_AID_ZONE": "test.example.com"}):
            cfg = DnsAidConfig()
        self.assertEqual(cfg.zone, "test.example.com")

    def test_env_var_port(self):
        with patch.dict(os.environ, {"DNS_AID_PORT": "5353"}):
            cfg = DnsAidConfig()
        self.assertEqual(cfg.dns_port, 5353)

    def test_env_var_port_invalid(self):
        """Invalid DNS_AID_PORT should log a warning and keep the default."""
        with patch.dict(os.environ, {"DNS_AID_PORT": "notanint"}):
            cfg = DnsAidConfig()
        self.assertEqual(cfg.dns_port, 53)

    def test_env_var_backend(self):
        with patch.dict(os.environ, {"DNS_AID_BACKEND": "route53"}):
            cfg = DnsAidConfig()
        self.assertEqual(cfg.backend, "route53")

    def test_explicit_port_not_overridden_by_env(self):
        """Explicit constructor value must win over env var."""
        with patch.dict(os.environ, {"DNS_AID_PORT": "8053"}):
            cfg = DnsAidConfig(dns_port=5353)
        self.assertEqual(cfg.dns_port, 5353)

    def test_explicit_backend_not_overridden_by_env(self):
        """Explicit constructor value must win over env var."""
        with patch.dict(os.environ, {"DNS_AID_BACKEND": "route53"}):
            cfg = DnsAidConfig(backend="cloudflare")
        self.assertEqual(cfg.backend, "cloudflare")


class TestDnsAidManagerEnabled(unittest.TestCase):
    def setUp(self):
        _dns_aid_stub.publish.reset_mock()
        _dns_aid_stub.delete.reset_mock()
        _backends_stub.create_backend.reset_mock()
        _backends_stub.create_backend.return_value = _MockBackend()

    def _make_manager(self, enabled=True, zone="agents.test"):
        cfg = DnsAidConfig(enabled=enabled, zone=zone)
        return DnsAidManager(cfg)

    # ------------------------------------------------------------------
    # _is_enabled / is_enabled
    # ------------------------------------------------------------------

    def test_disabled_by_config(self):
        mgr = self._make_manager(enabled=False)
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(mgr.is_enabled())

    def test_enabled_by_config(self):
        mgr = self._make_manager(enabled=True)
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(mgr.is_enabled())

    def test_enabled_by_env_var(self):
        mgr = self._make_manager(enabled=False)
        with patch.dict(os.environ, {"DNS_AID_ENABLED": "1"}):
            self.assertTrue(mgr.is_enabled())

    def test_disabled_by_env_var_overrides_config(self):
        mgr = self._make_manager(enabled=True)
        with patch.dict(os.environ, {"DNS_AID_ENABLED": "0"}):
            self.assertFalse(mgr.is_enabled())

    def test_enabled_by_env_var_true_string(self):
        mgr = self._make_manager(enabled=False)
        for val in ("true", "yes", "TRUE", "YES"):
            with patch.dict(os.environ, {"DNS_AID_ENABLED": val}):
                self.assertTrue(
                    mgr.is_enabled(),
                    msg=f"expected enabled for {val!r}",
                )

    def test_env_var_overrides_set_enabled(self):
        """DNS_AID_ENABLED env var is an operator-level override
        that takes priority over set_enabled()."""
        mgr = self._make_manager(enabled=True)
        mgr.set_enabled(False)
        # Without env var, set_enabled(False) disables
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(mgr.is_enabled())
        # With env var=1, set_enabled(False) is overridden
        mgr.set_enabled(False)
        with patch.dict(os.environ, {"DNS_AID_ENABLED": "1"}):
            self.assertTrue(mgr.is_enabled())

    # ------------------------------------------------------------------
    # register
    # ------------------------------------------------------------------

    def test_register_on_healthy(self):
        """register() should call dns_aid.publish with correct args."""
        mgr = self._make_manager()

        async def _run_tasks():
            mgr.register(
                "MyDep", "/my-dep", 3, "10.0.0.1", 8000, "my_app"
            )
            await asyncio.sleep(0)

        asyncio.run(_run_tasks())

        _dns_aid_stub.publish.assert_called_once()

    def test_register_no_op_when_disabled(self):
        """Disabled manager should never call publish."""
        mgr = self._make_manager(enabled=False)

        async def _run_tasks():
            mgr.register(
                "MyDep", "/my-dep", 3, "10.0.0.1", 8000, "my_app"
            )
            await asyncio.sleep(0)

        asyncio.run(_run_tasks())
        _dns_aid_stub.publish.assert_not_called()

    def test_register_no_op_when_zone_missing(self):
        """Without a zone, registration should skip gracefully."""
        cfg = DnsAidConfig(enabled=True)  # zone=None
        mgr = DnsAidManager(cfg)

        async def _run_tasks():
            await mgr._register_async("Dep", "/dep", 1, "10.0.0.1", 8000, "app")

        asyncio.run(_run_tasks())
        _dns_aid_stub.publish.assert_not_called()

    def test_controller_deduplication_across_loop_iterations(self):
        """Simulates the controller pattern: register() is only called for
        deployments NOT already in _dns_aid_running.  A deployment that was
        registered in a previous iteration must not generate a second call.
        """
        mgr = self._make_manager()

        async def _first_iteration():
            mgr.register(
                "MyDep", "/my-dep", 2, "10.0.0.1", 8000, "my_app"
            )
            await asyncio.sleep(0)

        async def _second_iteration():
            await asyncio.sleep(0)

        asyncio.run(_first_iteration())
        asyncio.run(_second_iteration())
        _dns_aid_stub.publish.assert_called_once()

    def test_different_apps_same_dep_name_get_distinct_names(self):
        """Two applications sharing a deployment name must produce
        different agent names."""
        mgr = self._make_manager()
        name1 = mgr._agent_name("Ingress", "app1")
        name2 = mgr._agent_name("Ingress", "app2")
        self.assertNotEqual(name1, name2)
        self.assertIn("app1", name1)
        self.assertIn("app2", name2)

    # ------------------------------------------------------------------
    # deregister
    # ------------------------------------------------------------------

    def test_deregister_on_delete(self):
        """deregister() should call dns_aid.delete."""
        mgr = self._make_manager()
        mgr._registered.add(("my_app", "MyDep"))

        async def _run_tasks():
            mgr.deregister("MyDep", "my_app")
            await asyncio.sleep(0)

        asyncio.run(_run_tasks())
        _dns_aid_stub.delete.assert_called_once()

    def test_deregister_no_op_when_disabled(self):
        mgr = self._make_manager(enabled=False)

        async def _run_tasks():
            mgr.deregister("MyDep", "my_app")
            await asyncio.sleep(0)

        asyncio.run(_run_tasks())
        _dns_aid_stub.delete.assert_not_called()

    def test_deregister_all_idempotent(self):
        """deregister_all() should only fire tasks on the first call."""
        mgr = self._make_manager()
        mgr._registered.add(("app", "Dep1"))
        mgr._registered.add(("app", "Dep2"))

        async def _run_tasks():
            mgr.deregister_all()
            mgr.deregister_all()  # second call should no-op
            await asyncio.sleep(0)

        asyncio.run(_run_tasks())
        self.assertEqual(_dns_aid_stub.delete.call_count, 2)

    def test_deregister_all_merges_known_keys(self):
        """deregister_all(known_keys=...) should cover in-flight
        registrations not yet in _registered."""
        mgr = self._make_manager()
        mgr._registered.add(("app", "Confirmed"))
        extra = {("app", "InFlight")}

        async def _run_tasks():
            mgr.deregister_all(known_keys=extra)
            await asyncio.sleep(0)

        asyncio.run(_run_tasks())
        # Should deregister both confirmed and in-flight
        self.assertEqual(_dns_aid_stub.delete.call_count, 2)

    def test_deregister_all_force_bypasses_enabled_check(self):
        """force=True should deregister even when disabled."""
        mgr = self._make_manager(enabled=False)
        mgr._registered.add(("app", "Dep"))

        async def _run_tasks():
            mgr.deregister_all(force=True)
            await asyncio.sleep(0)

        asyncio.run(_run_tasks())
        _dns_aid_stub.delete.assert_called_once()

    def test_reenable_resets_deregister_all_flag(self):
        """After disable+deregister_all, re-enabling must allow a
        subsequent deregister_all (e.g. on shutdown) to run."""
        mgr = self._make_manager()
        mgr._registered.add(("app", "Dep"))

        async def _run_tasks():
            # Simulate disable path
            mgr.deregister_all(force=True)
            await asyncio.sleep(0)

        asyncio.run(_run_tasks())
        self.assertTrue(mgr._deregister_all_called)

        # Re-enable should reset the flag
        mgr.set_enabled(True)
        self.assertFalse(mgr._deregister_all_called)

        # Now a second deregister_all should work
        mgr._registered.add(("app", "Dep2"))

        async def _shutdown():
            mgr.deregister_all()
            await asyncio.sleep(0)

        asyncio.run(_shutdown())
        # 1 from first call + 1 from second call
        self.assertEqual(_dns_aid_stub.delete.call_count, 2)

    def test_teardown_flag_reverts_inflight_register(self):
        """If deregister_all fires while a register is in flight,
        the register should immediately undo its publish."""
        mgr = self._make_manager()

        async def _run():
            # Simulate: deregister_all fires first (sets flag),
            # then a register_async completes.
            mgr.deregister_all(force=True)
            # Now run a registration — it should publish then
            # immediately delete.
            await mgr._register_async("Dep", "/dep", 1, "10.0.0.1", 8000, "app")

        asyncio.run(_run())
        # publish was called, then delete was called to revert
        _dns_aid_stub.publish.assert_called_once()
        # delete called once for deregister_all (empty set) +
        # once for the reverted in-flight register
        self.assertTrue(_dns_aid_stub.delete.called)
        # The key should NOT be in _registered
        self.assertNotIn(("app", "Dep"), mgr._registered)

    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------

    def test_register_exception_does_not_propagate(self):
        """A failing publish should be logged, never raised."""
        _dns_aid_stub.publish.side_effect = RuntimeError("DNS down")
        mgr = self._make_manager()

        async def _run_tasks():
            mgr.register("BadDep", "/bad", 1, "10.0.0.1", 8000, "app")
            await asyncio.sleep(0)

        asyncio.run(_run_tasks())
        _dns_aid_stub.publish.side_effect = None

    def test_deregister_exception_does_not_propagate(self):
        """A failing delete should be logged, never raised."""
        _dns_aid_stub.delete.side_effect = RuntimeError("DNS down")
        mgr = self._make_manager()
        mgr._registered.add(("app", "BadDep"))

        async def _run_tasks():
            mgr.deregister("BadDep", "app")
            await asyncio.sleep(0)

        asyncio.run(_run_tasks())
        _dns_aid_stub.delete.side_effect = None

    # ------------------------------------------------------------------
    # agent_name convention
    # ------------------------------------------------------------------

    def test_agent_name_convention(self):
        mgr = self._make_manager(zone="agents.example.com")
        self.assertEqual(
            mgr._agent_name("MyDeployment", "my_app"),
            "my_app--mydeployment",
        )

    def test_agent_name_lowercased(self):
        mgr = self._make_manager()
        name = mgr._agent_name("CamelCase", "AppName")
        self.assertEqual(name, name.lower())


class TestDnsAidManagerMissingPackage(unittest.TestCase):
    """Ensure DnsAidManager is safe when dns_aid is not installed."""

    def setUp(self):
        _dns_aid_stub.publish.reset_mock()
        _dns_aid_stub.delete.reset_mock()

    def test_import_error_on_register_is_silent(self):
        """If dns_aid is not installed, registration should silently skip."""
        cfg = DnsAidConfig(enabled=True, zone="test.local")
        mgr = DnsAidManager(cfg)

        saved = sys.modules.pop("dns_aid", None)
        try:

            async def _run_tasks():
                await mgr._register_async("Dep", "/dep", 1, "127.0.0.1", 8000, "app")

            asyncio.run(_run_tasks())
        finally:
            if saved is not None:
                sys.modules["dns_aid"] = saved

    def test_import_error_on_deregister_is_silent(self):
        cfg = DnsAidConfig(enabled=True, zone="test.local")
        mgr = DnsAidManager(cfg)

        saved = sys.modules.pop("dns_aid", None)
        try:

            async def _run_tasks():
                await mgr._deregister_async("Dep", "my_app")

            asyncio.run(_run_tasks())
        finally:
            if saved is not None:
                sys.modules["dns_aid"] = saved


if __name__ == "__main__":
    unittest.main()
