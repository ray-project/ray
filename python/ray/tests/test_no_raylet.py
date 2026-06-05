"""
Tests for the --no-raylet option for starting head nodes as pure management nodes.

This tests the feature described in https://github.com/ray-project/ray/issues/62090
"""

import json
import subprocess
import sys
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import ray
import ray._private.node
import ray._private.parameter
import ray.scripts.scripts as scripts
from ray._common.test_utils import wait_for_condition
from ray.cluster_utils import cluster_not_supported

import psutil

_skip_no_cluster = pytest.mark.skipif(
    cluster_not_supported, reason="cluster not supported on this platform"
)


HEAD_PORT = 6399
HEAD_ADDRESS = f"127.0.0.1:{HEAD_PORT}"


def _ray_stop() -> None:
    subprocess.run(
        [sys.executable, "-m", "ray.scripts.scripts", "stop", "--force"],
        capture_output=True,
        timeout=30,
    )


def _start_no_raylet_head() -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "ray.scripts.scripts",
            "start",
            "--head",
            "--no-raylet",
            f"--port={HEAD_PORT}",
            "--include-dashboard=false",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )


@pytest.fixture
def no_raylet_head():
    """Start a no-raylet head, yield its CompletedProcess, then tear it down."""
    _ray_stop()
    proc = _start_no_raylet_head()
    assert proc.returncode == 0, f"Failed to start head: {proc.stderr}"
    try:
        yield proc
    finally:
        _ray_stop()


@_skip_no_cluster
class TestCLIValidation:
    """CLI-level validation of --no-raylet."""

    def test_rejected_for_worker_node(self):
        from click.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(
            scripts.start, ["--no-raylet", f"--address={HEAD_ADDRESS}"]
        )
        assert result.exit_code != 0
        assert "can only be used when starting a head node" in result.output


@_skip_no_cluster
class TestRayParamsValidation:
    """RayParams / Node-level validation independent of the CLI."""

    def test_default_is_false(self):
        params = ray._private.parameter.RayParams()
        assert params.no_raylet is False

    def test_param_settable(self):
        params = ray._private.parameter.RayParams(no_raylet=True)
        assert params.no_raylet is True

    def test_node_rejects_no_raylet_on_worker(self):
        # The Python API path must enforce the same invariant as the CLI:
        # no_raylet=True without head=True is illegal.
        params = ray._private.parameter.RayParams(no_raylet=True)
        with pytest.raises(ValueError, match="no_raylet"):
            ray._private.node.Node(
                params, head=False, shutdown_at_exit=False, spawn_reaper=False
            )


@_skip_no_cluster
class TestNoRayletHeadStartup:
    """Behavioral tests against a running no-raylet head."""

    @pytest.mark.parametrize(
        "expected_substring",
        [
            "no-raylet mode",
            "RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES",
        ],
    )
    def test_startup_logs_and_env(self, no_raylet_head, expected_substring):
        assert expected_substring in no_raylet_head.stdout

    def test_no_raylet_process_running(self, no_raylet_head):
        # The whole point of --no-raylet: no raylet binary process should exist
        # on this host after head startup.
        binary_raylets = [
            p
            for p in psutil.process_iter(attrs=["name"])
            if p.info.get("name") == "raylet"
        ]
        assert binary_raylets == [], f"Unexpected raylet processes: {binary_raylets}"

    def test_stale_head_node_id_cleared(self, no_raylet_head):
        # If a previous run with raylet left head_node_id in GCS KV,
        # no-raylet startup must delete it so job drivers don't get
        # scheduled on the (non-schedulable) head node.
        # Use GcsClient directly instead of ray.init() because in no-raylet
        # mode there is no raylet for the driver to connect to.
        from ray._private import ray_constants
        from ray._raylet import GcsClient

        gcs_client = GcsClient(address=HEAD_ADDRESS)
        val = gcs_client.internal_kv_get(
            ray_constants.KV_HEAD_NODE_ID_KEY,
            ray_constants.KV_NAMESPACE_JOB,
        )
        assert val is None, f"Expected head_node_id to be deleted but got: {val}"

    def test_worker_node_connects_via_kv_fallback(self, no_raylet_head):
        # Worker startup exercises the GCS internal KV fallback path in
        # Node.__init__ since the head has no NodeInfo entry.
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "ray.scripts.scripts",
                "start",
                f"--address={HEAD_ADDRESS}",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, f"Worker failed to start: {result.stderr}"

        ray.init(address=HEAD_ADDRESS, ignore_reinit_error=True)
        try:
            # Exactly one alive node — the worker. The head has no raylet,
            # so it does not register as a node.
            wait_for_condition(
                lambda: len([n for n in ray.nodes() if n.get("Alive")]) == 1,
                timeout=30,
            )

            @ray.remote
            def whoami():
                return ray.get_runtime_context().get_node_id()

            node_id = ray.get(whoami.remote())
            alive_ids = {n["NodeID"] for n in ray.nodes() if n.get("Alive")}
            assert node_id in alive_ids
        finally:
            ray.shutdown()


class TestNoRayletDisablesClientServer:
    """Verify --no-raylet forces ray_client_server_port to None."""

    def test_client_server_port_cleared(self):
        """Even if ray client dependency is installed and the default port
        would be 10001, --no-raylet must force it to None."""
        from click.testing import CliRunner

        runner = CliRunner()
        # We patch Node so we don't actually start anything; we just inspect
        # the RayParams that would have been used.
        captured_params = {}

        def fake_init(self_node, ray_params, *args, **kwargs):
            captured_params[
                "ray_client_server_port"
            ] = ray_params.ray_client_server_port
            raise SystemExit(0)

        with patch.object(ray._private.node.Node, "__init__", fake_init):
            runner.invoke(
                scripts.start,
                [
                    "--head",
                    "--no-raylet",
                    "--port=6399",
                    "--include-dashboard=false",
                    "--ray-client-server-port=10001",
                ],
            )

        assert captured_params.get("ray_client_server_port") is None, (
            f"Expected ray_client_server_port=None but got "
            f"{captured_params.get('ray_client_server_port')}"
        )

    def test_client_server_disabled_warning(self):
        """When --no-raylet is set and client server port was configured,
        a warning should be printed."""
        from click.testing import CliRunner

        runner = CliRunner()

        with patch.object(
            ray._private.node.Node, "__init__", side_effect=SystemExit(0)
        ):
            result = runner.invoke(
                scripts.start,
                [
                    "--head",
                    "--no-raylet",
                    "--port=6399",
                    "--include-dashboard=false",
                    "--ray-client-server-port=10001",
                ],
            )

        assert "Ray Client Server is disabled" in result.output

    def test_env_var_set(self):
        """--no-raylet must set RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES=1."""
        import os

        from click.testing import CliRunner

        runner = CliRunner()

        env_captured = {}

        def fake_init(self_node, ray_params, *args, **kwargs):
            env_captured["val"] = os.environ.get("RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES")
            raise SystemExit(0)

        with patch.object(ray._private.node.Node, "__init__", fake_init):
            runner.invoke(
                scripts.start,
                [
                    "--head",
                    "--no-raylet",
                    "--port=6399",
                    "--include-dashboard=false",
                ],
            )

        assert env_captured.get("val") == "1"

    def test_head_no_raylet_env_var_set(self):
        """--no-raylet must set RAY_HEAD_NO_RAYLET=1 so dashboard subprocesses
        know not to call ray.init()."""
        import os

        from click.testing import CliRunner

        runner = CliRunner()

        env_captured = {}

        def fake_init(self_node, ray_params, *args, **kwargs):
            env_captured["val"] = os.environ.get("RAY_HEAD_NO_RAYLET")
            raise SystemExit(0)

        with patch.object(ray._private.node.Node, "__init__", fake_init):
            runner.invoke(
                scripts.start,
                [
                    "--head",
                    "--no-raylet",
                    "--port=6399",
                    "--include-dashboard=false",
                ],
            )

        assert env_captured.get("val") == "1"


class TestInitRayDecoratorNoRaylet:
    """Verify @init_ray_and_catch_exceptions returns 503 in no-raylet mode
    for head-side modules, but allows worker-side agents through."""

    @pytest.mark.asyncio
    async def test_decorator_returns_503_for_head_module(self):
        """When RAY_HEAD_NO_RAYLET=1, decorated handlers on head-side modules
        (SubprocessModule, DashboardHeadModule) must return 503."""
        import os

        import aiohttp.web

        from ray.dashboard.optional_utils import init_ray_and_catch_exceptions

        class FakeHeadModule:
            gcs_address = "127.0.0.1:6379"

        @init_ray_and_catch_exceptions()
        async def handler(self):
            return aiohttp.web.Response(text="should not reach here")

        with patch.dict(os.environ, {"RAY_HEAD_NO_RAYLET": "1"}):
            resp = await handler(FakeHeadModule())

        assert resp.status == 503
        assert "no-raylet" in resp.text

    @pytest.mark.asyncio
    async def test_decorator_allows_agent_module(self):
        """When RAY_HEAD_NO_RAYLET=1, DashboardAgentModule handlers must NOT
        be blocked — worker agents always have a local raylet."""
        import os

        from ray.dashboard.optional_utils import init_ray_and_catch_exceptions
        from ray.dashboard.utils import DashboardAgentModule

        class FakeAgentModule(DashboardAgentModule):
            gcs_address = "127.0.0.1:6379"

            @staticmethod
            def is_minimal_module():
                return False

            async def run(self, server):
                pass

        @init_ray_and_catch_exceptions()
        async def handler(self):
            return "ok"

        with patch.dict(os.environ, {"RAY_HEAD_NO_RAYLET": "1"}):
            # Bypass __init__ (which requires a dashboard_agent arg) using
            # __new__, then manually set gcs_address so the decorator can
            # read it.
            fake = FakeAgentModule.__new__(FakeAgentModule)
            fake.gcs_address = "127.0.0.1:6379"
            # Mock ray.init so it raises immediately instead of hanging
            # trying to connect to a non-existent GCS.
            with patch("ray.is_initialized", return_value=False), patch(
                "ray.init", side_effect=ConnectionError("mock")
            ), patch("ray.shutdown"):
                resp = await handler(fake)

        # The decorator caught the ConnectionError and returned 500,
        # proving it did NOT short-circuit with 503 for AgentModule.
        assert resp.status == 500

    @pytest.mark.asyncio
    async def test_decorator_normal_mode(self):
        """When RAY_HEAD_NO_RAYLET is not set, the decorator should proceed
        normally (attempt ray.init)."""
        import os

        from ray.dashboard.optional_utils import init_ray_and_catch_exceptions

        class FakeModule:
            gcs_address = "127.0.0.1:6379"

        @init_ray_and_catch_exceptions()
        async def handler(self):
            return "ok"

        env_clean = {k: v for k, v in os.environ.items() if k != "RAY_HEAD_NO_RAYLET"}
        with patch.dict(os.environ, env_clean, clear=True):
            # Mock ray.init so it raises immediately instead of hanging
            # trying to connect to a non-existent GCS.
            with patch("ray.is_initialized", return_value=False), patch(
                "ray.init", side_effect=ConnectionError("mock")
            ), patch("ray.shutdown"):
                resp = await handler(FakeModule())

        assert resp.status == 500


class TestGetTargetAgentFallback:
    """Unit tests for JobHead.get_target_agent fallback to worker agents."""

    @pytest.fixture
    def job_head_stub(self):
        """Create a minimal JobHead-like object with the methods under test,
        bypassing the full SubprocessModule construction."""
        from ray.dashboard.modules.job.job_head import JobHead

        stub = object.__new__(JobHead)
        stub._agents = {}
        stub._gcs_client = AsyncMock()
        stub._head_agent_unavailable = False
        return stub

    @pytest.mark.asyncio
    async def test_head_agent_preferred(self, job_head_stub):
        """When head agent is available, it should be returned directly
        without trying worker agents."""
        head_client = MagicMock()
        job_head_stub._get_head_node_agent_once = AsyncMock(return_value=head_client)
        job_head_stub._get_any_available_agent_once = AsyncMock()

        result = await job_head_stub.get_target_agent(timeout_s=5)

        assert result is head_client
        job_head_stub._get_any_available_agent_once.assert_not_called()

    @pytest.mark.asyncio
    async def test_fallback_to_worker_agent(self, job_head_stub):
        """When head agent is unavailable, should fallback to a worker agent."""
        worker_client = MagicMock()
        job_head_stub._get_head_node_agent_once = AsyncMock(
            side_effect=Exception("Head node id has not yet been persisted")
        )
        job_head_stub._get_any_available_agent_once = AsyncMock(
            return_value=worker_client
        )

        result = await job_head_stub.get_target_agent(timeout_s=5)

        assert result is worker_client

    @pytest.mark.asyncio
    async def test_timeout_when_no_agent(self, job_head_stub):
        """When neither head nor worker agents are available, should raise
        TimeoutError."""
        job_head_stub._get_head_node_agent_once = AsyncMock(
            side_effect=Exception("no head agent")
        )
        job_head_stub._get_any_available_agent_once = AsyncMock(
            side_effect=Exception("no worker agent")
        )

        with pytest.raises(TimeoutError, match="Failed to get any available agent"):
            await job_head_stub.get_target_agent(timeout_s=1)

    @pytest.mark.asyncio
    async def test_get_any_available_agent_scans_kv(self, job_head_stub):
        """_get_any_available_agent_once should scan GCS Internal KV for
        registered dashboard agents and return the first reachable one."""
        from ray.dashboard.consts import DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX

        node_id_hex = "a" * 28
        prefix = DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX
        kv_key = f"{prefix}{node_id_hex}"

        job_head_stub.gcs_client.async_internal_kv_keys = AsyncMock(
            return_value=[kv_key.encode()]
        )

        agent_info = json.dumps(("1.2.3.4", 52365, 52366))
        job_head_stub.gcs_client.async_internal_kv_get = AsyncMock(
            return_value=agent_info.encode()
        )

        from ray.dashboard.modules.job.job_head import JobHead

        result = await JobHead._get_any_available_agent_once(job_head_stub)

        assert result is not None
        node_id = ray.NodeID.from_hex(node_id_hex)
        assert node_id in job_head_stub._agents

    @pytest.mark.asyncio
    async def test_get_any_available_agent_no_keys(self, job_head_stub):
        """When no agents are registered in KV, should raise."""
        job_head_stub.gcs_client.async_internal_kv_keys = AsyncMock(return_value=[])

        from ray.dashboard.modules.job.job_head import JobHead

        with pytest.raises(Exception, match="No dashboard agent registered"):
            await JobHead._get_any_available_agent_once(job_head_stub)


class TestReconcilerNoRaylet:
    """Unit tests for autoscaler reconciler behavior with head_no_raylet=True."""

    @pytest.fixture
    def reconciler_setup(self):
        from ray.autoscaler.v2.instance_manager.instance_manager import (
            InstanceManager,
        )
        from ray.autoscaler.v2.instance_manager.instance_storage import (
            InstanceStorage,
        )
        from ray.autoscaler.v2.instance_manager.storage import InMemoryStorage

        storage = InMemoryStorage()
        instance_storage = InstanceStorage(cluster_id="test-cluster", storage=storage)
        instance_manager = InstanceManager(
            instance_storage=instance_storage,
            instance_status_update_subscribers=[],
        )
        return instance_manager, instance_storage

    def _create_instance(self, instance_id, status, node_kind, cloud_instance_id=""):
        from ray.core.generated.instance_manager_pb2 import (
            Instance,
            NodeKind,
        )

        return Instance(
            instance_id=instance_id,
            status=status,
            instance_type="headgroup" if node_kind == NodeKind.HEAD else "cu8",
            cloud_instance_id=cloud_instance_id,
            node_kind=node_kind,
            status_history=[
                Instance.StatusHistory(
                    instance_status=status, timestamp_ns=time.time_ns()
                )
            ],
        )

    def _add_instances(self, instance_storage, instances):
        # Bypass InstanceManager's state machine: it would reject creating
        # an instance directly in RAY_RUNNING status (only ALLOCATED / QUEUED
        # / TERMINATING are allowed for new instances). Tests need to seed
        # arbitrary statuses, so we write directly to storage.
        instance_storage.batch_upsert_instances(instances)

    def test_is_head_node_running_true_with_no_raylet(self, reconciler_setup):
        """With head_no_raylet=True, a HEAD in ALLOCATED should count as running."""
        from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
        from ray.core.generated.instance_manager_pb2 import (
            Instance,
            NodeKind,
        )

        instance_manager, instance_storage = reconciler_setup
        head = self._create_instance(
            "h-1", Instance.ALLOCATED, NodeKind.HEAD, cloud_instance_id="pod-head"
        )
        self._add_instances(instance_storage, [head])

        assert (
            Reconciler._is_head_node_running(instance_manager, head_no_raylet=False)
            is False
        )
        assert (
            Reconciler._is_head_node_running(instance_manager, head_no_raylet=True)
            is True
        )

    def test_is_head_node_running_still_true_for_ray_running(self, reconciler_setup):
        """Normal mode: HEAD in RAY_RUNNING returns True regardless of flag."""
        from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
        from ray.core.generated.instance_manager_pb2 import (
            Instance,
            NodeKind,
        )

        instance_manager, instance_storage = reconciler_setup
        head = self._create_instance(
            "h-1", Instance.RAY_RUNNING, NodeKind.HEAD, cloud_instance_id="pod-head"
        )
        self._add_instances(instance_storage, [head])

        assert (
            Reconciler._is_head_node_running(instance_manager, head_no_raylet=False)
            is True
        )
        assert (
            Reconciler._is_head_node_running(instance_manager, head_no_raylet=True)
            is True
        )


@_skip_no_cluster
class TestNoRayletKVFlag:
    """Integration test: no-raylet startup writes head_no_raylet=1 to GCS KV."""

    def test_head_no_raylet_kv_set(self, no_raylet_head):
        # Use GcsClient directly instead of ray.init() because in no-raylet
        # mode there is no raylet for the driver to connect to.
        from ray._private import ray_constants
        from ray._raylet import GcsClient

        gcs_client = GcsClient(address=HEAD_ADDRESS)
        val = gcs_client.internal_kv_get(
            ray_constants.KV_HEAD_NO_RAYLET_KEY,
            ray_constants.KV_NAMESPACE_SESSION,
        )
        assert val == b"1", f"Expected head_no_raylet=b'1' but got: {val}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
