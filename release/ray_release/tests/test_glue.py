import os
import pytest
import shutil
import sys
import tempfile
import time
from typing import Type, Callable, Optional
import unittest
from unittest.mock import patch

from ray_release.alerts.handle import result_to_handle_map
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.cluster_manager.full import FullClusterManager
from ray_release.command_runner.command_runner import CommandRunner
from ray_release.config import (
    Test,
    DEFAULT_COMMAND_TIMEOUT,
    DEFAULT_WAIT_FOR_NODES_TIMEOUT,
)
from ray_release.exception import (
    ReleaseTestConfigError,
    LocalEnvSetupError,
    ClusterComputeCreateError,
    ClusterEnvBuildError,
    ClusterEnvBuildTimeout,
    ClusterEnvCreateError,
    ClusterCreationError,
    ClusterStartupError,
    ClusterStartupTimeout,
    RemoteEnvSetupError,
    CommandError,
    PrepareCommandError,
    CommandTimeout,
    PrepareCommandTimeout,
    TestCommandError,
    TestCommandTimeout,
    FetchResultError,
    LogsError,
    ResultsAlert,
    ClusterNodesWaitTimeout,
)
from ray_release.file_manager.file_manager import FileManager
from ray_release.glue import (
    run_release_test,
    type_str_to_command_runner,
    command_runner_to_cluster_manager,
    TIMEOUT_BUFFER_MINUTES,
)
from ray_release.logger import logger
from ray_release.reporter.reporter import Reporter
from ray_release.result import Result, ExitCode
from ray_release.tests.utils import MockSDK, APIDict


def _fail_on_call(error_type: Type[Exception] = RuntimeError, message: str = "Fail"):
    def _fail(*args, **kwargs):
        raise error_type(message)

    return _fail


class MockReturn:
    return_dict = {}

    def __getattribute__(self, item):
        return_dict = object.__getattribute__(self, "return_dict")
        if item in return_dict:
            mocked = return_dict[item]
            if isinstance(mocked, Callable):
                return mocked()
            else:
                return lambda *a, **kw: mocked
        return object.__getattribute__(self, item)


@patch("ray_release.glue.reinstall_anyscale_dependencies", lambda: None)
@patch("ray_release.glue.get_pip_packages", lambda: ["pip-packages"])
class GlueTest(unittest.TestCase):
    def writeClusterEnv(self, content: str):
        with open(os.path.join(self.tempdir, "cluster_env.yaml"), "wt") as fp:
            fp.write(content)

    def writeClusterCompute(self, content: str):
        with open(os.path.join(self.tempdir, "cluster_compute.yaml"), "wt") as fp:
            fp.write(content)

    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp()
        self.sdk = MockSDK()

        self.sdk.returns["get_project"] = APIDict(
            result=APIDict(name="unit_test_project")
        )
        self.sdk.returns["get_cloud"] = APIDict(result=APIDict(provider="AWS"))

        self.writeClusterEnv("{'env': true}")
        self.writeClusterCompute("{'compute': true}")

        with open(os.path.join(self.tempdir, "driver_fail.sh"), "wt") as f:
            f.write("exit 1\n")

        with open(os.path.join(self.tempdir, "driver_succeed.sh"), "wt") as f:
            f.write("exit 0\n")

        this_sdk = self.sdk
        this_tempdir = self.tempdir

        self.instances = {}

        self.cluster_manager_return = {}
        self.command_runner_return = {}
        self.file_manager_return = {}

        this_instances = self.instances
        this_cluster_manager_return = self.cluster_manager_return
        this_command_runner_return = self.command_runner_return
        this_file_manager_return = self.file_manager_return

        class MockClusterManager(MockReturn, FullClusterManager):
            def __init__(
                self,
                test_name: str,
                project_id: str,
                sdk=None,
                smoke_test: bool = False,
            ):
                super(MockClusterManager, self).__init__(
                    test_name, project_id, this_sdk, smoke_test=smoke_test
                )
                self.return_dict = this_cluster_manager_return
                this_instances["cluster_manager"] = self

        class MockCommandRunner(MockReturn, CommandRunner):
            return_dict = self.cluster_manager_return

            def __init__(
                self,
                cluster_manager: ClusterManager,
                file_manager: FileManager,
                working_dir,
                sdk=None,
                artifact_path: Optional[str] = None,
            ):
                super(MockCommandRunner, self).__init__(
                    cluster_manager, file_manager, this_tempdir
                )
                self.return_dict = this_command_runner_return

        class MockFileManager(MockReturn, FileManager):
            def __init__(self, cluster_manager: ClusterManager):
                super(MockFileManager, self).__init__(cluster_manager)
                self.return_dict = this_file_manager_return

        self.mock_alert_return = None

        def mock_alerter(test: Test, result: Result):
            return self.mock_alert_return

        result_to_handle_map["unit_test_alerter"] = (mock_alerter, False)

        type_str_to_command_runner["unit_test"] = MockCommandRunner
        command_runner_to_cluster_manager[MockCommandRunner] = MockClusterManager

        self.test = Test(
            name="unit_test_end_to_end",
            run=dict(
                type="unit_test",
                prepare="prepare_cmd",
                script="test_cmd",
                wait_for_nodes=dict(num_nodes=4, timeout=40),
            ),
            working_dir=self.tempdir,
            cluster=dict(
                cluster_env="cluster_env.yaml", cluster_compute="cluster_compute.yaml"
            ),
            alert="unit_test_alerter",
            driver_setup="driver_fail.sh",
        )
        self.anyscale_project = "prj_unit12345678"
        self.ray_wheels_url = "http://mock.wheels/"

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)

    def _succeed_until(self, until: str):
        # These commands should succeed
        self.command_runner_return["prepare_local_env"] = None

        if until == "local_env":
            return

        self.test["driver_setup"] = "driver_succeed.sh"

        if until == "driver_setup":
            return

        self.cluster_manager_return["cluster_compute_id"] = "valid"
        self.cluster_manager_return["create_cluster_compute"] = None

        if until == "cluster_compute":
            return

        self.cluster_manager_return["cluster_env_id"] = "valid"
        self.cluster_manager_return["create_cluster_env"] = None
        self.cluster_manager_return["cluster_env_build_id"] = "valid"
        self.cluster_manager_return["build_cluster_env"] = None

        if until == "cluster_env":
            return

        self.cluster_manager_return["cluster_id"] = "valid"
        self.cluster_manager_return["start_cluster"] = None

        if until == "cluster_start":
            return

        self.command_runner_return["prepare_remote_env"] = None

        if until == "remote_env":
            return

        self.command_runner_return["wait_for_nodes"] = None

        if until == "wait_for_nodes":
            return

        self.command_runner_return["run_prepare_command"] = None

        if until == "prepare_command":
            return

        self.command_runner_return["run_command"] = None

        if until == "test_command":
            return

        self.command_runner_return["fetch_results"] = {
            "time_taken": 50,
            "last_update": time.time() - 60,
        }

        if until == "fetch_results":
            return

        self.command_runner_return["get_last_logs_ex"] = "Lorem ipsum"

        if until == "get_last_logs":
            return

        self.mock_alert_return = None

    def _run(self, result: Result, **kwargs):
        run_release_test(
            test=self.test,
            anyscale_project=self.anyscale_project,
            result=result,
            ray_wheels_url=self.ray_wheels_url,
            **kwargs
        )

    def testInvalidClusterEnv(self):
        result = Result()

        # Any ReleaseTestConfigError
        with patch(
            "ray_release.glue.load_test_cluster_env",
            _fail_on_call(ReleaseTestConfigError),
        ), self.assertRaises(ReleaseTestConfigError):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CONFIG_ERROR.value)

        # Fails because file not found
        os.unlink(os.path.join(self.tempdir, "cluster_env.yaml"))
        with self.assertRaisesRegex(ReleaseTestConfigError, "Path not found"):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CONFIG_ERROR.value)

        # Fails because invalid jinja template
        self.writeClusterEnv("{{ INVALID")
        with self.assertRaisesRegex(ReleaseTestConfigError, "yaml template"):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CONFIG_ERROR.value)

        # Fails because invalid json
        self.writeClusterEnv("{'test': true, 'fail}")
        with self.assertRaisesRegex(ReleaseTestConfigError, "quoted scalar"):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CONFIG_ERROR.value)

    def testInvalidClusterCompute(self):
        result = Result()

        with patch(
            "ray_release.glue.load_test_cluster_compute",
            _fail_on_call(ReleaseTestConfigError),
        ), self.assertRaises(ReleaseTestConfigError):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CONFIG_ERROR.value)

        # Fails because file not found
        os.unlink(os.path.join(self.tempdir, "cluster_compute.yaml"))
        with self.assertRaisesRegex(ReleaseTestConfigError, "Path not found"):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CONFIG_ERROR.value)

        # Fails because invalid jinja template
        self.writeClusterCompute("{{ INVALID")
        with self.assertRaisesRegex(ReleaseTestConfigError, "yaml template"):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CONFIG_ERROR.value)

        # Fails because invalid json
        self.writeClusterCompute("{'test': true, 'fail}")
        with self.assertRaisesRegex(ReleaseTestConfigError, "quoted scalar"):
            self._run(result)

        self.assertEqual(result.return_code, ExitCode.CONFIG_ERROR.value)

    def testAutomaticClusterEnvVariables(self):
        result = Result()

        self._succeed_until("local_env")

        with self.assertRaises(LocalEnvSetupError):
            self._run(result)

        cluster_manager = self.instances["cluster_manager"]

        command_timeout = self.test["run"].get("timeout", DEFAULT_COMMAND_TIMEOUT)
        prepare_cmd = self.test["run"].get("prepare", None)
        if prepare_cmd:
            prepare_timeout = self.test["run"].get("prepare_timeout", command_timeout)
        else:
            prepare_timeout = 0
        command_and_prepare_timeout = command_timeout + prepare_timeout

        wait_timeout = self.test["run"]["wait_for_nodes"].get(
            "timeout", DEFAULT_WAIT_FOR_NODES_TIMEOUT
        )

        expected_idle_termination_minutes = int(
            command_and_prepare_timeout / 60 + TIMEOUT_BUFFER_MINUTES
        )
        expected_maximum_uptime_minutes = int(
            expected_idle_termination_minutes + wait_timeout + TIMEOUT_BUFFER_MINUTES
        )

        self.assertEqual(
            cluster_manager.cluster_compute["idle_termination_minutes"],
            expected_idle_termination_minutes,
        )
        self.assertEqual(
            cluster_manager.cluster_compute["maximum_uptime_minutes"],
            expected_maximum_uptime_minutes,
        )

    def testInvalidPrepareLocalEnv(self):
        result = Result()

        self.command_runner_return["prepare_local_env"] = _fail_on_call(
            LocalEnvSetupError
        )
        with self.assertRaises(LocalEnvSetupError):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.LOCAL_ENV_SETUP_ERROR.value)

    def testDriverSetupFails(self):
        result = Result()

        self._succeed_until("local_env")

        with self.assertRaises(LocalEnvSetupError):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.LOCAL_ENV_SETUP_ERROR.value)

    def testInvalidClusterIdOverride(self):
        result = Result()

        self._succeed_until("driver_setup")

        self.sdk.returns["get_cluster_environment"] = None

        with self.assertRaises(ClusterEnvCreateError):
            self._run(result, cluster_env_id="existing")

        self.sdk.returns["get_cluster_environment"] = APIDict(
            result=APIDict(config_json={"overridden": True})
        )

        with self.assertRaises(Exception) as cm:  # Fail somewhere else
            self._run(result, cluster_env_id="existing")
            self.assertNotIsInstance(cm.exception, ClusterEnvCreateError)

    def testBuildConfigFailsClusterCompute(self):
        result = Result()

        self._succeed_until("driver_setup")

        # These commands should succeed
        self.command_runner_return["prepare_local_env"] = None

        # Fails because API response faulty
        with self.assertRaisesRegex(ClusterComputeCreateError, "Unexpected"):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CLUSTER_RESOURCE_ERROR.value)

        # Fails for random cluster compute reason
        self.cluster_manager_return["create_cluster_compute"] = _fail_on_call(
            ClusterComputeCreateError, "Known"
        )
        with self.assertRaisesRegex(ClusterComputeCreateError, "Known"):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CLUSTER_RESOURCE_ERROR.value)

    def testBuildConfigFailsClusterEnv(self):
        result = Result()

        self._succeed_until("cluster_compute")

        # Fails because API response faulty
        with self.assertRaisesRegex(ClusterEnvCreateError, "Unexpected"):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CLUSTER_RESOURCE_ERROR.value)

        # Fails for random cluster env create reason
        self.cluster_manager_return["create_cluster_env"] = _fail_on_call(
            ClusterEnvCreateError, "Known"
        )
        with self.assertRaisesRegex(ClusterEnvCreateError, "Known"):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CLUSTER_RESOURCE_ERROR.value)

        # Now, succeed creation but fail on cluster env build
        self.cluster_manager_return["cluster_env_id"] = "valid"
        self.cluster_manager_return["create_cluster_env"] = None
        self.cluster_manager_return["build_cluster_env"] = _fail_on_call(
            ClusterEnvBuildError
        )
        with self.assertRaises(ClusterEnvBuildError):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CLUSTER_ENV_BUILD_ERROR.value)

        # Now, fail on cluster env timeout
        self.cluster_manager_return["build_cluster_env"] = _fail_on_call(
            ClusterEnvBuildTimeout
        )
        with self.assertRaises(ClusterEnvBuildTimeout):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CLUSTER_ENV_BUILD_TIMEOUT.value)

    def testStartClusterFails(self):
        result = Result()

        self._succeed_until("cluster_env")

        # Fails because API response faulty
        with self.assertRaises(ClusterCreationError):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CLUSTER_RESOURCE_ERROR.value)

        self.cluster_manager_return["cluster_id"] = "valid"

        # Fail for random cluster startup reason
        self.cluster_manager_return["start_cluster"] = _fail_on_call(
            ClusterStartupError
        )
        with self.assertRaises(ClusterStartupError):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CLUSTER_STARTUP_ERROR.value)

        # Ensure cluster was terminated
        self.assertGreaterEqual(self.sdk.call_counter["terminate_cluster"], 1)

        # Fail for cluster startup timeout
        self.cluster_manager_return["start_cluster"] = _fail_on_call(
            ClusterStartupTimeout
        )
        with self.assertRaises(ClusterStartupTimeout):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CLUSTER_STARTUP_TIMEOUT.value)

        # Ensure cluster was terminated
        self.assertGreaterEqual(self.sdk.call_counter["terminate_cluster"], 1)

    def testPrepareRemoteEnvFails(self):
        result = Result()

        self._succeed_until("cluster_start")

        self.command_runner_return["prepare_remote_env"] = _fail_on_call(
            RemoteEnvSetupError
        )
        with self.assertRaises(RemoteEnvSetupError):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.REMOTE_ENV_SETUP_ERROR.value)

        # Ensure cluster was terminated
        self.assertGreaterEqual(self.sdk.call_counter["terminate_cluster"], 1)

    def testWaitForNodesFails(self):
        result = Result()

        self._succeed_until("remote_env")

        # Wait for nodes command fails
        self.command_runner_return["wait_for_nodes"] = _fail_on_call(
            ClusterNodesWaitTimeout
        )
        with self.assertRaises(ClusterNodesWaitTimeout):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CLUSTER_WAIT_TIMEOUT.value)

        # Ensure cluster was terminated
        self.assertGreaterEqual(self.sdk.call_counter["terminate_cluster"], 1)

    def testPrepareCommandFails(self):
        result = Result()

        self._succeed_until("wait_for_nodes")

        # Prepare command fails
        self.command_runner_return["run_prepare_command"] = _fail_on_call(CommandError)
        with self.assertRaises(PrepareCommandError):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.PREPARE_ERROR.value)

        # Prepare command times out
        self.command_runner_return["run_prepare_command"] = _fail_on_call(
            CommandTimeout
        )
        with self.assertRaises(PrepareCommandTimeout):
            self._run(result)
        # Special case: Prepare commands are usually waiting for nodes
        # (this may change in the future!)
        self.assertEqual(result.return_code, ExitCode.CLUSTER_WAIT_TIMEOUT.value)

        # Ensure cluster was terminated
        self.assertGreaterEqual(self.sdk.call_counter["terminate_cluster"], 1)

    def testTestCommandFails(self):
        result = Result()

        self._succeed_until("prepare_command")

        # Test command fails
        self.command_runner_return["run_command"] = _fail_on_call(CommandError)
        with self.assertRaises(TestCommandError):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.COMMAND_ERROR.value)

        # Test command times out
        self.command_runner_return["run_command"] = _fail_on_call(CommandTimeout)
        with self.assertRaises(TestCommandTimeout):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.COMMAND_TIMEOUT.value)

        # Ensure cluster was terminated
        self.assertGreaterEqual(self.sdk.call_counter["terminate_cluster"], 1)

    def testTestCommandTimeoutLongRunning(self):
        result = Result()

        self._succeed_until("fetch_results")

        # Test command times out
        self.command_runner_return["run_command"] = _fail_on_call(CommandTimeout)
        with self.assertRaises(TestCommandTimeout):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.COMMAND_TIMEOUT.value)

        # But now set test to long running
        self.test["run"]["long_running"] = True
        self._run(result)  # Will not fail this time

        self.assertGreaterEqual(result.results["last_update_diff"], 60.0)

        # Ensure cluster was terminated
        self.assertGreaterEqual(self.sdk.call_counter["terminate_cluster"], 1)

    def testSmokeUnstableTest(self):
        result = Result()

        self._succeed_until("complete")

        self.test["stable"] = False
        self._run(result, smoke_test=True)

        # Ensure stable and smoke_test are set correctly.
        assert not result.stable
        assert result.smoke_test

    def testFetchResultFails(self):
        result = Result()

        self._succeed_until("test_command")

        self.command_runner_return["fetch_results"] = _fail_on_call(FetchResultError)
        with self.assertLogs(logger, "ERROR") as cm:
            self._run(result)
            self.assertTrue(any("Could not fetch results" in o for o in cm.output))
        self.assertEqual(result.return_code, ExitCode.SUCCESS.value)
        self.assertEqual(result.status, "finished")

        # Ensure cluster was terminated
        self.assertGreaterEqual(self.sdk.call_counter["terminate_cluster"], 1)

    def testFetchResultFailsReqNonEmptyResult(self):
        # set `require_result` bit.
        new_handler = (result_to_handle_map["unit_test_alerter"], True)
        result_to_handle_map["unit_test_alerter"] = new_handler

        result = Result()

        self._succeed_until("test_command")

        self.command_runner_return["fetch_results"] = _fail_on_call(FetchResultError)
        with self.assertRaisesRegex(FetchResultError, "Fail"):
            with self.assertLogs(logger, "ERROR") as cm:
                self._run(result)
                self.assertTrue(any("Could not fetch results" in o for o in cm.output))
        self.assertEqual(result.return_code, ExitCode.FETCH_RESULT_ERROR.value)
        self.assertEqual(result.status, "infra_error")

        # Ensure cluster was terminated, no matter what
        self.assertGreaterEqual(self.sdk.call_counter["terminate_cluster"], 1)

    def testLastLogsFails(self):
        result = Result()

        self._succeed_until("fetch_results")

        self.command_runner_return["get_last_logs_ex"] = _fail_on_call(LogsError)

        with self.assertLogs(logger, "ERROR") as cm:
            self._run(result)
            self.assertTrue(any("Error fetching logs" in o for o in cm.output))
        self.assertEqual(result.return_code, ExitCode.SUCCESS.value)
        self.assertEqual(result.status, "finished")

        # Ensure cluster was terminated
        self.assertGreaterEqual(self.sdk.call_counter["terminate_cluster"], 1)

    def testAlertFails(self):
        result = Result()

        self._succeed_until("get_last_logs")

        self.mock_alert_return = "Alert raised"

        with self.assertRaises(ResultsAlert):
            self._run(result)

        self.assertEqual(result.return_code, ExitCode.COMMAND_ALERT.value)
        self.assertEqual(result.status, "error")

        # Ensure cluster was terminated
        self.assertGreaterEqual(self.sdk.call_counter["terminate_cluster"], 1)

    def testReportFails(self):
        result = Result()

        self._succeed_until("complete")

        class FailReporter(Reporter):
            def report_result_ex(self, test: Test, result: Result):
                raise RuntimeError

        with self.assertLogs(logger, "ERROR") as cm:
            self._run(result, reporters=[FailReporter()])
            self.assertTrue(any("Error reporting results" in o for o in cm.output))

        self.assertEqual(result.return_code, ExitCode.SUCCESS.value)
        self.assertEqual(result.status, "finished")

        # Ensure cluster was terminated
        self.assertGreaterEqual(self.sdk.call_counter["terminate_cluster"], 1)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
