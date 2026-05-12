import os
import shutil
import sys
import tempfile
import time
import unittest
from typing import Callable, Optional, Type

import pytest

from ray_release.alerts.handle import result_to_handle_map
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.cluster_manager.minimal import MinimalClusterManager
from ray_release.command_runner.anyscale_job_runner import AnyscaleJobRunner
from ray_release.exception import (
    CommandError,
    CommandTimeout,
    ExitCode,
    FetchResultError,
    LogsError,
    PrepareCommandError,
    PrepareCommandTimeout,
    ReleaseTestConfigError,
    ResultsAlert,
    TestCommandError,
    TestCommandTimeout,
)
from ray_release.file_manager.job_file_manager import JobFileManager
from ray_release.glue import (
    command_runner_to_cluster_manager,
    run_release_test,
    type_str_to_command_runner,
)
from ray_release.logger import logger
from ray_release.reporter.reporter import Reporter
from ray_release.result import Result
from ray_release.test import Test
from ray_release.tests.utils import APIDict, MockSDK


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


class MockTest(Test):
    def get_anyscale_byod_image(self) -> str:
        return ""


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
        self.writeClusterCompute(
            "{'head_node_type': {'name': 'head_node', 'instance_type': 'm5a.4xlarge'}, 'worker_node_types': []}"
        )

        with open(os.path.join(self.tempdir, "driver_fail.sh"), "wt") as f:
            f.write("exit 1\n")

        with open(os.path.join(self.tempdir, "driver_succeed.sh"), "wt") as f:
            f.write("exit 0\n")

        this_sdk = self.sdk
        this_tempdir = self.tempdir

        self.instances = {}

        self.cluster_manager_return = {}
        self.command_runner_return = {}

        this_instances = self.instances
        this_cluster_manager_return = self.cluster_manager_return
        this_command_runner_return = self.command_runner_return

        class MockClusterManager(MockReturn, MinimalClusterManager):
            def __init__(
                self,
                test_name: str,
                project_id: str,
                sdk=None,
                smoke_test: bool = False,
            ):
                super(MockClusterManager, self).__init__(
                    test_name,
                    project_id,
                    this_sdk,
                    smoke_test=smoke_test,
                )
                self.return_dict = this_cluster_manager_return
                this_instances["cluster_manager"] = self

        class FakeFileManager(JobFileManager):
            def __init__(self, cluster_manager: ClusterManager):
                super(FakeFileManager, self).__init__(cluster_manager)

            def download_from_cloud(
                self, key: str, target: str, delete_after_download: bool = False
            ):
                with open(target, "wt") as f:
                    f.write("fake download content")

            def delete(self, key: str, recursive: bool = False):
                pass

        class MockCommandRunner(MockReturn, AnyscaleJobRunner):
            return_dict = self.cluster_manager_return

            def __init__(
                self,
                cluster_manager: ClusterManager,
                file_manager: JobFileManager,
                working_dir,
                sdk=None,
                artifact_path: Optional[str] = None,
            ):
                super(MockCommandRunner, self).__init__(
                    cluster_manager,
                    FakeFileManager(cluster_manager),
                    this_tempdir,
                    sdk=this_sdk,
                    artifact_path=artifact_path,
                )
                self.return_dict = this_command_runner_return

        self.mock_alert_return = None

        def mock_alerter(test: Test, result: Result):
            return self.mock_alert_return

        result_to_handle_map["unit_test_alerter"] = (mock_alerter, False)

        type_str_to_command_runner["unit_test"] = MockCommandRunner
        command_runner_to_cluster_manager[MockCommandRunner] = MockClusterManager

        self.test = MockTest(
            name="unit_test_end_to_end",
            run=dict(
                type="unit_test",
                prepare="prepare_cmd",
                script="test_cmd",
                wait_for_nodes=dict(num_nodes=4, timeout=40),
            ),
            working_dir=self.tempdir,
            cluster=dict(
                cluster_env="cluster_env.yaml",
                cluster_compute="cluster_compute.yaml",
                byod={},
            ),
            alert="unit_test_alerter",
        )
        self.kuberay_test = MockTest(
            name="unit_test_end_to_end_kuberay",
            run=dict(
                type="unit_test",
                prepare="prepare_cmd",
                script="test_cmd",
                wait_for_nodes=dict(num_nodes=4, timeout=40),
            ),
            working_dir=self.tempdir,
            cluster=dict(
                cluster_env="cluster_env.yaml",
                cluster_compute="cluster_compute.yaml",
                byod={},
            ),
            env="kuberay",
            alert="unit_test_alerter",
        )
        self.anyscale_project = "prj_unit12345678"

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)

    def _succeed_until(self, until: str):
        # These commands should succeed
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

        if until == "cluster_start":
            return

        self.command_runner_return["prepare_remote_env"] = None

        if until == "remote_env":
            return

        self.command_runner_return["wait_for_nodes"] = None

        if until == "wait_for_nodes":
            return

        self.command_runner_return["run_prepare_command"] = None

        self.command_runner_return["job_url"] = "http://mock-job-url"
        self.command_runner_return["job_id"] = "mock-job-id"

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

    def _run(self, result: Result, kuberay: bool = False, **kwargs):
        if kuberay:
            run_release_test(test=self.kuberay_test, result=result, **kwargs)
        else:
            run_release_test(
                test=self.test,
                anyscale_project=self.anyscale_project,
                result=result,
                **kwargs
            )

    def testInvalidClusterCompute(self):
        result = Result()

        # Fails because file not found
        os.unlink(os.path.join(self.tempdir, "cluster_compute.yaml"))
        with self.assertRaisesRegex(ReleaseTestConfigError, "Path not found"):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CONFIG_ERROR.value)
        with self.assertRaisesRegex(ReleaseTestConfigError, "Path not found"):
            self._run(result, True)
        self.assertEqual(result.return_code, ExitCode.CONFIG_ERROR.value)

        # Fails because invalid jinja template
        self.writeClusterCompute("{{ INVALID")
        with self.assertRaisesRegex(ReleaseTestConfigError, "yaml template"):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CONFIG_ERROR.value)
        with self.assertRaisesRegex(ReleaseTestConfigError, "yaml template"):
            self._run(result, True)
        self.assertEqual(result.return_code, ExitCode.CONFIG_ERROR.value)

        # Fails because invalid json
        self.writeClusterCompute("{'test': true, 'fail}")
        with self.assertRaisesRegex(ReleaseTestConfigError, "quoted scalar"):
            self._run(result)
        self.assertEqual(result.return_code, ExitCode.CONFIG_ERROR.value)
        with self.assertRaisesRegex(ReleaseTestConfigError, "quoted scalar"):
            self._run(result, True)
        self.assertEqual(result.return_code, ExitCode.CONFIG_ERROR.value)

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
        self.assertIsNone(result.job_url)

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
        self.assertIsNotNone(result.job_url)

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
        self.assertEqual(result.status, "success")

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

    def testLastLogsFails(self):
        result = Result()

        self._succeed_until("fetch_results")

        self.command_runner_return["get_last_logs_ex"] = _fail_on_call(LogsError)

        with self.assertLogs(logger, "ERROR") as cm:
            self._run(result)
            self.assertTrue(any("Error fetching logs" in o for o in cm.output))
        self.assertEqual(result.return_code, ExitCode.SUCCESS.value)
        self.assertEqual(result.status, "success")

    def testAlertFails(self):
        result = Result()

        self._succeed_until("get_last_logs")

        self.mock_alert_return = "Alert raised"

        with self.assertRaises(ResultsAlert):
            self._run(result)

        self.assertEqual(result.return_code, ExitCode.COMMAND_ALERT.value)
        self.assertEqual(result.status, "error")

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
        self.assertEqual(result.status, "success")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
