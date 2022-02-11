import shutil
import tempfile
import unittest
from typing import Type
from unittest.mock import patch

from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.cluster_manager.full import FullClusterManager
from ray_release.command_runner.command_runner import CommandRunner
from ray_release.config import Test
from ray_release.exception import ReleaseTestConfigError
from ray_release.file_manager.file_manager import FileManager
from ray_release.glue import (
    run_release_test,
    type_str_to_command_runner,
    command_runner_to_cluster_manager,
    command_runner_to_file_manager,
)
from ray_release.result import Result
from ray_release.tests.utils import MockSDK, APIDict


def _fail_on_call(error_type: Type[Exception] = RuntimeError, message: str = "Fail"):
    def _fail(*args, **kwargs):
        raise error_type(message)

    return _fail


class EndToEndTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp()
        self.sdk = MockSDK()

        self.sdk.returns["get_project"] = APIDict(
            result=APIDict(name="unit_test_project")
        )

        this_sdk = self.sdk
        this_tempdir = self.tempdir

        class MockClusterManager(FullClusterManager):
            def __init__(self, test_name: str, project_id: str, sdk=None):
                super(MockClusterManager, self).__init__(
                    test_name, project_id, this_sdk
                )

        class MockCommandRunner(CommandRunner):
            def __init__(
                self,
                cluster_manager: ClusterManager,
                file_manager: FileManager,
                working_dir: str,
            ):
                super(MockCommandRunner, self).__init__(
                    cluster_manager, file_manager, this_tempdir
                )

        class MockFileManager(FileManager):
            pass

        type_str_to_command_runner["unit_test"] = MockCommandRunner
        command_runner_to_cluster_manager[MockCommandRunner] = MockClusterManager
        command_runner_to_file_manager[MockCommandRunner] = MockFileManager

        self.test = Test(
            name="unit_test_end_to_end",
            run=dict(type="unit_test"),
            working_dir=self.tempdir,
        )
        self.anyscale_project = "prj_unit12345678"
        self.ray_wheels_url = "http://mock.wheels/"

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)

    def testConfigInvalid(self):
        # Missing keys
        # Unknown command runner
        pass

    def testInvalidClusterEnv(self):
        result = Result()

        with patch(
            "ray_release.glue.load_test_cluster_env",
            _fail_on_call(ReleaseTestConfigError),
        ), self.assertRaises(ReleaseTestConfigError):
            run_release_test(
                test=self.test,
                anyscale_project=self.anyscale_project,
                result=result,
                ray_wheels_url=self.ray_wheels_url,
            )
            assert result.return_code > 0

        # Not specified
        # File not found
        # Jinja render error
        pass

    def testInvalidClusterCompute(self):
        # Not specified
        # File not found
        # Jinja render error
        pass

    def testInvalidPrepareLocalEnv(self):
        # Anything
        pass

    def testInvalidClusterIdOverride(self):
        # get_cluster_name() fails
        pass

    def testBuildConfigFailsClusterCompute(self):
        # Create cluster compute fails
        pass

    def testBuildConfigFailsClusterEnv(self):
        # Create cluster env fails
        # Build cluster env fails
        pass

    def testStartClusterFails(self):
        pass

    def testStartClusterTimeout(self):
        pass

    def testPrepareRemoteEnvFails(self):
        pass

    def testPrepareCommandFails(self):
        pass

    def testPrepareCommandTimeout(self):
        pass

    def testTestCommandFails(self):
        pass

    def testTestCommandTimeout(self):
        pass

    def testFetchResultFails(self):
        pass

    def testLastLogsFails(self):
        pass

    def testAlertFails(self):
        pass

    def testReportFails(self):
        pass

    def testSuccessCaseOne(self):
        # New cluster compute
        # New cluster env
        # New cluster build
        # No smoke test
        pass

    def testSuccessCaseTwo(self):
        # Existing cluster compute
        # Existing cluster env
        # New cluster build
        # Smoke test
        pass

    def testSuccessCaseThree(self):
        # Existing cluster compute
        # Existing cluster env
        # Existing cluster build
        pass
