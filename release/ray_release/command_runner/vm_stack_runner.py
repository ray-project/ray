import json
import os
import tempfile
import time
from typing import Optional, Dict, Any
import subprocess

from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK
from ray_release.anyscale_util import LAST_LOGS_LENGTH

from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.command_runner.command_runner import CommandRunner
from ray_release.exception import (
    CommandTimeout,
    CommandError,
    ResultsError,
    LogsError,
    RemoteEnvSetupError,
    ClusterNodesWaitTimeout,
    LocalEnvSetupError,
)
from ray_release.file_manager.file_manager import FileManager
from ray_release.logger import logger
from ray_release.util import format_link, get_anyscale_sdk, exponential_backoff_retry
from ray_release.wheels import install_matching_ray_locally

def test_ray_up():
    subprocess.check_output(
        f"ray up cluster_launcher_config.yaml",
        shell=True,
        env=os.environ,
        stderr=subprocess.STDOUT,
        text=True,
    )


class VmStackRunner(CommandRunner):
    def __init__(
        self,
        cluster_manager: ClusterManager,
        file_manager: FileManager,
        working_dir: str,
        sdk: Optional[AnyscaleSDK] = None,
    ):
        super(CommandRunner, self).__init__()

    # I presume this is to install necessary deps to run commands
    def prepare_local_env(self, ray_wheels_url: Optional[str] = None):
        '''
        TODO copy from client_runner, it installs matching ray locally.
        '''
        try:
            install_matching_ray_locally(
                ray_wheels_url or os.environ.get("RAY_WHEELS", None)
            )

        except Exception as e:
            raise LocalEnvSetupError(f"Error setting up local environment: {e}") from e

    def prepare_remote_env(self):
        # TODO
        pass

    def wait_for_nodes(self, num_nodes: int, timeout: float = 900):
        # TODO
        pass

    def run_command(
        self, command: str, env: Optional[Dict] = None, timeout: float = 3600.0
    ) -> float:
        # TODO
        pass

    def get_last_logs(self, scd_id: Optional[str] = None):
        raise NotImplementedError

    def fetch_results(self) -> Dict[str, Any]:
        # TODO
        return {}
        
