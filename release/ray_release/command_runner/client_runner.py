import os
import shlex
import subprocess
import sys
import tempfile
import threading
import time
from collections import deque
from typing import Optional, Dict, Any

from ray_release.anyscale_util import LAST_LOGS_LENGTH

from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.exception import (
    ResultsError,
    LocalEnvSetupError,
    ClusterNodesWaitTimeout,
    CommandTimeout,
    ClusterStartupError,
    CommandError,
)
from ray_release.file_manager.file_manager import FileManager
from ray_release.logger import logger
from ray_release.command_runner.command_runner import CommandRunner
from ray_release.wheels import install_matching_ray_locally
from ray_release.util import read_json


def install_cluster_env_packages(cluster_env: Dict[Any, Any]):
    os.environ.update(cluster_env.get("env_vars", {}))
    packages = cluster_env["python"]["pip_packages"]
    logger.info(f"Installing cluster env packages locally: {packages}")

    for package in packages:
        subprocess.check_output(
            f"pip install -U {shlex.quote(package)}",
            shell=True,
            env=os.environ,
            text=True,
        )


class ClientRunner(CommandRunner):
    def __init__(
        self,
        cluster_manager: ClusterManager,
        file_manager: FileManager,
        working_dir: str,
    ):
        super(ClientRunner, self).__init__(cluster_manager, file_manager, working_dir)

        self.last_logs = None
        self.result_output_json = tempfile.mktemp()
        self.metrics_output_json = tempfile.mktemp()

    def prepare_remote_env(self):
        pass

    def prepare_local_env(self, ray_wheels_url: Optional[str] = None):
        try:
            install_matching_ray_locally(
                ray_wheels_url or os.environ.get("RAY_WHEELS", None)
            )
            install_cluster_env_packages(self.cluster_manager.cluster_env)
        except Exception as e:
            raise LocalEnvSetupError(f"Error setting up local environment: {e}") from e

    def wait_for_nodes(self, num_nodes: int, timeout: float = 900):
        import ray

        ray_address = self.cluster_manager.get_cluster_address()
        try:
            if ray.is_initialized:
                ray.shutdown()

            ray.init(address=ray_address)

            start_time = time.monotonic()
            timeout_at = start_time + timeout
            next_status = start_time + 30
            nodes_up = sum(1 for node in ray.nodes() if node["Alive"])
            while nodes_up < num_nodes:
                now = time.monotonic()
                if now >= timeout_at:
                    raise ClusterNodesWaitTimeout(
                        f"Only {nodes_up}/{num_nodes} are up after "
                        f"{timeout} seconds."
                    )

                if now >= next_status:
                    logger.info(
                        f"Waiting for nodes to come up: "
                        f"{nodes_up}/{num_nodes} "
                        f"({now - start_time:.2f} seconds, "
                        f"timeout: {timeout} seconds)."
                    )
                    next_status += 30

                time.sleep(1)
                nodes_up = sum(1 for node in ray.nodes() if node["Alive"])

            ray.shutdown()
        except Exception as e:
            raise ClusterStartupError(f"Exception when waiting for nodes: {e}") from e

        logger.info(f"All {num_nodes} nodes are up.")

    def save_metrics(self, start_time: float, timeout: float = 900):
        metrics_script = os.path.join(
            os.path.dirname(__file__), "_prometheus_metrics.py"
        )
        self.run_command(
            f'python "{metrics_script}" {start_time} --use_ray', timeout=timeout
        )

    def get_last_logs(self) -> Optional[str]:
        return self.last_logs

    def _fetch_json(self, path: str) -> Dict[str, Any]:
        try:
            return read_json(path)
        except Exception as e:
            raise ResultsError(
                f"Could not load local results from client command: {e}"
            ) from e

    def fetch_results(self) -> Dict[str, Any]:
        return self._fetch_json(self.result_output_json)

    def fetch_metrics(self) -> Dict[str, Any]:
        return self._fetch_json(self.metrics_output_json)

    def run_command(
        self, command: str, env: Optional[Dict] = None, timeout: float = 3600.0
    ) -> float:
        logger.info(
            f"Running command using Ray client on cluster "
            f"{self.cluster_manager.cluster_name}: {command}"
        )

        env = env or {}
        full_env = self.get_full_command_env(
            {
                **os.environ,
                **env,
                "RAY_ADDRESS": self.cluster_manager.get_cluster_address(),
                "RAY_JOB_NAME": "test_job",
                "PYTHONUNBUFFERED": "1",
            }
        )

        kill_event = threading.Event()

        def _kill_after(
            proc: subprocess.Popen,
            timeout: int = 30,
            kill_event: Optional[threading.Event] = None,
        ):
            timeout_at = time.monotonic() + timeout
            while time.monotonic() < timeout_at:
                if proc.poll() is not None:
                    return
                time.sleep(1)
            logger.info(
                f"Client command timed out after {timeout} seconds, "
                f"killing subprocess."
            )
            if kill_event:
                kill_event.set()
            proc.terminate()

        start_time = time.monotonic()
        proc = subprocess.Popen(
            command,
            env=full_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,
            text=True,
        )

        kill_thread = threading.Thread(
            target=_kill_after, args=(proc, timeout, kill_event)
        )
        kill_thread.start()

        proc.stdout.reconfigure(line_buffering=True)
        sys.stdout.reconfigure(line_buffering=True)
        logs = deque(maxlen=LAST_LOGS_LENGTH)
        for line in proc.stdout:
            logs.append(line)
            sys.stdout.write(line)
        proc.wait()
        sys.stdout.reconfigure(line_buffering=False)
        time_taken = time.monotonic() - start_time
        self.last_logs = "\n".join(logs)

        return_code = proc.poll()
        if return_code == -15 or return_code == 15 or kill_event.is_set():
            # Process has been terminated
            raise CommandTimeout(f"Cluster command timed out after {timeout} seconds.")
        if return_code != 0:
            raise CommandError(f"Command returned non-success status: {return_code}")

        logger.warning(f"WE GOT RETURN CODE {return_code} AFTER {time_taken}")

        return time_taken
