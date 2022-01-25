import time

import ray

from ray_release.exception import NodesTimeoutError
from ray_release.logger import logger
from ray_release.util import run_with_timeout
from release.ray_release.command_runner.command_runner import CommandRunner


class ClientRunner(CommandRunner):
    def wait_for_nodes(self, num_nodes: int, timeout: float = 900):
        ray_address = self.cluster_launcher.get_cluster_address()
        if ray.is_initialized:
            ray.shutdown()

        def _wait():
            ray.init(address=ray_address)
            while len(ray.nodes()) < num_nodes:
                time.sleep(1)
            ray.shutdown()

        def _status_fn(time_elapsed: float):
            logger.info(
                f"Waiting for nodes to come up: "
                f"{len(ray.nodes())}/{num_nodes} "
                f"({time_elapsed} seconds, timeout: {timeout} seconds).")

        def _error_fn():
            raise NodesTimeoutError(
                f"Only {len(ray.nodes())}/{num_nodes} are up after "
                f"{timeout} seconds.")

        run_with_timeout(
            _wait, timeout=timeout, status_fn=_status_fn, error_fn=_error_fn)
