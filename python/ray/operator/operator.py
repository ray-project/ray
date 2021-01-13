import logging
import multiprocessing as mp
import os
from typing import Any, Callable, Dict, Optional

from kubernetes.client.exceptions import ApiException
import yaml

from ray._private import services
from ray.autoscaler._private import commands
from ray import monitor
from ray.operator import operator_utils
from ray import ray_constants


class RayCluster():
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = self.config["cluster_name"]
        self.config_path = operator_utils.config_path(self.name)

        self.setup_logging()

        self.subprocess = None  # type: Optional[mp.Process]

    def do_in_subprocess(self,
                         f: Callable[[], None],
                         wait_to_finish: bool = False) -> None:
        # First stop the subprocess if it's alive
        self.clean_up_subprocess()
        # Reinstantiate process with f as target and start.
        self.subprocess = mp.Process(name=self.name, target=f)
        # Kill subprocess if monitor dies
        self.subprocess.daemon = True
        self.subprocess.start()
        if wait_to_finish:
            self.subprocess.join()

    def clean_up_subprocess(self):
        if self.subprocess and self.subprocess.is_alive():
            self.subprocess.terminate()
            self.subprocess.join()

    def create_or_update(self) -> None:
        self.do_in_subprocess(self._create_or_update)

    def _create_or_update(self) -> None:
        self.start_head()
        self.start_monitor()

    def start_head(self) -> None:
        self.write_config()
        self.config = commands.create_or_update_cluster(
            self.config_path,
            override_min_workers=None,
            override_max_workers=None,
            no_restart=False,
            restart_only=False,
            yes=True,
            no_config_cache=True)
        self.write_config()

    def start_monitor(self) -> None:
        ray_head_pod_ip = commands.get_head_node_ip(self.config_path)
        # TODO: Add support for user-specified redis port and password
        redis_address = services.address(ray_head_pod_ip,
                                         ray_constants.DEFAULT_PORT)
        self.mtr = monitor.Monitor(
            redis_address=redis_address,
            autoscaling_config=self.config_path,
            redis_password=ray_constants.REDIS_DEFAULT_PASSWORD,
            prefix_cluster_info=True)
        self.mtr.run()

    def clean_up(self) -> None:
        self.clean_up_subprocess()
        self.clean_up_logging()
        self.delete_config()

    def setup_logging(self) -> None:
        self.handler = logging.StreamHandler()
        self.handler.addFilter(lambda rec: rec.processName == self.name)
        logging_format = ":".join([self.name, ray_constants.LOGGER_FORMAT])
        self.handler.setFormatter(logging.Formatter(logging_format))
        operator_utils.root_logger.addHandler(self.handler)

    def clean_up_logging(self) -> None:
        operator_utils.root_logger.removeHandler(self.handler)

    def write_config(self) -> None:
        with open(self.config_path, "w") as file:
            yaml.dump(self.config, file)

    def delete_config(self) -> None:
        os.remove(self.config_path)


ray_clusters = {}


def cluster_action(cluster_config: Dict[str, Any], event_type: str) -> None:
    cluster_name = cluster_config["cluster_name"]
    if event_type == "ADDED":
        ray_clusters[cluster_name] = RayCluster(cluster_config)
        ray_clusters[cluster_name].create_or_update()
    elif event_type == "MODIFIED":
        ray_clusters[cluster_name].create_or_update()
    elif event_type == "DELETED":
        ray_clusters[cluster_name].clean_up()
        del ray_clusters[cluster_name]


def main() -> None:
    # Make directory for ray cluster configs
    if not os.path.isdir(operator_utils.RAY_CONFIG_DIR):
        os.mkdir(operator_utils.RAY_CONFIG_DIR)
    # Control loop
    cluster_cr_stream = operator_utils.cluster_cr_stream()
    try:
        for event in cluster_cr_stream:
            cluster_cr = event["object"]
            event_type = event["type"]
            cluster_config = operator_utils.cr_to_config(cluster_cr)
            cluster_action(cluster_config, event_type)
    except ApiException as e:
        if e.status == 404:
            raise Exception(
                "Caught a 404 error. Has the RayCluster CRD been created?")
        else:
            raise


if __name__ == "__main__":
    main()
