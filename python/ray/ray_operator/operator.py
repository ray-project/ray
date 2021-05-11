import logging
import multiprocessing as mp
import os
import time
import threading
from typing import Any
from typing import Callable
from typing import Dict
from typing import Tuple
from typing import Optional

from kubernetes.client.exceptions import ApiException
import yaml

import ray.autoscaler._private.monitor as monitor
from ray._private import services
from ray.autoscaler._private import commands
from ray.ray_operator import operator_utils
from ray import ray_constants

logger = logging.getLogger(__name__)

# Queue to process cluster status updates.
cluster_status_q = mp.Queue()  # type: mp.Queue[Tuple[str, str, str]]


class RayCluster():
    def __init__(self, config: Dict[str, Any]):
        self.set_config(config)
        self.name = self.config["cluster_name"]
        self.namespace = self.config["provider"]["namespace"]

        # Make directory for configs of clusters in the namespace,
        # if the director doesn't exist already.
        namespace_dir = operator_utils.namespace_dir(self.namespace)
        if not os.path.isdir(namespace_dir):
            os.mkdir(namespace_dir)
        self.config_path = operator_utils.config_path(
            cluster_namespace=self.namespace, cluster_name=self.name)

        # Tracks metadata.generation field of associated custom resource.
        # K8s increments this field whenever the spec of the custom resource is
        # updated.
        self._generation = 0

        self.subprocess = None  # type: Optional[mp.Process]
        # Monitor logs for this cluster will be prefixed by the monitor
        # subprocess name:
        self.subprocess_name = ",".join([self.name, self.namespace])
        self.monitor_stop_event = mp.Event()

        self.setup_logging()

    def set_config(self, config: Dict[str, Any]) -> None:
        self.config = config

    def set_generation(self, generation: int) -> None:
        self._generation = generation

    def get_generation(self) -> int:
        return self._generation

    def do_in_subprocess(self, f: Callable[[], None]) -> None:
        # First stop the subprocess if it's alive
        self.clean_up_subprocess()
        # Reinstantiate process with f as target and start.
        self.subprocess = mp.Process(
            name=self.subprocess_name, target=f, daemon=True)
        self.subprocess.start()

    def clean_up_subprocess(self):
        if self.subprocess and self.subprocess.is_alive():
            self.monitor_stop_event.set()
            self.subprocess.join()
            self.monitor_stop_event.clear()

    def create_or_update(self) -> None:
        self.do_in_subprocess(self._create_or_update)

    def _create_or_update(self) -> None:
        try:
            self.start_head()
            self.start_monitor()
        except Exception:
            cluster_status_q.put((self.name, self.namespace, "Error"))
            raise

    def start_head(self) -> None:
        self.write_config()
        self.config = commands.create_or_update_cluster(
            self.config_path,
            override_min_workers=None,
            override_max_workers=None,
            no_restart=False,
            restart_only=False,
            yes=True,
            no_config_cache=True,
            no_monitor_on_head=True)
        self.write_config()

    def start_monitor(self) -> None:
        ray_head_pod_ip = commands.get_head_node_ip(self.config_path)
        port = operator_utils.infer_head_port(self.config)
        redis_address = services.address(ray_head_pod_ip, port)
        self.mtr = monitor.Monitor(
            redis_address=redis_address,
            autoscaling_config=self.config_path,
            redis_password=ray_constants.REDIS_DEFAULT_PASSWORD,
            prefix_cluster_info=True,
            stop_event=self.monitor_stop_event)
        self.mtr.run()

    def clean_up(self) -> None:
        self.clean_up_subprocess()
        self.clean_up_logging()
        self.delete_config()

    def setup_logging(self) -> None:
        """Add a log handler which appends the name and namespace of this
        cluster to the cluster's monitor logs.
        """
        self.handler = logging.StreamHandler()
        # Filter by subprocess name to get this cluster's monitor logs.
        self.handler.addFilter(
            lambda rec: rec.processName == self.subprocess_name)
        # Lines start with "<cluster name>,<cluster namespace>:"
        logging_format = ":".join(
            [self.subprocess_name, ray_constants.LOGGER_FORMAT])
        self.handler.setFormatter(logging.Formatter(logging_format))
        operator_utils.root_logger.addHandler(self.handler)

    def clean_up_logging(self) -> None:
        operator_utils.root_logger.removeHandler(self.handler)

    def write_config(self) -> None:
        with open(self.config_path, "w") as file:
            yaml.dump(self.config, file)

    def delete_config(self) -> None:
        os.remove(self.config_path)


# Maps ray cluster (name, namespace) pairs to RayCluster python objects.
ray_clusters = {}  # type: Dict[Tuple[str, str], RayCluster]


def run_event_loop():
    # Instantiate event stream.
    if operator_utils.NAMESPACED_OPERATOR:
        raycluster_cr_stream = operator_utils.namespaced_cr_stream(
            namespace=operator_utils.OPERATOR_NAMESPACE)
    else:
        raycluster_cr_stream = operator_utils.cluster_scoped_cr_stream()

    # Run control loop.
    for event in raycluster_cr_stream:
        cluster_cr = event["object"]
        cluster_name = cluster_cr["metadata"]["name"]
        cluster_namespace = cluster_cr["metadata"]["namespace"]
        event_type = event["type"]
        handle_event(event_type, cluster_cr, cluster_name, cluster_namespace)


def handle_event(event_type, cluster_cr, cluster_name, cluster_namespace):
    # TODO: This only detects errors in the parent process and thus doesn't
    # catch cluster-specific autoscaling failures. Fix that (perhaps at
    # the same time that we eliminate subprocesses).
    try:
        cluster_action(event_type, cluster_cr, cluster_name, cluster_namespace)
    except Exception:
        logger.exception(f"Error while updating RayCluster {cluster_name}.")
        cluster_status_q.put((cluster_name, cluster_namespace, "Error"))


def cluster_action(event_type: str, cluster_cr: Dict[str, Any],
                   cluster_name: str, cluster_namespace: str) -> None:

    cluster_config = operator_utils.cr_to_config(cluster_cr)
    cluster_name = cluster_config["cluster_name"]
    cluster_identifier = (cluster_name, cluster_namespace)

    if event_type == "ADDED":
        operator_utils.check_redis_password_not_specified(
            cluster_config, cluster_identifier)

        cluster_status_q.put((cluster_name, cluster_namespace, "Running"))

        ray_cluster = RayCluster(cluster_config)

        # Track changes to the custom resource's spec field:
        generation = cluster_cr["metadata"]["generation"]
        ray_cluster.set_generation(generation)

        ray_cluster.create_or_update()

        ray_clusters[cluster_identifier] = ray_cluster

    elif event_type == "MODIFIED":
        ray_cluster = ray_clusters[cluster_identifier]
        # Check metadata.generation to determine if there's a spec change.
        current_generation = cluster_cr["metadata"]["generation"]
        # Only update if there's been a change to the spec.
        if current_generation > ray_cluster.get_generation():
            ray_cluster.set_generation(current_generation)
            ray_cluster.set_config(cluster_config)
            ray_cluster.create_or_update()

    elif event_type == "DELETED":
        ray_cluster = ray_clusters[cluster_identifier]
        ray_cluster.clean_up()
        del ray_clusters[cluster_identifier]


def status_handling_loop():
    while True:
        cluster_name, cluster_namespace, status = cluster_status_q.get()
        operator_utils.set_status(cluster_name, cluster_namespace, status)


def main() -> None:
    # Run status-handling loop.
    status_handler = threading.Thread(target=status_handling_loop, daemon=True)
    status_handler.start()

    # Make directory for Ray cluster configs
    if not os.path.isdir(operator_utils.RAY_CONFIG_DIR):
        os.mkdir(operator_utils.RAY_CONFIG_DIR)

    while True:
        # This outer loop waits for creation of a RayCluster CRD if it hasn't
        # already been created.
        try:
            # Enter main event loop.
            run_event_loop()
        except ApiException as e:
            if e.status == 404:
                logger.warning("Waiting for creation of the RayCluster CRD")
                time.sleep(5)
            else:
                logger.error("Failed to enter operator event loop.")
                # Unforeseen startup error. Operator pod is
                # likely to end up in a crash loop.
                raise


if __name__ == "__main__":
    main()
