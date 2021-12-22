import asyncio
import logging
import multiprocessing as mp
import os
import threading
from typing import Any
from typing import Callable
from typing import Dict
from typing import Tuple
from typing import Optional

import kopf
import yaml

import ray.autoscaler._private.monitor as monitor
from ray._private import services
from ray.autoscaler._private import commands
from ray.ray_operator import operator_utils
from ray.ray_operator.operator_utils import STATUS_AUTOSCALING_EXCEPTION
from ray.ray_operator.operator_utils import STATUS_RUNNING
from ray.ray_operator.operator_utils import STATUS_UPDATING
from ray import ray_constants

logger = logging.getLogger(__name__)

# Queue to process cluster status updates.
cluster_status_q = mp.Queue()  # type: mp.Queue[Optional[Tuple[str, str, str]]]


class RayCluster:
    """Manages an autoscaling Ray cluster.

    Attributes:
        config: Autoscaling configuration dict.
        subprocess: The subprocess used to create, update, and monitor the
        Ray cluster.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = self.config["cluster_name"]
        self.namespace = self.config["provider"]["namespace"]

        # Make directory for configs of clusters in the namespace,
        # if the directory doesn't exist already.
        namespace_dir = operator_utils.namespace_dir(self.namespace)
        os.makedirs(namespace_dir, exist_ok=True)

        self.config_path = operator_utils.config_path(
            cluster_namespace=self.namespace, cluster_name=self.name)

        # Monitor subprocess
        # self.subprocess is non-null iff there's an active monitor subprocess
        # or a finished monitor subprocess in need of cleanup.
        self.subprocess = None  # type: Optional[mp.Process]
        # Monitor logs for this cluster will be prefixed by the monitor
        # subprocess name:
        self.subprocess_name = ",".join([self.name, self.namespace])
        self.monitor_stop_event = mp.Event()
        self.setup_logging()

    def create_or_update(self, restart_ray: bool = False) -> None:
        """ Create/update the Ray Cluster and run the monitoring loop, all in a
        subprocess.

        The main function of the Operator is managing the
        subprocesses started by this method.

        Args:
            restart_ray: If True, restarts Ray to recover from failure.
        """
        self.do_in_subprocess(self._create_or_update, args=(restart_ray, ))

    def _create_or_update(self, restart_ray: bool = False) -> None:
        try:
            self.start_head(restart_ray=restart_ray)
            self.start_monitor()
        except Exception:
            # Report failed autoscaler status to trigger cluster restart.
            cluster_status_q.put((self.name, self.namespace,
                                  STATUS_AUTOSCALING_EXCEPTION))
            # `status_handling_loop` will increment the
            # `status.AutoscalerRetries` of the CR. A restart will trigger
            # at the subsequent "MODIFIED" event.
            raise

    def start_head(self, restart_ray: bool = False) -> None:
        self.write_config()
        # Don't restart Ray on head unless recovering from failure.
        no_restart = not restart_ray
        # Create or update cluster head and record config side effects.
        self.config = commands.create_or_update_cluster(
            self.config_path,
            override_min_workers=None,
            override_max_workers=None,
            no_restart=no_restart,
            restart_only=False,
            yes=True,
            no_config_cache=True,
            no_monitor_on_head=True,
        )
        # Write the resulting config for use by the autoscaling monitor:
        self.write_config()

    def start_monitor(self) -> None:
        """Runs the autoscaling monitor."""
        ray_head_pod_ip = commands.get_head_node_ip(self.config_path)
        port = operator_utils.infer_head_port(self.config)
        address = services.address(ray_head_pod_ip, port)
        mtr = monitor.Monitor(
            address,
            autoscaling_config=self.config_path,
            redis_password=ray_constants.REDIS_DEFAULT_PASSWORD,
            prefix_cluster_info=True,
            stop_event=self.monitor_stop_event,
        )
        mtr.run()

    def teardown(self) -> None:
        """Attempt orderly tear-down of Ray processes before RayCluster
        resource deletion."""
        self.do_in_subprocess(self._teardown, args=(), block=True)

    def _teardown(self) -> None:
        commands.teardown_cluster(
            self.config_path,
            yes=True,
            workers_only=False,
            override_cluster_name=None,
            keep_min_workers=False)

    def do_in_subprocess(self,
                         f: Callable[[], None],
                         args: Tuple = (),
                         block: bool = False) -> None:
        # First stop the subprocess if it's alive
        self.clean_up_subprocess()
        # Reinstantiate process with f as target and start.
        self.subprocess = mp.Process(
            name=self.subprocess_name, target=f, args=args, daemon=True)
        self.subprocess.start()
        if block:
            self.subprocess.join()

    def clean_up_subprocess(self):
        """
        Clean up the monitor process.

        Executed when CR for this cluster is "DELETED".
        Executed when Autoscaling monitor is restarted.
        """

        if self.subprocess is None:
            # Nothing to clean.
            return

        # Triggers graceful stop of the monitor loop.
        self.monitor_stop_event.set()
        self.subprocess.join()
        # Clears the event for subsequent runs of the monitor.
        self.monitor_stop_event.clear()
        # Signal completed cleanup.
        self.subprocess = None

    def clean_up(self) -> None:
        """Executed when the CR for this cluster is "DELETED".

        The key thing is to end the monitoring subprocess.
        """
        self.teardown()
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

    def set_config(self, config: Dict[str, Any]) -> None:
        self.config = config

    def write_config(self) -> None:
        """Write config to disk for use by the autoscaling monitor."""
        with open(self.config_path, "w") as file:
            yaml.dump(self.config, file)

    def delete_config(self) -> None:
        try:
            os.remove(self.config_path)
        except OSError:
            log_prefix = ",".join([self.name, self.namespace])
            logger.warning(
                f"{log_prefix}: config path does not exist {self.config_path}")


@kopf.on.startup()
def start_background_worker(memo: kopf.Memo, **_):
    memo.status_handler = threading.Thread(
        target=status_handling_loop, args=(cluster_status_q, ))
    memo.status_handler.start()


@kopf.on.cleanup()
def stop_background_worker(memo: kopf.Memo, **_):
    cluster_status_q.put(None)
    memo.status_handler.join()


def status_handling_loop(queue: mp.Queue):
    # TODO: Status will not be set if Operator restarts after `queue.put`
    # but before `set_status`.
    while True:
        item = queue.get()
        if item is None:
            break

        cluster_name, cluster_namespace, phase = item
        try:
            operator_utils.set_status(cluster_name, cluster_namespace, phase)
        except Exception:
            log_prefix = ",".join([cluster_name, cluster_namespace])
            logger.exception(f"{log_prefix}: Error setting RayCluster status.")


@kopf.on.create("rayclusters")
@kopf.on.update("rayclusters")
@kopf.on.resume("rayclusters")
def create_or_update_cluster(body, name, namespace, logger, memo: kopf.Memo,
                             **kwargs):
    """
    1. On creation of a RayCluster resource, create the Ray cluster.
    2. On update of a RayCluster resource, update the cluster
        without restarting Ray processes,
        unless the Ray head's config is modified.
    3. On operator restart ("resume"), rebuild operator memo state and restart
        the Ray cluster's monitor process, without restarting Ray processes.
    """
    _create_or_update_cluster(body, name, namespace, memo, restart_ray=False)


@kopf.on.field("rayclusters", field="status.autoscalerRetries")
def restart_cluster(body, status, name, namespace, memo: kopf.Memo, **kwargs):
    """On increment of status.autoscalerRetries, restart Ray processes.

    Increment of autoscalerRetries happens when cluster's monitor fails,
    for example due to Ray head failure.
    """
    # Don't act on initialization of status.autoscalerRetries from nil to 0.
    if status.get("autoscalerRetries"):
        # Restart the Ray cluster:
        _create_or_update_cluster(
            body, name, namespace, memo, restart_ray=True)


def _create_or_update_cluster(cluster_cr_body,
                              name,
                              namespace,
                              memo,
                              restart_ray=False):
    """Create, update, or restart the Ray cluster described by a RayCluster
    resource.

    Args:
        cluster_cr_body: The body of the K8s RayCluster resources describing
            a Ray cluster.
        name: The name of the Ray cluster.
        namespace: The K8s namespace in which the Ray cluster runs.
        memo: kopf memo state for this Ray cluster.
        restart_ray: Only restart cluster Ray processes if this is true.
    """
    # Convert the RayCluster custom resource to a Ray autoscaling config.
    cluster_config = operator_utils.cr_to_config(cluster_cr_body)
    # Verify the user didn't set a custom Redis password in Ray start commands.
    # (custom Redis password is not supported by K8s operator.)
    operator_utils.check_redis_password_not_specified(cluster_config, name,
                                                      namespace)

    # Fetch or create the RayCluster python object encapsulating cluster state.
    ray_cluster = memo.get("ray_cluster")
    if ray_cluster is None:
        ray_cluster = RayCluster(cluster_config)
        memo.ray_cluster = ray_cluster

    # Indicate in status.phase that a "create-or-update" is in progress.
    cluster_status_q.put((name, namespace, STATUS_UPDATING))

    # Store the autoscaling config for use by the Ray autoscaler.
    ray_cluster.set_config(cluster_config)

    # Launch a the Ray cluster by SSHing into the pod and running
    # the initialization commands. This will not restart the cluster
    # unless there was a failure.
    ray_cluster.create_or_update(restart_ray=restart_ray)

    # Indicate in status.phase that the head is up and the monitor is running.
    cluster_status_q.put((name, namespace, STATUS_RUNNING))


@kopf.on.delete("rayclusters")
def delete_fn(memo: kopf.Memo, **kwargs):
    ray_cluster = memo.get("ray_cluster")
    if ray_cluster is None:
        return

    ray_cluster.clean_up()


def main():
    if operator_utils.NAMESPACED_OPERATOR:
        kwargs = {"namespaces": [operator_utils.OPERATOR_NAMESPACE]}
    else:
        kwargs = {"clusterwide": True}

    asyncio.run(kopf.operator(**kwargs))


if __name__ == "__main__":
    main()
