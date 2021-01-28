"""Autoscaler monitoring loop daemon."""

import argparse
import logging
import logging.handlers
import os
import time
import traceback
import json

import ray
from ray.autoscaler._private.autoscaler import StandardAutoscaler
from ray.autoscaler._private.commands import teardown_cluster
from ray.autoscaler._private.constants import AUTOSCALER_UPDATE_INTERVAL_S
from ray.autoscaler._private.event_summarizer import EventSummarizer
from ray.autoscaler._private.load_metrics import LoadMetrics
from ray.autoscaler._private.constants import \
    AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE
from ray.autoscaler._private.util import DEBUG_AUTOSCALING_STATUS
import ray.gcs_utils
import ray.utils
import ray.ray_constants as ray_constants
from ray.ray_logging import setup_component_logger
from ray._raylet import GlobalStateAccessor
from ray.experimental.internal_kv import _internal_kv_put, \
    _internal_kv_initialized, _internal_kv_get

logger = logging.getLogger(__name__)


def parse_resource_demands(resource_load_by_shape):
    """Handle the message.resource_load_by_shape protobuf for the demand
    based autoscaling. Catch and log all exceptions so this doesn't
    interfere with the utilization based autoscaler until we're confident
    this is stable. Worker queue backlogs are added to the appropriate
    resource demand vector.

    Args:
        resource_load_by_shape (pb2.gcs.ResourceLoad): The resource demands
            in protobuf form or None.

    Returns:
        List[ResourceDict]: Waiting bundles (ready and feasible).
        List[ResourceDict]: Infeasible bundles.
    """
    waiting_bundles, infeasible_bundles = [], []
    try:
        for resource_demand_pb in list(
                resource_load_by_shape.resource_demands):
            request_shape = dict(resource_demand_pb.shape)
            for _ in range(resource_demand_pb.num_ready_requests_queued):
                waiting_bundles.append(request_shape)
            for _ in range(resource_demand_pb.num_infeasible_requests_queued):
                infeasible_bundles.append(request_shape)

            # Infeasible and ready states for tasks are (logically)
            # mutually exclusive.
            if resource_demand_pb.num_infeasible_requests_queued > 0:
                backlog_queue = infeasible_bundles
            else:
                backlog_queue = waiting_bundles
            for _ in range(resource_demand_pb.backlog_size):
                backlog_queue.append(request_shape)
            if len(waiting_bundles+infeasible_bundles) > \
                    AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE:
                break
    except Exception:
        logger.exception("Failed to parse resource demands.")

    return waiting_bundles, infeasible_bundles


class Monitor:
    """Autoscaling monitor.

    This process periodically collects stats from the GCS and triggers
    autoscaler updates.

    Attributes:
        redis: A connection to the Redis server.
    """

    def __init__(self,
                 redis_address,
                 autoscaling_config,
                 redis_password=None,
                 prefix_cluster_info=False):
        # Initialize the Redis clients.
        ray.state.state._initialize_global_state(
            redis_address, redis_password=redis_password)
        self.redis = ray._private.services.create_redis_client(
            redis_address, password=redis_password)
        self.global_state_accessor = GlobalStateAccessor(
            redis_address, redis_password, False)
        self.global_state_accessor.connect()
        # Set the redis client and mode so _internal_kv works for autoscaler.
        worker = ray.worker.global_worker
        worker.redis_client = self.redis
        worker.mode = 0
        # Keep a mapping from raylet client ID to IP address to use
        # for updating the load metrics.
        self.raylet_id_to_ip_map = {}
        head_node_ip = redis_address.split(":")[0]
        self.load_metrics = LoadMetrics(local_ip=head_node_ip)
        self.last_avail_resources = None
        self.event_summarizer = EventSummarizer()
        if autoscaling_config:
            self.autoscaler = StandardAutoscaler(
                autoscaling_config,
                self.load_metrics,
                prefix_cluster_info=prefix_cluster_info,
                event_summarizer=self.event_summarizer)
            self.autoscaling_config = autoscaling_config
        else:
            self.autoscaler = None
            self.autoscaling_config = None

        logger.info("Monitor: Started")

    def __del__(self):
        """Destruct the monitor object."""
        # We close the pubsub client to avoid leaking file descriptors.
        if self.global_state_accessor is not None:
            self.global_state_accessor.disconnect()
            self.global_state_accessor = None

    def update_load_metrics(self):
        """Fetches resource usage data from GCS and updates load metrics."""

        all_resources = self.global_state_accessor.get_all_resource_usage()
        resources_batch_data = \
            ray.gcs_utils.ResourceUsageBatchData.FromString(all_resources)
        for resource_message in resources_batch_data.batch:
            resource_load = dict(resource_message.resource_load)
            total_resources = dict(resource_message.resources_total)
            available_resources = dict(resource_message.resources_available)

            waiting_bundles, infeasible_bundles = parse_resource_demands(
                resources_batch_data.resource_load_by_shape)

            pending_placement_groups = list(
                resources_batch_data.placement_group_load.placement_group_data)

            # Update the load metrics for this raylet.
            node_id = ray.utils.binary_to_hex(resource_message.node_id)
            ip = self.raylet_id_to_ip_map.get(node_id)
            if ip:
                self.load_metrics.update(ip, total_resources,
                                         available_resources, resource_load,
                                         waiting_bundles, infeasible_bundles,
                                         pending_placement_groups)
            else:
                logger.warning(
                    f"Monitor: could not find ip for node {node_id}")

    def update_resource_requests(self):
        """Fetches resource requests from the internal KV and updates load."""
        if not _internal_kv_initialized():
            return
        data = _internal_kv_get(
            ray.ray_constants.AUTOSCALER_RESOURCE_REQUEST_CHANNEL)
        if data:
            try:
                resource_request = json.loads(data)
                self.load_metrics.set_resource_requests(resource_request)
            except Exception:
                logger.exception("Error parsing resource requests")

    def update_raylet_map(self, _append_port=False):
        """Updates internal raylet map.

        Args:
            _append_port (bool): Defaults to False. Appending the port is
                useful in testing, as mock clusters have many nodes with
                the same IP and cannot be uniquely identified.
        """
        all_raylet_nodes = ray.nodes()
        self.raylet_id_to_ip_map = {}
        for raylet_info in all_raylet_nodes:
            node_id = (raylet_info.get("DBClientID") or raylet_info["NodeID"])
            ip_address = (raylet_info.get("AuxAddress")
                          or raylet_info["NodeManagerAddress"]).split(":")[0]
            if _append_port:
                ip_address += ":" + str(raylet_info["NodeManagerPort"])
            self.raylet_id_to_ip_map[node_id] = ip_address

    def _run(self):
        """Run the monitor loop."""

        while True:
            self.update_raylet_map()
            self.update_load_metrics()
            self.update_resource_requests()
            self.update_event_summary()
            status = {
                "load_metrics_report": self.load_metrics.summary()._asdict()
            }

            # Process autoscaling actions
            if self.autoscaler:
                # Only used to update the load metrics for the autoscaler.
                self.autoscaler.update()
                status[
                    "autoscaler_report"] = self.autoscaler.summary()._asdict()

                for msg in self.event_summarizer.summary():
                    logger.info(":event_summary:{}".format(msg))
                self.event_summarizer.clear()

            as_json = json.dumps(status)
            if _internal_kv_initialized():
                _internal_kv_put(
                    DEBUG_AUTOSCALING_STATUS, as_json, overwrite=True)

            # Wait for a autoscaler update interval before processing the next
            # round of messages.
            time.sleep(AUTOSCALER_UPDATE_INTERVAL_S)

    def update_event_summary(self):
        """Report the current size of the cluster.

        To avoid log spam, only cluster size changes (CPU or GPU count change)
        are reported to the event summarizer. The event summarizer will report
        only the latest cluster size per batch.
        """
        avail_resources = self.load_metrics.resources_avail_summary()
        if avail_resources != self.last_avail_resources:
            self.event_summarizer.add(
                "Resized to {}.",  # e.g., Resized to 100 CPUs, 4 GPUs.
                quantity=avail_resources,
                aggregate=lambda old, new: new)
            self.last_avail_resources = avail_resources

    def destroy_autoscaler_workers(self):
        """Cleanup the autoscaler, in case of an exception in the run() method.

        We kill the worker nodes, but retain the head node in order to keep
        logs around, keeping costs minimal. This monitor process runs on the
        head node anyway, so this is more reliable."""

        if self.autoscaler is None:
            return  # Nothing to clean up.

        if self.autoscaling_config is None:
            # This is a logic error in the program. Can't do anything.
            logger.error(
                "Monitor: Cleanup failed due to lack of autoscaler config.")
            return

        logger.info("Monitor: Exception caught. Taking down workers...")
        clean = False
        while not clean:
            try:
                teardown_cluster(
                    config_file=self.autoscaling_config,
                    yes=True,  # Non-interactive.
                    workers_only=True,  # Retain head node for logs.
                    override_cluster_name=None,
                    keep_min_workers=True,  # Retain minimal amount of workers.
                )
                clean = True
                logger.info("Monitor: Workers taken down.")
            except Exception:
                logger.error("Monitor: Cleanup exception. Trying again...")
                time.sleep(2)

    def run(self):
        try:
            self._run()
        except Exception:
            logger.exception("Error in monitor loop")
            if self.autoscaler:
                self.autoscaler.kill_workers()
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Parse Redis server for the "
                     "monitor to connect to."))
    parser.add_argument(
        "--redis-address",
        required=True,
        type=str,
        help="the address to use for Redis")
    parser.add_argument(
        "--autoscaling-config",
        required=False,
        type=str,
        help="the path to the autoscaling config file")
    parser.add_argument(
        "--redis-password",
        required=False,
        type=str,
        default=None,
        help="the password to use for Redis")
    parser.add_argument(
        "--logging-level",
        required=False,
        type=str,
        default=ray_constants.LOGGER_LEVEL,
        choices=ray_constants.LOGGER_LEVEL_CHOICES,
        help=ray_constants.LOGGER_LEVEL_HELP)
    parser.add_argument(
        "--logging-format",
        required=False,
        type=str,
        default=ray_constants.LOGGER_FORMAT,
        help=ray_constants.LOGGER_FORMAT_HELP)
    parser.add_argument(
        "--logging-filename",
        required=False,
        type=str,
        default=ray_constants.MONITOR_LOG_FILE_NAME,
        help="Specify the name of log file, "
        "log to stdout if set empty, default is "
        f"\"{ray_constants.MONITOR_LOG_FILE_NAME}\"")
    parser.add_argument(
        "--logs-dir",
        required=True,
        type=str,
        help="Specify the path of the temporary directory used by Ray "
        "processes.")
    parser.add_argument(
        "--logging-rotate-bytes",
        required=False,
        type=int,
        default=ray_constants.LOGGING_ROTATE_BYTES,
        help="Specify the max bytes for rotating "
        "log file, default is "
        f"{ray_constants.LOGGING_ROTATE_BYTES} bytes.")
    parser.add_argument(
        "--logging-rotate-backup-count",
        required=False,
        type=int,
        default=ray_constants.LOGGING_ROTATE_BACKUP_COUNT,
        help="Specify the backup count of rotated log file, default is "
        f"{ray_constants.LOGGING_ROTATE_BACKUP_COUNT}.")
    args = parser.parse_args()
    setup_component_logger(
        logging_level=args.logging_level,
        logging_format=args.logging_format,
        log_dir=args.logs_dir,
        filename=args.logging_filename,
        max_bytes=args.logging_rotate_bytes,
        backup_count=args.logging_rotate_backup_count)

    if args.autoscaling_config:
        autoscaling_config = os.path.expanduser(args.autoscaling_config)
    else:
        autoscaling_config = None

    monitor = Monitor(
        args.redis_address,
        autoscaling_config,
        redis_password=args.redis_password)

    try:
        monitor.run()
    except Exception as e:
        # Take down autoscaler workers if necessary.
        monitor.destroy_autoscaler_workers()

        # Something went wrong, so push an error to all drivers.
        redis_client = ray._private.services.create_redis_client(
            args.redis_address, password=args.redis_password)
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        message = ("The monitor failed with the "
                   f"following error:\n{traceback_str}")
        ray.utils.push_error_to_driver_through_redis(
            redis_client, ray_constants.MONITOR_DIED_ERROR, message)
        raise e
