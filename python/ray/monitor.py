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
from ray.autoscaler._private.load_metrics import LoadMetrics
from ray.autoscaler._private.constants import \
    AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE
import ray.gcs_utils
import ray.utils
import ray.ray_constants as ray_constants
from ray.ray_logging import setup_component_logger
from ray._raylet import GlobalStateAccessor

import redis

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

    # Bound the total number of bundles to 2xMAX_RESOURCE_DEMAND_VECTOR_SIZE.
    # This guarantees the resource demand scheduler bin packing algorithm takes
    # a reasonable amount of time to run.
    return waiting_bundles[:AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE], \
        infeasible_bundles[:AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE]


class Monitor:
    """A monitor for Ray processes.

    The monitor is in charge of cleaning up the tables in the global state
    after processes have died. The monitor is currently not responsible for
    detecting component failures.

    Attributes:
        redis: A connection to the Redis server.
        primary_subscribe_client: A pubsub client for the Redis server.
            This is used to receive notifications about failed components.
    """

    def __init__(self, redis_address, autoscaling_config, redis_password=None):
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
        # Setup subscriptions to the primary Redis server and the Redis shards.
        self.primary_subscribe_client = self.redis.pubsub(
            ignore_subscribe_messages=True)
        # Keep a mapping from raylet client ID to IP address to use
        # for updating the load metrics.
        self.raylet_id_to_ip_map = {}
        head_node_ip = redis_address.split(":")[0]
        self.load_metrics = LoadMetrics(local_ip=head_node_ip)
        if autoscaling_config:
            self.autoscaler = StandardAutoscaler(autoscaling_config,
                                                 self.load_metrics)
            self.autoscaling_config = autoscaling_config
        else:
            self.autoscaler = None
            self.autoscaling_config = None

    def __del__(self):
        """Destruct the monitor object."""
        # We close the pubsub client to avoid leaking file descriptors.
        try:
            primary_subscribe_client = self.primary_subscribe_client
        except AttributeError:
            primary_subscribe_client = None
        if primary_subscribe_client is not None:
            primary_subscribe_client.close()
        if self.global_state_accessor is not None:
            self.global_state_accessor.disconnect()
            self.global_state_accessor = None

    def subscribe(self, channel):
        """Subscribe to the given channel on the primary Redis shard.

        Args:
            channel (str): The channel to subscribe to.

        Raises:
            Exception: An exception is raised if the subscription fails.
        """
        self.primary_subscribe_client.subscribe(channel)

    def update_load_metrics(self):
        """Fetches heartbeat data from GCS and updates load metrics."""

        all_heartbeat = self.global_state_accessor.get_all_heartbeat()
        heartbeat_batch_data = \
            ray.gcs_utils.HeartbeatBatchTableData.FromString(all_heartbeat)
        for heartbeat_message in heartbeat_batch_data.batch:
            resource_load = dict(heartbeat_message.resource_load)
            total_resources = dict(heartbeat_message.resources_total)
            available_resources = dict(heartbeat_message.resources_available)

            waiting_bundles, infeasible_bundles = parse_resource_demands(
                heartbeat_batch_data.resource_load_by_shape)

            pending_placement_groups = list(
                heartbeat_batch_data.placement_group_load.placement_group_data)

            # Update the load metrics for this raylet.
            node_id = ray.utils.binary_to_hex(heartbeat_message.node_id)
            ip = self.raylet_id_to_ip_map.get(node_id)
            if ip:
                self.load_metrics.update(ip, total_resources,
                                         available_resources, resource_load,
                                         waiting_bundles, infeasible_bundles,
                                         pending_placement_groups)
            else:
                logger.warning(
                    f"Monitor: could not find ip for node {node_id}")

    def autoscaler_resource_request_handler(self, _, data):
        """Handle a notification of a resource request for the autoscaler.

        This channel and method are only used by the manual
        `ray.autoscaler.sdk.request_resources` api.

        Args:
            channel: unused
            data: a resource request as JSON, e.g. {"CPU": 1}
        """

        if not self.autoscaler:
            return

        try:
            self.autoscaler.request_resources(json.loads(data))
        except Exception:
            # We don't want this to kill the monitor.
            traceback.print_exc()

    def process_messages(self, max_messages=10000):
        """Process all messages ready in the subscription channels.

        This reads messages from the subscription channels and calls the
        appropriate handlers until there are no messages left.

        Args:
            max_messages: The maximum number of messages to process before
                returning.
        """
        subscribe_clients = [self.primary_subscribe_client]
        for subscribe_client in subscribe_clients:
            for _ in range(max_messages):
                message = None
                try:
                    message = subscribe_client.get_message()
                except redis.exceptions.ConnectionError:
                    pass
                if message is None:
                    # Continue on to the next subscribe client.
                    break

                # Parse the message.
                channel = message["channel"]
                data = message["data"]

                if (channel ==
                        ray.ray_constants.AUTOSCALER_RESOURCE_REQUEST_CHANNEL):
                    message_handler = self.autoscaler_resource_request_handler
                else:
                    assert False, "This code should be unreachable."

                # Call the handler.
                message_handler(channel, data)

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
        """Run the monitor.

        This function loops forever, checking for messages about dead database
        clients and cleaning up state accordingly.
        """

        self.subscribe(ray.ray_constants.AUTOSCALER_RESOURCE_REQUEST_CHANNEL)

        # Handle messages from the subscription channels.
        while True:
            # Process autoscaling actions
            if self.autoscaler:
                # Only used to update the load metrics for the autoscaler.
                self.update_raylet_map()
                self.update_load_metrics()
                self.autoscaler.update()

            # Process a round of messages.
            self.process_messages()

            # Wait for a autoscaler update interval before processing the next
            # round of messages.
            time.sleep(AUTOSCALER_UPDATE_INTERVAL_S)

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
