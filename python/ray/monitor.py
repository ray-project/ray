import argparse
import logging
import os
import time
import traceback
import json

import ray
from ray.autoscaler.autoscaler import LoadMetrics, StandardAutoscaler
import ray.gcs_utils
import ray.utils
import ray.ray_constants as ray_constants
from ray.utils import (binary_to_hex, binary_to_object_id, binary_to_task_id,
                       hex_to_binary, setup_logger)
from ray.autoscaler.commands import teardown_cluster

logger = logging.getLogger(__name__)


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
        self.redis = ray.services.create_redis_client(
            redis_address, password=redis_password)
        # Setup subscriptions to the primary Redis server and the Redis shards.
        self.primary_subscribe_client = self.redis.pubsub(
            ignore_subscribe_messages=True)
        # Keep a mapping from raylet client ID to IP address to use
        # for updating the load metrics.
        self.raylet_id_to_ip_map = {}
        self.load_metrics = LoadMetrics()
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
        self.primary_subscribe_client.close()

    def subscribe(self, channel):
        """Subscribe to the given channel on the primary Redis shard.

        Args:
            channel (str): The channel to subscribe to.

        Raises:
            Exception: An exception is raised if the subscription fails.
        """
        self.primary_subscribe_client.subscribe(channel)

    def xray_heartbeat_batch_handler(self, unused_channel, data):
        """Handle an xray heartbeat batch message from Redis."""

        gcs_entries = ray.gcs_utils.GcsEntry.FromString(data)
        heartbeat_data = gcs_entries.entries[0]

        message = ray.gcs_utils.HeartbeatBatchTableData.FromString(
            heartbeat_data)
        for heartbeat_message in message.batch:
            resource_load = dict(
                zip(heartbeat_message.resource_load_label,
                    heartbeat_message.resource_load_capacity))
            total_resources = dict(
                zip(heartbeat_message.resources_total_label,
                    heartbeat_message.resources_total_capacity))
            available_resources = dict(
                zip(heartbeat_message.resources_available_label,
                    heartbeat_message.resources_available_capacity))
            for resource in total_resources:
                available_resources.setdefault(resource, 0.0)

            # Update the load metrics for this raylet.
            client_id = ray.utils.binary_to_hex(heartbeat_message.client_id)
            ip = self.raylet_id_to_ip_map.get(client_id)
            if ip:
                self.load_metrics.update(ip, total_resources,
                                         available_resources, resource_load)
            else:
                logger.warning(
                    "Monitor: "
                    "could not find ip for client {}".format(client_id))

    def _xray_clean_up_entries_for_job(self, job_id):
        """Remove this job's object/task entries from redis.

        Removes control-state entries of all tasks and task return
        objects belonging to the driver.

        Args:
            job_id: The job id.
        """

        xray_task_table_prefix = (
            ray.gcs_utils.TablePrefix_RAYLET_TASK_string.encode("ascii"))
        xray_object_table_prefix = (
            ray.gcs_utils.TablePrefix_OBJECT_string.encode("ascii"))

        task_table_objects = ray.tasks()
        job_id_hex = binary_to_hex(job_id)
        job_task_id_bins = set()
        for task_id_hex, task_info in task_table_objects.items():
            task_table_object = task_info["TaskSpec"]
            task_job_id_hex = task_table_object["JobID"]
            if job_id_hex != task_job_id_hex:
                # Ignore tasks that aren't from this driver.
                continue
            job_task_id_bins.add(hex_to_binary(task_id_hex))

        # Get objects associated with the driver.
        object_table_objects = ray.objects()
        job_object_id_bins = set()
        for object_id, _ in object_table_objects.items():
            task_id_bin = ray._raylet.compute_task_id(object_id).binary()
            if task_id_bin in job_task_id_bins:
                job_object_id_bins.add(object_id.binary())

        def to_shard_index(id_bin):
            if len(id_bin) == ray.TaskID.size():
                return binary_to_task_id(id_bin).redis_shard_hash() % len(
                    ray.state.state.redis_clients)
            else:
                return binary_to_object_id(id_bin).redis_shard_hash() % len(
                    ray.state.state.redis_clients)

        # Form the redis keys to delete.
        sharded_keys = [[] for _ in range(len(ray.state.state.redis_clients))]
        for task_id_bin in job_task_id_bins:
            sharded_keys[to_shard_index(task_id_bin)].append(
                xray_task_table_prefix + task_id_bin)
        for object_id_bin in job_object_id_bins:
            sharded_keys[to_shard_index(object_id_bin)].append(
                xray_object_table_prefix + object_id_bin)

        # Remove with best effort.
        for shard_index in range(len(sharded_keys)):
            keys = sharded_keys[shard_index]
            if len(keys) == 0:
                continue
            redis = ray.state.state.redis_clients[shard_index]
            num_deleted = redis.delete(*keys)
            logger.info("Monitor: "
                        "Removed {} dead redis entries of the "
                        "driver from redis shard {}.".format(
                            num_deleted, shard_index))
            if num_deleted != len(keys):
                logger.warning("Monitor: "
                               "Failed to remove {} relevant redis "
                               "entries from redis shard {}.".format(
                                   len(keys) - num_deleted, shard_index))

    def xray_job_notification_handler(self, unused_channel, data):
        """Handle a notification that a job has been added or removed.

        Args:
            unused_channel: The message channel.
            data: The message data.
        """
        gcs_entries = ray.gcs_utils.GcsEntry.FromString(data)
        job_data = gcs_entries.entries[0]
        message = ray.gcs_utils.JobTableData.FromString(job_data)
        job_id = message.job_id
        if message.is_dead:
            logger.info("Monitor: "
                        "XRay Driver {} has been removed.".format(
                            binary_to_hex(job_id)))
            self._xray_clean_up_entries_for_job(job_id)

    def autoscaler_resource_request_handler(self, _, data):
        """Handle a notification of a resource request for the autoscaler.

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
                message = subscribe_client.get_message()
                if message is None:
                    # Continue on to the next subscribe client.
                    break

                # Parse the message.
                channel = message["channel"]
                data = message["data"]

                # Determine the appropriate message handler.
                if channel == ray.gcs_utils.XRAY_HEARTBEAT_BATCH_CHANNEL:
                    # Similar functionality as raylet info channel
                    message_handler = self.xray_heartbeat_batch_handler
                elif channel == ray.gcs_utils.XRAY_JOB_CHANNEL:
                    # Handles driver death.
                    message_handler = self.xray_job_notification_handler
                elif (channel ==
                      ray.ray_constants.AUTOSCALER_RESOURCE_REQUEST_CHANNEL):
                    message_handler = self.autoscaler_resource_request_handler
                else:
                    raise Exception("This code should be unreachable.")

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
        # Initialize the subscription channel.
        self.subscribe(ray.gcs_utils.XRAY_HEARTBEAT_BATCH_CHANNEL)
        self.subscribe(ray.gcs_utils.XRAY_JOB_CHANNEL)

        if self.autoscaler:
            self.subscribe(
                ray.ray_constants.AUTOSCALER_RESOURCE_REQUEST_CHANNEL)

        # TODO(rkn): If there were any dead clients at startup, we should clean
        # up the associated state in the state tables.

        # Handle messages from the subscription channels.
        while True:
            # Update the mapping from raylet client ID to IP address.
            # This is only used to update the load metrics for the autoscaler.
            self.update_raylet_map()

            # Process autoscaling actions
            if self.autoscaler:
                self.autoscaler.update()

            # Process a round of messages.
            self.process_messages()

            # Wait for a heartbeat interval before processing the next round of
            # messages.
            time.sleep(
                ray._config.raylet_heartbeat_timeout_milliseconds() * 1e-3)

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
    args = parser.parse_args()
    setup_logger(args.logging_level, args.logging_format)

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
        redis_client = ray.services.create_redis_client(
            args.redis_address, password=args.redis_password)
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        message = "The monitor failed with the following error:\n{}".format(
            traceback_str)
        ray.utils.push_error_to_driver_through_redis(
            redis_client, ray_constants.MONITOR_DIED_ERROR, message)
        raise e
