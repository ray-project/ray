from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import os
import time
import traceback

import redis

import ray
from ray.autoscaler.autoscaler import LoadMetrics, StandardAutoscaler
import ray.cloudpickle as pickle
import ray.gcs_utils
import ray.utils
import ray.ray_constants as ray_constants
from ray.services import get_ip_address, get_port
from ray.utils import (binary_to_hex, binary_to_object_id, hex_to_binary,
                       setup_logger)

logger = logging.getLogger(__name__)


class Monitor(object):
    """A monitor for Ray processes.

    The monitor is in charge of cleaning up the tables in the global state
    after processes have died. The monitor is currently not responsible for
    detecting component failures.

    Attributes:
        redis: A connection to the Redis server.
        subscribe_client: A pubsub client for the Redis server. This is used to
            receive notifications about failed components.
    """

    def __init__(self, redis_address, autoscaling_config, redis_password=None):
        # Initialize the Redis clients.
        self.state = ray.experimental.state.GlobalState()
        redis_ip_address = get_ip_address(args.redis_address)
        redis_port = get_port(args.redis_address)
        self.state._initialize_global_state(
            redis_ip_address, redis_port, redis_password=redis_password)
        self.redis = ray.services.create_redis_client(
            redis_address, password=redis_password)
        # Setup subscriptions to the primary Redis server and the Redis shards.
        self.primary_subscribe_client = self.redis.pubsub(
            ignore_subscribe_messages=True)
        # Keep a mapping from local scheduler client ID to IP address to use
        # for updating the load metrics.
        self.local_scheduler_id_to_ip_map = {}
        self.load_metrics = LoadMetrics()
        if autoscaling_config:
            self.autoscaler = StandardAutoscaler(autoscaling_config,
                                                 self.load_metrics)
        else:
            self.autoscaler = None

        # Experimental feature: GCS flushing.
        self.issue_gcs_flushes = "RAY_USE_NEW_GCS" in os.environ
        self.gcs_flush_policy = None
        if self.issue_gcs_flushes:
            # Data is stored under the first data shard, so we issue flushes to
            # that redis server.
            addr_port = self.redis.lrange("RedisShards", 0, -1)
            if len(addr_port) > 1:
                logger.warning(
                    "Monitor: "
                    "TODO: if launching > 1 redis shard, flushing needs to "
                    "touch shards in parallel.")
                self.issue_gcs_flushes = False
            else:
                addr_port = addr_port[0].split(b":")
                self.redis_shard = redis.StrictRedis(
                    host=addr_port[0],
                    port=addr_port[1],
                    password=redis_password)
                try:
                    self.redis_shard.execute_command("HEAD.FLUSH 0")
                except redis.exceptions.ResponseError as e:
                    logger.info(
                        "Monitor: "
                        "Turning off flushing due to exception: {}".format(
                            str(e)))
                    self.issue_gcs_flushes = False

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

        gcs_entries = ray.gcs_utils.GcsTableEntry.GetRootAsGcsTableEntry(
            data, 0)
        heartbeat_data = gcs_entries.Entries(0)

        message = (ray.gcs_utils.HeartbeatBatchTableData.
                   GetRootAsHeartbeatBatchTableData(heartbeat_data, 0))

        for j in range(message.BatchLength()):
            heartbeat_message = message.Batch(j)

            num_resources = heartbeat_message.ResourcesAvailableLabelLength()
            static_resources = {}
            dynamic_resources = {}
            for i in range(num_resources):
                dyn = heartbeat_message.ResourcesAvailableLabel(i)
                static = heartbeat_message.ResourcesTotalLabel(i)
                dynamic_resources[dyn] = (
                    heartbeat_message.ResourcesAvailableCapacity(i))
                static_resources[static] = (
                    heartbeat_message.ResourcesTotalCapacity(i))

            # Update the load metrics for this local scheduler.
            client_id = ray.utils.binary_to_hex(heartbeat_message.ClientId())
            ip = self.local_scheduler_id_to_ip_map.get(client_id)
            if ip:
                self.load_metrics.update(ip, static_resources,
                                         dynamic_resources)
            else:
                logger.warning(
                    "Monitor: "
                    "could not find ip for client {}".format(client_id))

    def _xray_clean_up_entries_for_driver(self, driver_id):
        """Remove this driver's object/task entries from redis.

        Removes control-state entries of all tasks and task return
        objects belonging to the driver.

        Args:
            driver_id: The driver id.
        """

        xray_task_table_prefix = (
            ray.gcs_utils.TablePrefix_RAYLET_TASK_string.encode("ascii"))
        xray_object_table_prefix = (
            ray.gcs_utils.TablePrefix_OBJECT_string.encode("ascii"))

        task_table_objects = self.state.task_table()
        driver_id_hex = binary_to_hex(driver_id)
        driver_task_id_bins = set()
        for task_id_hex, task_info in task_table_objects.items():
            task_table_object = task_info["TaskSpec"]
            task_driver_id_hex = task_table_object["DriverID"]
            if driver_id_hex != task_driver_id_hex:
                # Ignore tasks that aren't from this driver.
                continue
            driver_task_id_bins.add(hex_to_binary(task_id_hex))

        # Get objects associated with the driver.
        object_table_objects = self.state.object_table()
        driver_object_id_bins = set()
        for object_id, _ in object_table_objects.items():
            task_id_bin = ray._raylet.compute_task_id(object_id).binary()
            if task_id_bin in driver_task_id_bins:
                driver_object_id_bins.add(object_id.binary())

        def to_shard_index(id_bin):
            return binary_to_object_id(id_bin).redis_shard_hash() % len(
                self.state.redis_clients)

        # Form the redis keys to delete.
        sharded_keys = [[] for _ in range(len(self.state.redis_clients))]
        for task_id_bin in driver_task_id_bins:
            sharded_keys[to_shard_index(task_id_bin)].append(
                xray_task_table_prefix + task_id_bin)
        for object_id_bin in driver_object_id_bins:
            sharded_keys[to_shard_index(object_id_bin)].append(
                xray_object_table_prefix + object_id_bin)

        # Remove with best effort.
        for shard_index in range(len(sharded_keys)):
            keys = sharded_keys[shard_index]
            if len(keys) == 0:
                continue
            redis = self.state.redis_clients[shard_index]
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

    def xray_driver_removed_handler(self, unused_channel, data):
        """Handle a notification that a driver has been removed.

        Args:
            unused_channel: The message channel.
            data: The message data.
        """
        gcs_entries = ray.gcs_utils.GcsTableEntry.GetRootAsGcsTableEntry(
            data, 0)
        driver_data = gcs_entries.Entries(0)
        message = ray.gcs_utils.DriverTableData.GetRootAsDriverTableData(
            driver_data, 0)
        driver_id = message.DriverId()
        logger.info("Monitor: "
                    "XRay Driver {} has been removed.".format(
                        binary_to_hex(driver_id)))
        self._xray_clean_up_entries_for_driver(driver_id)

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
                    # Similar functionality as local scheduler info channel
                    message_handler = self.xray_heartbeat_batch_handler
                elif channel == ray.gcs_utils.XRAY_DRIVER_CHANNEL:
                    # Handles driver death.
                    message_handler = self.xray_driver_removed_handler
                else:
                    raise Exception("This code should be unreachable.")

                # Call the handler.
                message_handler(channel, data)

    def update_local_scheduler_map(self):
        local_schedulers = self.state.client_table()
        self.local_scheduler_id_to_ip_map = {}
        for local_scheduler_info in local_schedulers:
            client_id = local_scheduler_info.get("DBClientID") or \
                local_scheduler_info["ClientID"]
            ip_address = (
                local_scheduler_info.get("AuxAddress")
                or local_scheduler_info["NodeManagerAddress"]).split(":")[0]
            self.local_scheduler_id_to_ip_map[client_id] = ip_address

    def _maybe_flush_gcs(self):
        """Experimental: issue a flush request to the GCS.

        The purpose of this feature is to control GCS memory usage.

        To activate this feature, Ray must be compiled with the flag
        RAY_USE_NEW_GCS set, and Ray must be started at run time with the flag
        as well.
        """
        if not self.issue_gcs_flushes:
            return
        if self.gcs_flush_policy is None:
            serialized = self.redis.get("gcs_flushing_policy")
            if serialized is None:
                # Client has not set any policy; by default flushing is off.
                return
            self.gcs_flush_policy = pickle.loads(serialized)

        if not self.gcs_flush_policy.should_flush(self.redis_shard):
            return

        max_entries_to_flush = self.gcs_flush_policy.num_entries_to_flush()
        num_flushed = self.redis_shard.execute_command(
            "HEAD.FLUSH {}".format(max_entries_to_flush))
        logger.info("Monitor: num_flushed {}".format(num_flushed))

        # This flushes event log and log files.
        ray.experimental.flush_redis_unsafe(self.redis)

        self.gcs_flush_policy.record_flush()

    def run(self):
        """Run the monitor.

        This function loops forever, checking for messages about dead database
        clients and cleaning up state accordingly.
        """
        # Initialize the subscription channel.
        self.subscribe(ray.gcs_utils.XRAY_HEARTBEAT_BATCH_CHANNEL)
        self.subscribe(ray.gcs_utils.XRAY_DRIVER_CHANNEL)

        # TODO(rkn): If there were any dead clients at startup, we should clean
        # up the associated state in the state tables.

        # Handle messages from the subscription channels.
        while True:
            # Update the mapping from local scheduler client ID to IP address.
            # This is only used to update the load metrics for the autoscaler.
            self.update_local_scheduler_map()

            # Process autoscaling actions
            if self.autoscaler:
                self.autoscaler.update()

            self._maybe_flush_gcs()

            # Process a round of messages.
            self.process_messages()

            # Wait for a heartbeat interval before processing the next round of
            # messages.
            time.sleep(ray._config.heartbeat_timeout_milliseconds() * 1e-3)

        # TODO(rkn): This infinite loop should be inside of a try/except block,
        # and if an exception is thrown we should push an error message to all
        # drivers.


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
        # Something went wrong, so push an error to all drivers.
        redis_client = ray.services.create_redis_client(
            args.redis_address, password=args.redis_password)
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        message = "The monitor failed with the following error:\n{}".format(
            traceback_str)
        ray.utils.push_error_to_driver_through_redis(
            redis_client, ray_constants.MONITOR_DIED_ERROR, message)
        raise e
