from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import binascii
import logging
import os
import time
from collections import Counter, defaultdict

import ray
import ray.utils
import redis
# Import flatbuffer bindings.
from ray.core.generated.DriverTableMessage import DriverTableMessage
from ray.core.generated.LocalSchedulerInfoMessage import \
    LocalSchedulerInfoMessage
from ray.core.generated.SubscribeToDBClientTableReply import \
    SubscribeToDBClientTableReply
from ray.autoscaler.autoscaler import LoadMetrics, StandardAutoscaler
from ray.core.generated.TaskInfo import TaskInfo
from ray.services import get_ip_address, get_port
from ray.utils import binary_to_hex, binary_to_object_id, hex_to_binary
from ray.worker import NIL_ACTOR_ID

# These variables must be kept in sync with the C codebase.
# common/common.h
DB_CLIENT_ID_SIZE = 20
NIL_ID = b"\xff" * DB_CLIENT_ID_SIZE

# common/task.h
TASK_STATUS_LOST = 32

# common/state/redis.cc
LOCAL_SCHEDULER_INFO_CHANNEL = b"local_schedulers"
PLASMA_MANAGER_HEARTBEAT_CHANNEL = b"plasma_managers"
DRIVER_DEATH_CHANNEL = b"driver_deaths"

# common/redis_module/ray_redis_module.cc
OBJECT_INFO_PREFIX = b"OI:"
OBJECT_LOCATION_PREFIX = b"OL:"
TASK_TABLE_PREFIX = b"TT:"
DB_CLIENT_PREFIX = b"CL:"
DB_CLIENT_TABLE_NAME = b"db_clients"

# local_scheduler/local_scheduler.h
LOCAL_SCHEDULER_CLIENT_TYPE = b"local_scheduler"

# plasma/plasma_manager.cc
PLASMA_MANAGER_CLIENT_TYPE = b"plasma_manager"

# Set up logging.
logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.INFO)


class Monitor(object):
    """A monitor for Ray processes.

    The monitor is in charge of cleaning up the tables in the global state
    after processes have died. The monitor is currently not responsible for
    detecting component failures.

    Attributes:
        redis: A connection to the Redis server.
        subscribe_client: A pubsub client for the Redis server. This is used to
            receive notifications about failed components.
        subscribed: A dictionary mapping channel names (str) to whether or not
            the subscription to that channel has succeeded yet (bool).
        dead_local_schedulers: A set of the local scheduler IDs of all of the
            local schedulers that were up at one point and have died since
            then.
        live_plasma_managers: A counter mapping live plasma manager IDs to the
            number of heartbeats that have passed since we last heard from that
            plasma manager. A plasma manager is live if we received a heartbeat
            from it at any point, and if it has not timed out.
        dead_plasma_managers: A set of the plasma manager IDs of all the plasma
            managers that were up at one point and have died since then.
    """

    def __init__(self, redis_address, redis_port, autoscaling_config):
        # Initialize the Redis clients.
        self.state = ray.experimental.state.GlobalState()
        self.state._initialize_global_state(redis_address, redis_port)
        self.redis = redis.StrictRedis(
            host=redis_address, port=redis_port, db=0)
        # TODO(swang): Update pubsub client to use ray.experimental.state once
        # subscriptions are implemented there.
        self.subscribe_client = self.redis.pubsub()
        self.subscribed = {}
        # Initialize data structures to keep track of the active database
        # clients.
        self.dead_local_schedulers = set()
        self.live_plasma_managers = Counter()
        self.dead_plasma_managers = set()
        # Keep a mapping from local scheduler client ID to IP address to use
        # for updating the load metrics.
        self.local_scheduler_id_to_ip_map = dict()
        self.load_metrics = LoadMetrics()
        if autoscaling_config:
            self.autoscaler = StandardAutoscaler(autoscaling_config,
                                                 self.load_metrics)
        else:
            self.autoscaler = None

    def subscribe(self, channel):
        """Subscribe to the given channel.

        Args:
            channel (str): The channel to subscribe to.

        Raises:
            Exception: An exception is raised if the subscription fails.
        """
        self.subscribe_client.subscribe(channel)
        self.subscribed[channel] = False

    def cleanup_task_table(self):
        """Clean up global state for failed local schedulers.

        This marks any tasks that were scheduled on dead local schedulers as
        TASK_STATUS_LOST. A local scheduler is deemed dead if it is in
        self.dead_local_schedulers.
        """
        tasks = self.state.task_table()
        num_tasks_updated = 0
        for task_id, task in tasks.items():
            # See if the corresponding local scheduler is alive.
            if task["LocalSchedulerID"] not in self.dead_local_schedulers:
                continue

            # Remove dummy objects returned by actor tasks from any plasma
            # manager. Although the objects may still exist in that object
            # store, this deletion makes them effectively unreachable by any
            # local scheduler connected to a different store.
            # TODO(swang): Actually remove the objects from the object store,
            # so that the reconstructed actor can reuse the same object store.
            if hex_to_binary(task["TaskSpec"]["ActorID"]) != NIL_ACTOR_ID:
                dummy_object_id = task["TaskSpec"]["ReturnObjectIDs"][-1]
                obj = self.state.object_table(dummy_object_id)
                manager_ids = obj["ManagerIDs"]
                if manager_ids is not None:
                    # The dummy object should exist on at most one plasma
                    # manager, the manager associated with the local scheduler
                    # that died.
                    assert len(manager_ids) <= 1
                    # Remove the dummy object from the plasma manager
                    # associated with the dead local scheduler, if any.
                    for manager in manager_ids:
                        ok = self.state._execute_command(
                            dummy_object_id, "RAY.OBJECT_TABLE_REMOVE",
                            dummy_object_id.id(), hex_to_binary(manager))
                        if ok != b"OK":
                            log.warn("Failed to remove object location for "
                                     "dead plasma manager.")

            # If the task is scheduled on a dead local scheduler, mark the
            # task as lost.
            key = binary_to_object_id(hex_to_binary(task_id))
            ok = self.state._execute_command(
                key, "RAY.TASK_TABLE_UPDATE", hex_to_binary(task_id),
                ray.experimental.state.TASK_STATUS_LOST, NIL_ID,
                task["ExecutionDependenciesString"], task["SpillbackCount"])
            if ok != b"OK":
                log.warn("Failed to update lost task for dead scheduler.")
            num_tasks_updated += 1

        if num_tasks_updated > 0:
            log.warn("Marked {} tasks as lost.".format(num_tasks_updated))

    def cleanup_object_table(self):
        """Clean up global state for failed plasma managers.

        This removes dead plasma managers from any location entries in the
        object table. A plasma manager is deemed dead if it is in
        self.dead_plasma_managers.
        """
        # TODO(swang): Also kill the associated plasma store, since it's no
        # longer reachable without a plasma manager.
        objects = self.state.object_table()
        num_objects_removed = 0
        for object_id, obj in objects.items():
            manager_ids = obj["ManagerIDs"]
            if manager_ids is None:
                continue
            for manager in manager_ids:
                if manager in self.dead_plasma_managers:
                    # If the object was on a dead plasma manager, remove that
                    # location entry.
                    ok = self.state._execute_command(
                        object_id, "RAY.OBJECT_TABLE_REMOVE", object_id.id(),
                        hex_to_binary(manager))
                    if ok != b"OK":
                        log.warn("Failed to remove object location for dead "
                                 "plasma manager.")
                    num_objects_removed += 1
        if num_objects_removed > 0:
            log.warn("Marked {} objects as lost.".format(num_objects_removed))

    def scan_db_client_table(self):
        """Scan the database client table for dead clients.

        After subscribing to the client table, it's necessary to call this
        before reading any messages from the subscription channel. This ensures
        that we do not miss any notifications for deleted clients that occurred
        before we subscribed.
        """
        clients = self.state.client_table()
        for node_ip_address, node_clients in clients.items():
            for client in node_clients:
                db_client_id = client["DBClientID"]
                client_type = client["ClientType"]
                if client["Deleted"]:
                    if client_type == LOCAL_SCHEDULER_CLIENT_TYPE:
                        self.dead_local_schedulers.add(db_client_id)
                    elif client_type == PLASMA_MANAGER_CLIENT_TYPE:
                        self.dead_plasma_managers.add(db_client_id)

    def subscribe_handler(self, channel, data):
        """Handle a subscription success message from Redis."""
        log.debug("Subscribed to {}, data was {}".format(channel, data))
        self.subscribed[channel] = True

    def db_client_notification_handler(self, unused_channel, data):
        """Handle a notification from the db_client table from Redis.

        This handler processes notifications from the db_client table.
        Notifications should be parsed using the SubscribeToDBClientTableReply
        flatbuffer. Deletions are processed, insertions are ignored. Cleanup of
        the associated state in the state tables should be handled by the
        caller.
        """
        notification_object = (SubscribeToDBClientTableReply.
                               GetRootAsSubscribeToDBClientTableReply(data, 0))
        db_client_id = binary_to_hex(notification_object.DbClientId())
        client_type = notification_object.ClientType()
        is_insertion = notification_object.IsInsertion()

        # If the update was an insertion, we ignore it.
        if is_insertion:
            return

        # If the update was a deletion, add them to our accounting for dead
        # local schedulers and plasma managers.
        log.warn("Removed {}, client ID {}".format(client_type, db_client_id))
        if client_type == LOCAL_SCHEDULER_CLIENT_TYPE:
            if db_client_id not in self.dead_local_schedulers:
                self.dead_local_schedulers.add(db_client_id)
        elif client_type == PLASMA_MANAGER_CLIENT_TYPE:
            if db_client_id not in self.dead_plasma_managers:
                self.dead_plasma_managers.add(db_client_id)
            # Stop tracking this plasma manager's heartbeats, since it's
            # already dead.
            del self.live_plasma_managers[db_client_id]

    def local_scheduler_info_handler(self, unused_channel, data):
        """Handle a local scheduler heartbeat from Redis."""

        message = LocalSchedulerInfoMessage.GetRootAsLocalSchedulerInfoMessage(
            data, 0)
        num_resources = message.DynamicResourcesLength()
        static_resources = {}
        dynamic_resources = {}
        for i in range(num_resources):
            dyn = message.DynamicResources(i)
            static = message.StaticResources(i)
            dynamic_resources[dyn.Key().decode("utf-8")] = dyn.Value()
            static_resources[static.Key().decode("utf-8")] = static.Value()

        # Update the load metrics for this local scheduler.
        client_id = binascii.hexlify(message.DbClientId()).decode("utf-8")
        ip = self.local_scheduler_id_to_ip_map.get(client_id)
        if ip:
            self.load_metrics.update(ip, static_resources, dynamic_resources)
        else:
            print("Warning: could not find ip for client {}."
                  .format(client_id))

    def plasma_manager_heartbeat_handler(self, unused_channel, data):
        """Handle a plasma manager heartbeat from Redis.

        This resets the number of heartbeats that we've missed from this plasma
        manager.
        """
        # The first DB_CLIENT_ID_SIZE characters are the client ID.
        db_client_id = data[:DB_CLIENT_ID_SIZE]
        # Reset the number of heartbeats that we've missed from this plasma
        # manager.
        self.live_plasma_managers[db_client_id] = 0

    def _entries_for_driver_in_shard(self, driver_id, redis_shard_index):
        """Collect IDs of control-state entries for a driver from a shard.

        Args:
            driver_id: The ID of the driver.
            redis_shard_index: The index of the Redis shard to query.

        Returns:
            Lists of IDs: (returned_object_ids, task_ids, put_objects). The
                first two are relevant to the driver and are safe to delete.
                The last contains all "put" objects in this redis shard; each
                element is an (object_id, corresponding task_id) pair.
        """
        # TODO(zongheng): consider adding save & restore functionalities.
        redis = self.state.redis_clients[redis_shard_index]
        task_table_infos = {}  # task id -> TaskInfo messages

        # Scan the task table & filter to get the list of tasks belong to this
        # driver.  Use a cursor in order not to block the redis shards.
        for key in redis.scan_iter(match=TASK_TABLE_PREFIX + b"*"):
            entry = redis.hgetall(key)
            task_info = TaskInfo.GetRootAsTaskInfo(entry[b"TaskSpec"], 0)
            if driver_id != task_info.DriverId():
                # Ignore tasks that aren't from this driver.
                continue
            task_table_infos[task_info.TaskId()] = task_info

        # Get the list of objects returned by these tasks.  Note these might
        # not belong to this redis shard.
        returned_object_ids = []
        for task_info in task_table_infos.values():
            returned_object_ids.extend([
                task_info.Returns(i) for i in range(task_info.ReturnsLength())
            ])

        # Also record all the ray.put()'d objects.
        put_objects = []
        for key in redis.scan_iter(match=OBJECT_INFO_PREFIX + b"*"):
            entry = redis.hgetall(key)
            if entry[b"is_put"] == "0":
                continue
            object_id = key.split(OBJECT_INFO_PREFIX)[1]
            task_id = entry[b"task"]
            put_objects.append((object_id, task_id))

        return returned_object_ids, task_table_infos.keys(), put_objects

    def _clean_up_entries_from_shard(self, object_ids, task_ids, shard_index):
        redis = self.state.redis_clients[shard_index]
        # Clean up (in the future, save) entries for non-empty objects.
        object_ids_locs = set()
        object_ids_infos = set()
        for object_id in object_ids:
            # OL.
            obj_loc = redis.zrange(OBJECT_LOCATION_PREFIX + object_id, 0, -1)
            if obj_loc:
                object_ids_locs.add(object_id)
            # OI.
            obj_info = redis.hgetall(OBJECT_INFO_PREFIX + object_id)
            if obj_info:
                object_ids_infos.add(object_id)

        # Form the redis keys to delete.
        keys = [TASK_TABLE_PREFIX + k for k in task_ids]
        keys.extend([OBJECT_LOCATION_PREFIX + k for k in object_ids_locs])
        keys.extend([OBJECT_INFO_PREFIX + k for k in object_ids_infos])

        if not keys:
            return
        # Remove with best effort.
        num_deleted = redis.delete(*keys)
        log.info(
            "Removed {} dead redis entries of the driver from redis shard {}.".
            format(num_deleted, shard_index))
        if num_deleted != len(keys):
            log.warning(
                "Failed to remove {} relevant redis entries"
                " from redis shard {}.".format(len(keys) - num_deleted))

    def _clean_up_entries_for_driver(self, driver_id):
        """Remove this driver's object/task entries from all redis shards.

        Specifically, removes control-state entries of:
            * all objects (OI and OL entries) created by `ray.put()` from the
              driver
            * all tasks belonging to the driver.
        """
        # TODO(zongheng): handle function_table, client_table, log_files --
        # these are in the metadata redis server, not in the shards.
        driver_object_ids = []
        driver_task_ids = []
        all_put_objects = []

        # Collect relevant ids.
        # TODO(zongheng): consider parallelizing this loop.
        for shard_index in range(len(self.state.redis_clients)):
            returned_object_ids, task_ids, put_objects = \
                self._entries_for_driver_in_shard(driver_id, shard_index)
            driver_object_ids.extend(returned_object_ids)
            driver_task_ids.extend(task_ids)
            all_put_objects.extend(put_objects)

        # For the put objects, keep those from relevant tasks.
        driver_task_ids_set = set(driver_task_ids)
        for object_id, task_id in all_put_objects:
            if task_id in driver_task_ids_set:
                driver_object_ids.append(object_id)

        # Partition IDs and distribute to shards.
        object_ids_per_shard = defaultdict(list)
        task_ids_per_shard = defaultdict(list)

        def ToShardIndex(index):
            return binary_to_object_id(index).redis_shard_hash() % len(
                self.state.redis_clients)

        for object_id in driver_object_ids:
            object_ids_per_shard[ToShardIndex(object_id)].append(object_id)
        for task_id in driver_task_ids:
            task_ids_per_shard[ToShardIndex(task_id)].append(task_id)

        # TODO(zongheng): consider parallelizing this loop.
        for shard_index in range(len(self.state.redis_clients)):
            self._clean_up_entries_from_shard(
                object_ids_per_shard[shard_index],
                task_ids_per_shard[shard_index], shard_index)

    def driver_removed_handler(self, unused_channel, data):
        """Handle a notification that a driver has been removed.

        This releases any GPU resources that were reserved for that driver in
        Redis.
        """
        message = DriverTableMessage.GetRootAsDriverTableMessage(data, 0)
        driver_id = message.DriverId()
        log.info("Driver {} has been removed.".format(
            binary_to_hex(driver_id)))

        self._clean_up_entries_for_driver(driver_id)

    def process_messages(self, max_messages=10000):
        """Process all messages ready in the subscription channels.

        This reads messages from the subscription channels and calls the
        appropriate handlers until there are no messages left.

        Args:
            max_messages: The maximum number of messages to process before
                returning.
        """
        for _ in range(max_messages):
            message = self.subscribe_client.get_message()
            if message is None:
                return

            # Parse the message.
            channel = message["channel"]
            data = message["data"]

            # Determine the appropriate message handler.
            message_handler = None
            if not self.subscribed[channel]:
                # If the data was an integer, then the message was a response
                # to an initial subscription request.
                message_handler = self.subscribe_handler
            elif channel == PLASMA_MANAGER_HEARTBEAT_CHANNEL:
                assert self.subscribed[channel]
                # The message was a heartbeat from a plasma manager.
                message_handler = self.plasma_manager_heartbeat_handler
            elif channel == LOCAL_SCHEDULER_INFO_CHANNEL:
                assert self.subscribed[channel]
                # The message was a heartbeat from a local scheduler
                message_handler = self.local_scheduler_info_handler
            elif channel == DB_CLIENT_TABLE_NAME:
                assert self.subscribed[channel]
                # The message was a notification from the db_client table.
                message_handler = self.db_client_notification_handler
            elif channel == DRIVER_DEATH_CHANNEL:
                assert self.subscribed[channel]
                # The message was a notification that a driver was removed.
                log.info("message-handler: driver_removed_handler")
                message_handler = self.driver_removed_handler
            else:
                raise Exception("This code should be unreachable.")

            # Call the handler.
            assert (message_handler is not None)
            message_handler(channel, data)

    def run(self):
        """Run the monitor.

        This function loops forever, checking for messages about dead database
        clients and cleaning up state accordingly.
        """
        # Initialize the subscription channel.
        self.subscribe(DB_CLIENT_TABLE_NAME)
        self.subscribe(LOCAL_SCHEDULER_INFO_CHANNEL)
        self.subscribe(PLASMA_MANAGER_HEARTBEAT_CHANNEL)
        self.subscribe(DRIVER_DEATH_CHANNEL)

        # Scan the database table for dead database clients. NOTE: This must be
        # called before reading any messages from the subscription channel.
        # This ensures that we start in a consistent state, since we may have
        # missed notifications that were sent before we connected to the
        # subscription channel.
        self.scan_db_client_table()
        # If there were any dead clients at startup, clean up the associated
        # state in the state tables.
        if len(self.dead_local_schedulers) > 0:
            self.cleanup_task_table()
        if len(self.dead_plasma_managers) > 0:
            self.cleanup_object_table()
        log.debug("{} dead local schedulers, {} plasma managers total, {} "
                  "dead plasma managers".format(
                      len(self.dead_local_schedulers),
                      (len(self.live_plasma_managers) + len(
                          self.dead_plasma_managers)),
                      len(self.dead_plasma_managers)))

        # Handle messages from the subscription channels.
        while True:
            # Update the mapping from local scheduler client ID to IP address.
            # This is only used to update the load metrics for the autoscaler.
            local_schedulers = self.state.local_schedulers()
            self.local_scheduler_id_to_ip_map = {}
            for local_scheduler_info in local_schedulers:
                client_id = local_scheduler_info["DBClientID"]
                ip_address = local_scheduler_info["AuxAddress"].split(":")[0]
                self.local_scheduler_id_to_ip_map[client_id] = ip_address

            # Process autoscaling actions
            if self.autoscaler:
                self.autoscaler.update()
            # Record how many dead local schedulers and plasma managers we had
            # at the beginning of this round.
            num_dead_local_schedulers = len(self.dead_local_schedulers)
            num_dead_plasma_managers = len(self.dead_plasma_managers)
            # Process a round of messages.
            self.process_messages()
            # If any new local schedulers or plasma managers were marked as
            # dead in this round, clean up the associated state.
            if len(self.dead_local_schedulers) > num_dead_local_schedulers:
                self.cleanup_task_table()
            if len(self.dead_plasma_managers) > num_dead_plasma_managers:
                self.cleanup_object_table()

            # Handle plasma managers that timed out during this round.
            plasma_manager_ids = list(self.live_plasma_managers.keys())
            for plasma_manager_id in plasma_manager_ids:
                if ((self.live_plasma_managers[plasma_manager_id]) >=
                        ray._config.num_heartbeats_timeout()):
                    log.warn("Timed out {}".format(PLASMA_MANAGER_CLIENT_TYPE))
                    # Remove the plasma manager from the managers whose
                    # heartbeats we're tracking.
                    del self.live_plasma_managers[plasma_manager_id]
                    # Remove the plasma manager from the db_client table. The
                    # corresponding state in the object table will be cleaned
                    # up once we receive the notification for this db_client
                    # deletion.
                    self.redis.execute_command("RAY.DISCONNECT",
                                               plasma_manager_id)

            # Increment the number of heartbeats that we've missed from each
            # plasma manager.
            for plasma_manager_id in self.live_plasma_managers:
                self.live_plasma_managers[plasma_manager_id] += 1

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
    args = parser.parse_args()

    redis_ip_address = get_ip_address(args.redis_address)
    redis_port = get_port(args.redis_address)

    if args.autoscaling_config:
        autoscaling_config = os.path.expanduser(args.autoscaling_config)
    else:
        autoscaling_config = None

    monitor = Monitor(redis_ip_address, redis_port, autoscaling_config)
    monitor.run()
