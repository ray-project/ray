from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
from collections import Counter
import json
import logging
import redis
import time

import ray
from ray.services import get_ip_address
from ray.services import get_port
from ray.utils import binary_to_object_id
from ray.utils import binary_to_hex
from ray.utils import hex_to_binary

# Import flatbuffer bindings.
from ray.core.generated.SubscribeToDBClientTableReply \
    import SubscribeToDBClientTableReply
from ray.core.generated.DriverTableMessage import DriverTableMessage

# These variables must be kept in sync with the C codebase.
# common/common.h
HEARTBEAT_TIMEOUT_MILLISECONDS = 100
NUM_HEARTBEATS_TIMEOUT = 100
DB_CLIENT_ID_SIZE = 20
NIL_ID = b"\xff" * DB_CLIENT_ID_SIZE
# common/task.h
TASK_STATUS_LOST = 32
# common/state/redis.cc
PLASMA_MANAGER_HEARTBEAT_CHANNEL = b"plasma_managers"
DRIVER_DEATH_CHANNEL = b"driver_deaths"
# common/redis_module/ray_redis_module.cc
OBJECT_PREFIX = "OL:"
DB_CLIENT_PREFIX = "CL:"
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

  The monitor is in charge of cleaning up the tables in the global state after
  processes have died. The monitor is currently not responsible for detecting
  component failures.

  Attributes:
    redis: A connection to the Redis server.
    subscribe_client: A pubsub client for the Redis server. This is used to
      receive notifications about failed components.
    subscribed: A dictionary mapping channel names (str) to whether or not the
      subscription to that channel has succeeded yet (bool).
    dead_local_schedulers: A set of the local scheduler IDs of all of the local
      schedulers that were up at one point and have died since then.
    live_plasma_managers: A counter mapping live plasma manager IDs to the
      number of heartbeats that have passed since we last heard from that
      plasma manager. A plasma manager is live if we received a heartbeat from
      it at any point, and if it has not timed out.
    dead_plasma_managers: A set of the plasma manager IDs of all the plasma
      managers that were up at one point and have died since then.
  """
  def __init__(self, redis_address, redis_port):
    # Initialize the Redis clients.
    self.state = ray.experimental.state.GlobalState()
    self.state._initialize_global_state(redis_address, redis_port)
    self.redis = redis.StrictRedis(host=redis_address, port=redis_port, db=0)
    # TODO(swang): Update pubsub client to use ray.experimental.state once
    # subscriptions are implemented there.
    self.subscribe_client = self.redis.pubsub()
    self.subscribed = {}
    # Initialize data structures to keep track of the active database clients.
    self.dead_local_schedulers = set()
    self.live_plasma_managers = Counter()
    self.dead_plasma_managers = set()

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
      if task["LocalSchedulerID"] in self.dead_local_schedulers:
        # If the task is scheduled on a dead local scheduler, mark the task as
        # lost.
        key = binary_to_object_id(hex_to_binary(task_id))
        ok = self.state._execute_command(
            key, "RAY.TASK_TABLE_UPDATE", hex_to_binary(task_id),
            ray.experimental.state.TASK_STATUS_LOST, NIL_ID)
        if ok != b"OK":
          log.warn("Failed to update lost task for dead scheduler.")
        num_tasks_updated += 1
    if num_tasks_updated > 0:
      log.warn("Marked {} tasks as lost.".format(num_tasks_updated))

  def cleanup_object_table(self):
    """Clean up global state for failed plasma managers.

    This removes dead plasma managers from any location entries in the object
    table. A plasma manager is deemed dead if it is in
    self.dead_plasma_managers.
    """
    # TODO(swang): Also kill the associated plasma store, since it's no longer
    # reachable without a plasma manager.
    objects = self.state.object_table()
    num_objects_removed = 0
    for object_id, obj in objects.items():
      manager_ids = obj["ManagerIDs"]
      if manager_ids is None:
        continue
      for manager in manager_ids:
        if manager in self.dead_plasma_managers:
          # If the object was on a dead plasma manager, remove that location
          # entry.
          ok = self.state._execute_command(object_id,
                                           "RAY.OBJECT_TABLE_REMOVE",
                                           object_id.id(),
                                           hex_to_binary(manager))
          if ok != b"OK":
            log.warn("Failed to remove object location for dead plasma "
                     "manager.")
          num_objects_removed += 1
    if num_objects_removed > 0:
      log.warn("Marked {} objects as lost.".format(num_objects_removed))

  def scan_db_client_table(self):
    """Scan the database client table for dead clients.

    After subscribing to the client table, it's necessary to call this before
    reading any messages from the subscription channel. This ensures that we do
    not miss any notifications for deleted clients that occurred before we
    subscribed.
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
    """Handle a subscription success message from Redis.
    """
    log.debug("Subscribed to {}, data was {}".format(channel, data))
    self.subscribed[channel] = True

  def db_client_notification_handler(self, channel, data):
    """Handle a notification from the db_client table from Redis.

    This handler processes notifications from the db_client table.
    Notifications should be parsed using the SubscribeToDBClientTableReply
    flatbuffer. Deletions are processed, insertions are ignored. Cleanup of the
    associated state in the state tables should be handled by the caller.
    """
    notification_object = (SubscribeToDBClientTableReply
                           .GetRootAsSubscribeToDBClientTableReply(data, 0))
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

  def plasma_manager_heartbeat_handler(self, channel, data):
    """Handle a plasma manager heartbeat from Redis.

    This resets the number of heartbeats that we've missed from this plasma
    manager.
    """
    # The first DB_CLIENT_ID_SIZE characters are the client ID.
    db_client_id = data[:DB_CLIENT_ID_SIZE]
    # Reset the number of heartbeats that we've missed from this plasma
    # manager.
    self.live_plasma_managers[db_client_id] = 0

  def driver_removed_handler(self, channel, data):
    """Handle a notification that a driver has been removed.

    This releases any GPU resources that were reserved for that driver in
    Redis.
    """
    message = DriverTableMessage.GetRootAsDriverTableMessage(data, 0)
    driver_id = message.DriverId()
    log.info("Driver {} has been removed.".format(binary_to_hex(driver_id)))

    # Get a list of the local schedulers.
    client_table = ray.global_state.client_table()
    local_schedulers = []
    for ip_address, clients in client_table.items():
      for client in clients:
        if client["ClientType"] == "local_scheduler":
          local_schedulers.append(client)

    # Release any GPU resources that have been reserved for this driver in
    # Redis.
    for local_scheduler in local_schedulers:
      if int(local_scheduler["NumGPUs"]) > 0:
        local_scheduler_id = local_scheduler["DBClientID"]

        num_gpus_returned = 0

        # Perform a transaction to return the GPUs.
        with self.redis.pipeline() as pipe:
          while True:
            try:
              # If this key is changed before the transaction below (the
              # multi/exec block), then the transaction will not take place.
              pipe.watch(local_scheduler_id)

              result = pipe.hget(local_scheduler_id, "gpus_in_use")
              gpus_in_use = dict() if result is None else json.loads(result)

              driver_id_hex = binary_to_hex(driver_id)
              if driver_id_hex in gpus_in_use:
                num_gpus_returned = gpus_in_use.pop(driver_id_hex)

              pipe.multi()

              pipe.hset(local_scheduler_id, "gpus_in_use",
                        json.dumps(gpus_in_use))

              pipe.execute()
              # If a WatchError is not raise, then the operations should have
              # gone through atomically.
              break
            except redis.WatchError:
              # Another client must have changed the watched key between the
              # time we started WATCHing it and the pipeline's execution. We
              # should just retry.
              continue

        log.info("Driver {} is returning GPU IDs {} to local scheduler {}."
                 .format(driver_id, num_gpus_returned, local_scheduler_id))

  def process_messages(self):
    """Process all messages ready in the subscription channels.

    This reads messages from the subscription channels and calls the
    appropriate handlers until there are no messages left.
    """
    while True:
      message = self.subscribe_client.get_message()
      if message is None:
        return

      # Parse the message.
      channel = message["channel"]
      data = message["data"]

      # Determine the appropriate message handler.
      message_handler = None
      if not self.subscribed[channel]:
        # If the data was an integer, then the message was a response to an
        # initial subscription request.
        message_handler = self.subscribe_handler
      elif channel == PLASMA_MANAGER_HEARTBEAT_CHANNEL:
        assert(self.subscribed[channel])
        # The message was a heartbeat from a plasma manager.
        message_handler = self.plasma_manager_heartbeat_handler
      elif channel == DB_CLIENT_TABLE_NAME:
        assert(self.subscribed[channel])
        # The message was a notification from the db_client table.
        message_handler = self.db_client_notification_handler
      elif channel == DRIVER_DEATH_CHANNEL:
        assert(self.subscribed[channel])
        # The message was a notification that a driver was removed.
        message_handler = self.driver_removed_handler
      else:
        raise Exception("This code should be unreachable.")

      # Call the handler.
      assert(message_handler is not None)
      message_handler(channel, data)

  def run(self):
    """Run the monitor.

    This function loops forever, checking for messages about dead database
    clients and cleaning up state accordingly.
    """
    # Initialize the subscription channel.
    self.subscribe(DB_CLIENT_TABLE_NAME)
    self.subscribe(PLASMA_MANAGER_HEARTBEAT_CHANNEL)
    self.subscribe(DRIVER_DEATH_CHANNEL)

    # Scan the database table for dead database clients. NOTE: This must be
    # called before reading any messages from the subscription channel. This
    # ensures that we start in a consistent state, since we may have missed
    # notifications that were sent before we connected to the subscription
    # channel.
    self.scan_db_client_table()
    # If there were any dead clients at startup, clean up the associated state
    # in the state tables.
    if len(self.dead_local_schedulers) > 0:
      self.cleanup_task_table()
    if len(self.dead_plasma_managers) > 0:
      self.cleanup_object_table()
    log.debug("{} dead local schedulers, {} plasma managers total, {} dead "
              "plasma managers".format(len(self.dead_local_schedulers),
                                       (len(self.live_plasma_managers) +
                                        len(self.dead_plasma_managers)),
                                       len(self.dead_plasma_managers)))

    # Handle messages from the subscription channels.
    while True:
      # Record how many dead local schedulers and plasma managers we had at the
      # beginning of this round.
      num_dead_local_schedulers = len(self.dead_local_schedulers)
      num_dead_plasma_managers = len(self.dead_plasma_managers)
      # Process a round of messages.
      self.process_messages()
      # If any new local schedulers or plasma managers were marked as dead in
      # this round, clean up the associated state.
      if len(self.dead_local_schedulers) > num_dead_local_schedulers:
        self.cleanup_task_table()
      if len(self.dead_plasma_managers) > num_dead_plasma_managers:
        self.cleanup_object_table()

      # Handle plasma managers that timed out during this round.
      plasma_manager_ids = list(self.live_plasma_managers.keys())
      for plasma_manager_id in plasma_manager_ids:
        if ((self.live_plasma_managers
             [plasma_manager_id]) >= NUM_HEARTBEATS_TIMEOUT):
          log.warn("Timed out {}".format(PLASMA_MANAGER_CLIENT_TYPE))
          # Remove the plasma manager from the managers whose heartbeats we're
          # tracking.
          del self.live_plasma_managers[plasma_manager_id]
          # Remove the plasma manager from the db_client table. The
          # corresponding state in the object table will be cleaned up once we
          # receive the notification for this db_client deletion.
          self.redis.execute_command("RAY.DISCONNECT", plasma_manager_id)

      # Increment the number of heartbeats that we've missed from each plasma
      # manager.
      for plasma_manager_id in self.live_plasma_managers:
        self.live_plasma_managers[plasma_manager_id] += 1

      # Wait for a heartbeat interval before processing the next round of
      # messages.
      time.sleep(HEARTBEAT_TIMEOUT_MILLISECONDS * 1e-3)


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description=("Parse Redis server for the "
                                                "monitor to connect to."))
  parser.add_argument("--redis-address", required=True, type=str,
                      help="the address to use for Redis")
  args = parser.parse_args()

  redis_ip_address = get_ip_address(args.redis_address)
  redis_port = get_port(args.redis_address)

  # Initialize the global state.
  ray.global_state._initialize_global_state(redis_ip_address, redis_port)

  monitor = Monitor(redis_ip_address, redis_port)
  monitor.run()
