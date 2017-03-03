from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import binascii
from collections import Counter
import logging
import redis
import time

from ray.services import get_ip_address
from ray.services import get_port

# These variables must be kept in sync with the C codebase.
# common/common.h
DB_CLIENT_ID_SIZE = 20
NIL_ID = b"\xff" * DB_CLIENT_ID_SIZE
# common/task.h
TASK_STATUS_LOST = 32
# common/redis_module/ray_redis_module.c
TASK_PREFIX = "TT:"
DB_CLIENT_PREFIX = "CL:"
DB_CLIENT_TABLE_NAME = b"db_clients"
# local_scheduler/local_scheduler.h
LOCAL_SCHEDULER_HEARTBEAT_TIMEOUT_MILLISECONDS = 100
LOCAL_SCHEDULER_CLIENT_TYPE = b"local_scheduler"

# Set up logging.
logging.basicConfig()
log = logging.getLogger()

class Monitor(object):
  """A monitor for Ray processes.

  The monitor is in charge of cleaning up the tables in the global state after
  processes have died. The monitor is currently not responsible for detecting
  component failures.

  Attributes:
    redis: A connection to the Redis server.
    subscribe_client: A pubsub client for the Redis server. This is used to
      receive notifications about failed components.
    local_schedulers: A set of the local scheduler IDs of all of the currently
      live local schedulers in the cluster. In addition, this also includes
      NIL_ID.
  """
  def __init__(self, redis_address, redis_port):
    self.redis = redis.StrictRedis(host=redis_address, port=redis_port, db=0)
    self.subscribe_client = self.redis.pubsub()

    # Initialize data structures to keep track of the active database clients.
    self.local_schedulers = set()
    # Add the NIL_ID so that we don't accidentally mark tasks that aren't
    # associated with a node as LOST during cleanup.
    self.local_schedulers.add(NIL_ID)

  def subscribe(self):
    """Subscribe to the db_clients channel.

    Raises:
      Exception: An exception is raised if the subscription fails.
    """
    self.subscribe_client.subscribe(DB_CLIENT_TABLE_NAME)
    # Wait for the first message to signal that the subscription was successful.
    while True:
      message = self.subscribe_client.get_message()
      if message is None:
        time.sleep(LOCAL_SCHEDULER_HEARTBEAT_TIMEOUT_MILLISECONDS / 1000)
        continue
      break

    # The first message's payload should be the index of our subscription.
    if "data" not in message:
      Exception("Unable to subscribe to local scheduler table.")

  def read_message(self):
    """Read a message from the db_clients channel.

    Returns:
      None if no message was to read. Otherwise, a tuple of (db_client_id,
        client_type, auxiliary_address, is_insertion) is returned. The value
        is_insertion is a bool that is true if the update to the db_clients
        table was an insertion and false if deletion.
    """
    message = self.subscribe_client.get_message()
    if message is None:
      return None

    # Parse the message.
    data = message["data"]
    db_client_id = data[:DB_CLIENT_ID_SIZE]
    data = data[DB_CLIENT_ID_SIZE + 1:]
    data = data.split(b" ")
    client_type, auxiliary_address, is_insertion = data
    is_insertion = int(is_insertion)
    if is_insertion != 1 and is_insertion != 0:
      raise Exception("Expected 0 or 1 for insertion field, got {} instead".format(is_insertion))
    is_insertion = bool(is_insertion)

    return db_client_id, client_type, auxiliary_address, is_insertion

  def cleanup_task_table(self):
    """Clean up global state for a failed local schedulers.

    This marks any tasks that were scheduled on dead local schedulers as
    TASK_STATUS_LOST. A local scheduler is deemed dead if it is not in
    self.local_schedulers.
    """
    task_ids = self.redis.scan_iter(match="{prefix}*".format(prefix=TASK_PREFIX))
    for task_id in task_ids:
      task_id = task_id[len(TASK_PREFIX):]
      response = self.redis.execute_command("RAY.TASK_TABLE_GET", task_id)
      if response[1] not in self.local_schedulers:
        ok = self.redis.execute_command("RAY.TASK_TABLE_UPDATE",
                                        task_id,
                                        TASK_STATUS_LOST,
                                        NIL_ID)
        if ok != b"OK":
          log.warn("Failed to update lost task for dead scheduler.")

  def scan_db_client_table(self):
    """Scan the database client table for the current clients.

    After subscribing to the client table, it's necessary to call this before
    reading any messages from the subscription channel.
    """
    db_client_keys = self.redis.keys("{prefix}*".format(prefix=DB_CLIENT_PREFIX))
    for db_client_key in db_client_keys:
      db_client_id = db_client_key[len(DB_CLIENT_PREFIX):]
      client_type = self.redis.hget(db_client_key, "client_type")
      if client_type == LOCAL_SCHEDULER_CLIENT_TYPE:
        self.local_schedulers.add(db_client_id)

  def run(self):
    """Run the monitor.

    This function loops forever, checking for messages about dead database
    clients and cleaning up state accordingly.
    """
    # Initialize the subscription channel.
    self.subscribe()

    # Scan the database table and clean up any state associated with clients
    # not in the database table. NOTE: This must be called before reading any
    # messages from the subscription channel. This ensures that we start in a
    # consistent state, since we may have missed notifications that were sent
    # before we connected to the subscription channel.
    self.scan_db_client_table()
    self.cleanup_task_table()
    log.debug("Scanned schedulers: {}".format(self.local_schedulers))

    # Read messages from the subscription channel.
    while True:
      time.sleep(LOCAL_SCHEDULER_HEARTBEAT_TIMEOUT_MILLISECONDS / 1000)
      client = self.read_message()
      # There was no message to be read.
      if client is None:
        continue

      db_client_id, client_type, auxiliary_address, is_insertion = client

      # If the update was an insertion, record the client ID.
      if is_insertion:
        self.local_schedulers.add(db_client_id)
        log.debug("Added scheduler: {}".format(db_client_id))
        continue

      # If the update was a deletion, clean up global state.
      if client_type == LOCAL_SCHEDULER_CLIENT_TYPE:
        if db_client_id in self.local_schedulers:
          log.warn("Removed scheduler: {}".format(db_client_id))
          self.local_schedulers.remove(db_client_id)
          self.cleanup_task_table()

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description=("Parse Redis server for the "
                                                "monitor to connect to."))
  parser.add_argument("--redis-address", required=True, type=str,
                      help="the address to use for Redis")
  args = parser.parse_args()

  redis_ip_address = get_ip_address(args.redis_address)
  redis_port = get_port(args.redis_address)

  monitor = Monitor(redis_ip_address, redis_port)
  monitor.run()
