from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import redis
import time

import ray

EVENT_KEY = "RAY_MULTI_NODE_TEST_KEY"
"""This key is used internally within this file for coordinating drivers."""


def _wait_for_nodes_to_join(num_nodes, timeout=20):
  """Wait until the nodes have joined the cluster.

  This will wait until exactly num_nodes have joined the cluster and each node
  has a local scheduler and a plasma manager.

  Args:
    num_nodes: The number of nodes to wait for.
    timeout: The amount of time in seconds to wait before failing.

  Raises:
    Exception: An exception is raised if too many nodes join the cluster or if
      the timeout expires while we are waiting.
  """
  start_time = time.time()
  while time.time() - start_time < timeout:
    client_table = ray.global_state.client_table()
    num_ready_nodes = len(client_table)
    if num_ready_nodes == num_nodes:
      ready = True
      # Check that for each node, a local scheduler and a plasma manager are
      # present.
      for ip_address, clients in client_table.items():
        client_types = [client["ClientType"] for client in clients]
        if "local_scheduler" not in client_types:
          ready = False
        if "plasma_manager" not in client_types:
          ready = False
      if ready:
        return
    if num_ready_nodes > num_nodes:
      # Too many nodes have joined. Something must be wrong.
      raise Exception("{} nodes have joined the cluster, but we were "
                      "expecting {} nodes.".format(num_ready_nodes, num_nodes))
    time.sleep(0.1)

  # If we get here then we timed out.
  raise Exception("Timed out while waiting for {} nodes to join. Only {} "
                  "nodes have joined so far.".format(num_ready_nodes,
                                                     num_nodes))


def _broadcast_event(event_name, redis_address):
  """Broadcast an event.

  Args:
    event_name: The name of the event to wait for.
    redis_address: The address of the Redis server to use for synchronization.

  This is used to synchronize drivers for the multi-node tests.
  """
  redis_host, redis_port = redis_address.split(":")
  redis_client = redis.StrictRedis(host=redis_host, port=int(redis_port))
  redis_client.rpush(EVENT_KEY, event_name)


def _wait_for_event(event_name, redis_address, extra_buffer=1):
  """Block until an event has been broadcast.

  Args:
    event_name: The name of the event to wait for.
    redis_address: The address of the Redis server to use for synchronization.
    extra_buffer: An amount of time in seconds to wait after the event.

  This is used to synchronize drivers for the multi-node tests.
  """
  redis_host, redis_port = redis_address.split(":")
  redis_client = redis.StrictRedis(host=redis_host, port=int(redis_port))
  while True:
    event_names = redis_client.lrange(EVENT_KEY, 0, -1)
    if event_name.encode("ascii") in event_names:
      break
  time.sleep(extra_buffer)
