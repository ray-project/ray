from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import binascii
import numpy as np
import redis
import traceback

import ray

parser = argparse.ArgumentParser(description=("Parse addresses for the worker "
                                              "to connect to."))
parser.add_argument("--node-ip-address", required=True, type=str,
                    help="the ip address of the worker's node")
parser.add_argument("--redis-address", required=True, type=str,
                    help="the address to use for Redis")
parser.add_argument("--object-store-name", required=True, type=str,
                    help="the object store's name")
parser.add_argument("--object-store-manager-name", required=True, type=str,
                    help="the object store manager's name")
parser.add_argument("--local-scheduler-name", required=True, type=str,
                    help="the local scheduler's name")
parser.add_argument("--actor-id", required=False, type=str,
                    help="the actor ID of this worker")


def random_string():
  return np.random.bytes(20)


if __name__ == "__main__":
  args = parser.parse_args()
  info = {"node_ip_address": args.node_ip_address,
          "redis_address": args.redis_address,
          "store_socket_name": args.object_store_name,
          "manager_socket_name": args.object_store_manager_name,
          "local_scheduler_socket_name": args.local_scheduler_name}

  if args.actor_id is not None:
    actor_id = binascii.unhexlify(args.actor_id)
  else:
    actor_id = ray.worker.NIL_ACTOR_ID

  ray.worker.connect(info, mode=ray.WORKER_MODE, actor_id=actor_id)

  error_explanation = """
This error is unexpected and should not have happened. Somehow a worker crashed
in an unanticipated way causing the main_loop to throw an exception, which is
being caught in "python/ray/workers/default_worker.py".
"""

  while True:
    try:
      # This call to main_loop should never return if things are working. Most
      # exceptions that are thrown (e.g., inside the execution of a task)
      # should be caught and handled inside of the call to main_loop. If an
      # exception is thrown here, then that means that there is some error that
      # we didn't anticipate.
      ray.worker.main_loop()
    except Exception as e:
      traceback_str = traceback.format_exc() + error_explanation
      DRIVER_ID_LENGTH = 20
      # We use a driver ID of all zeros to push an error message to all
      # drivers.
      driver_id = DRIVER_ID_LENGTH * b"\x00"
      error_key = b"Error:" + driver_id + b":" + random_string()
      redis_ip_address, redis_port = args.redis_address.split(":")
      # For this command to work, some other client (on the same machine as
      # Redis) must have run "CONFIG SET protected-mode no".
      redis_client = redis.StrictRedis(host=redis_ip_address,
                                       port=int(redis_port))
      redis_client.hmset(error_key, {"type": "worker_crash",
                                     "message": traceback_str,
                                     "note": ("This error is unexpected and "
                                              "should not have happened.")})
      redis_client.rpush("ErrorKeys", error_key)
      # TODO(rkn): Note that if the worker was in the middle of executing a
      # task, the any worker or driver that is blocking in a get call and
      # waiting for the output of that task will hang. We need to address this.

    # After putting the error message in Redis, this worker will attempt to
    # reenter the main loop. TODO(rkn): We should probably reset it's state and
    # call connect again.
