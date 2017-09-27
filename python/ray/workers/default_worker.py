from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import binascii
import numpy as np
import redis
import traceback

import ray
import ray.actor

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
parser.add_argument("--reconstruct", action="store_true",
                    help=("true if the actor should be started in reconstruct "
                          "mode"))


def random_string():
    return np.random.bytes(20)


def create_redis_client(redis_address):
    redis_ip_address, redis_port = redis_address.split(":")
    # For this command to work, some other client (on the same machine
    # as Redis) must have run "CONFIG SET protected-mode no".
    return redis.StrictRedis(host=redis_ip_address, port=int(redis_port))


def push_error_to_all_drivers(redis_client, message):
    """Push an error message to all drivers.

    Args:
        redis_client: The redis client to use.
        message: The error message to push.
    """
    DRIVER_ID_LENGTH = 20
    # We use a driver ID of all zeros to push an error message to all
    # drivers.
    driver_id = DRIVER_ID_LENGTH * b"\x00"
    error_key = b"Error:" + driver_id + b":" + random_string()
    # Create a Redis client.
    redis_client.hmset(error_key, {"type": "worker_crash",
                                   "message": message})
    redis_client.rpush("ErrorKeys", error_key)


if __name__ == "__main__":
    args = parser.parse_args()

    # If this worker is not an actor, it cannot be started in reconstruct mode.
    if args.actor_id is None:
        assert not args.reconstruct

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
  This error is unexpected and should not have happened. Somehow a worker
  crashed in an unanticipated way causing the main_loop to throw an exception,
  which is being caught in "python/ray/workers/default_worker.py".
  """

    try:
        # This call to main_loop should never return if things are working.
        # Most exceptions that are thrown (e.g., inside the execution of a
        # task) should be caught and handled inside of the call to
        # main_loop. If an exception is thrown here, then that means that
        # there is some error that we didn't anticipate.
        ray.worker.global_worker.main_loop()
    except Exception as e:
        traceback_str = traceback.format_exc() + error_explanation
        # Create a Redis client.
        redis_client = create_redis_client(args.redis_address)
        push_error_to_all_drivers(redis_client, traceback_str)
        # TODO(rkn): Note that if the worker was in the middle of executing
        # a task, then any worker or driver that is blocking in a get call
        # and waiting for the output of that task will hang. We need to
        # address this.
