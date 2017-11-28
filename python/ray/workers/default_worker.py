from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import binascii
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
        redis_client = ray.services.create_redis_client(args.redis_address)
        ray.utils.push_error_to_driver(redis_client, "worker_crash",
                                       traceback_str, driver_id=None)
        # TODO(rkn): Note that if the worker was in the middle of executing
        # a task, then any worker or driver that is blocking in a get call
        # and waiting for the output of that task will hang. We need to
        # address this.
