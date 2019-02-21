from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import traceback

import ray
import ray.actor
import ray.node
import ray.ray_constants as ray_constants
import ray.utils
from ray.parameter import RayParams

parser = argparse.ArgumentParser(
    description=("Parse addresses for the worker "
                 "to connect to."))
parser.add_argument(
    "--node-ip-address",
    required=True,
    type=str,
    help="the ip address of the worker's node")
parser.add_argument(
    "--redis-address",
    required=True,
    type=str,
    help="the address to use for Redis")
parser.add_argument(
    "--redis-password",
    required=False,
    type=str,
    default=None,
    help="the password to use for Redis")
parser.add_argument(
    "--object-store-name",
    required=True,
    type=str,
    help="the object store's name")
parser.add_argument(
    "--raylet-name", required=False, type=str, help="the raylet's name")
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
    "--temp-dir",
    required=False,
    type=str,
    default=None,
    help="Specify the path of the temporary directory use by Ray process.")
parser.add_argument(
    "--load-code-from-local",
    default=False,
    action='store_true',
    help="True if code is loaded from local files, as opposed to the GCS.")

if __name__ == "__main__":
    args = parser.parse_args()

    info = {
        "node_ip_address": args.node_ip_address,
        "redis_address": args.redis_address,
        "redis_password": args.redis_password,
        "store_socket_name": args.object_store_name,
        "raylet_socket_name": args.raylet_name,
    }

    ray.utils.setup_logger(args.logging_level, args.logging_format)

    ray_params = RayParams(
        node_ip_address=args.node_ip_address,
        redis_address=args.redis_address,
        redis_password=args.redis_password,
        plasma_store_socket_name=args.object_store_name,
        raylet_socket_name=args.raylet_name,
        temp_dir=args.temp_dir,
        load_code_from_local=args.load_code_from_local)

    node = ray.node.Node(
        ray_params, head=False, shutdown_at_exit=False, connect_only=True)
    ray.worker._global_node = node

    # TODO(suquark): Use "node" as the input of "connect".
    ray.worker.connect(
        info,
        redis_password=args.redis_password,
        mode=ray.WORKER_MODE,
        load_code_from_local=args.load_code_from_local)

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
    except Exception:
        traceback_str = traceback.format_exc() + error_explanation
        ray.utils.push_error_to_driver(
            ray.worker.global_worker,
            "worker_crash",
            traceback_str,
            driver_id=None)
        # TODO(rkn): Note that if the worker was in the middle of executing
        # a task, then any worker or driver that is blocking in a get call
        # and waiting for the output of that task will hang. We need to
        # address this.
