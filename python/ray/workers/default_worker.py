from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

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
    action="store_true",
    help="True if code is loaded from local files, as opposed to the GCS.")
parser.add_argument(
    "--use-pickle",
    default=False,
    action="store_true",
    help="True if cloudpickle should be used for serialization.")

if __name__ == "__main__":
    args = parser.parse_args()

    ray.utils.setup_logger(args.logging_level, args.logging_format)

    ray_params = RayParams(
        node_ip_address=args.node_ip_address,
        redis_address=args.redis_address,
        redis_password=args.redis_password,
        plasma_store_socket_name=args.object_store_name,
        raylet_socket_name=args.raylet_name,
        temp_dir=args.temp_dir,
        load_code_from_local=args.load_code_from_local,
        use_pickle=args.use_pickle)

    node = ray.node.Node(
        ray_params, head=False, shutdown_at_exit=False, connect_only=True)
    ray.worker._global_node = node
    ray.worker.connect(node, mode=ray.WORKER_MODE)
    ray.worker.global_worker.main_loop()
