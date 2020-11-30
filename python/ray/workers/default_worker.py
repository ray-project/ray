import argparse
import base64
import json
import time
import sys
import os

import ray
import ray.actor
import ray.node
import ray.ray_constants as ray_constants
import ray.utils
from ray.parameter import RayParams
from ray.ray_logging import (StandardStreamInterceptor,
                             setup_and_get_worker_interceptor_logger)

parser = argparse.ArgumentParser(
    description=("Parse addresses for the worker "
                 "to connect to."))
parser.add_argument(
    "--node-ip-address",
    required=True,
    type=str,
    help="the ip address of the worker's node")
parser.add_argument(
    "--node-manager-port",
    required=True,
    type=int,
    help="the port of the worker's node")
parser.add_argument(
    "--raylet-ip-address",
    required=False,
    type=str,
    default=None,
    help="the ip address of the worker's raylet")
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
    "--config-list",
    required=False,
    type=str,
    default=None,
    help="Override internal config options for the worker process.")
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
parser.add_argument(
    "--worker-type",
    required=False,
    type=str,
    default="WORKER",
    help="Specify the type of the worker process")
parser.add_argument(
    "--metrics-agent-port",
    required=True,
    type=int,
    help="the port of the node's metric agent.")
parser.add_argument(
    "--object-spilling-config",
    required=False,
    type=str,
    default="",
    help="The configuration of object spilling. Only used by I/O workers.")
parser.add_argument(
    "--code-search-path",
    default=None,
    type=str,
    help="A list of directories or jar files separated by colon that specify "
    "the search path for user code. This will be used as `CLASSPATH` in "
    "Java and `PYTHONPATH` in Python.")
if __name__ == "__main__":
    # NOTE(sang): For some reason, if we move the code below
    # to a separate function, tensorflow will capture that method
    # as a step function. For more details, check out
    # https://github.com/ray-project/ray/pull/12225#issue-525059663.
    args = parser.parse_args()
    ray.ray_logging.setup_logger(args.logging_level, args.logging_format)

    if args.worker_type == "WORKER":
        mode = ray.WORKER_MODE
    elif args.worker_type == "SPILL_WORKER":
        mode = ray.SPILL_WORKER_MODE
    elif args.worker_type == "RESTORE_WORKER":
        mode = ray.RESTORE_WORKER_MODE
    else:
        raise ValueError("Unknown worker type: " + args.worker_type)

    # NOTE(suquark): We must initialize the external storage before we
    # connect to raylet. Otherwise we may receive requests before the
    # external storage is intialized.
    if mode == ray.RESTORE_WORKER_MODE or mode == ray.SPILL_WORKER_MODE:
        from ray import external_storage
        if args.object_spilling_config:
            object_spilling_config = base64.b64decode(
                args.object_spilling_config)
            object_spilling_config = json.loads(object_spilling_config)
        else:
            object_spilling_config = {}
        external_storage.setup_external_storage(object_spilling_config)

    raylet_ip_address = args.raylet_ip_address
    if raylet_ip_address is None:
        raylet_ip_address = args.node_ip_address

    code_search_path = args.code_search_path
    if code_search_path is not None:
        for p in code_search_path.split(":"):
            if os.path.isfile(p):
                p = os.path.dirname(p)
            sys.path.append(p)

    ray_params = RayParams(
        node_ip_address=args.node_ip_address,
        raylet_ip_address=raylet_ip_address,
        node_manager_port=args.node_manager_port,
        redis_address=args.redis_address,
        redis_password=args.redis_password,
        plasma_store_socket_name=args.object_store_name,
        raylet_socket_name=args.raylet_name,
        temp_dir=args.temp_dir,
        load_code_from_local=args.load_code_from_local,
        metrics_agent_port=args.metrics_agent_port,
    )

    node = ray.node.Node(
        ray_params,
        head=False,
        shutdown_at_exit=False,
        spawn_reaper=False,
        connect_only=True)
    ray.worker._global_node = node
    ray.worker.connect(node, mode=mode)

    # Redirect stdout and stderr to the default worker interceptor logger.
    # NOTE: We deprecated redirect_worker_output arg,
    # so we don't need to handle here.
    stdout_interceptor = StandardStreamInterceptor(
        setup_and_get_worker_interceptor_logger(args, is_for_stdout=True),
        intercept_stdout=True)
    stderr_interceptor = StandardStreamInterceptor(
        setup_and_get_worker_interceptor_logger(args, is_for_stdout=False),
        intercept_stdout=False)
    # Although the os level fd is duplicated already, we should overwrite
    # the python level stdout/stderr object.
    # Otherwise, buffers won't be flushed.
    sys.stdout = stdout_interceptor
    sys.stderr = stderr_interceptor

    if mode == ray.WORKER_MODE:
        ray.worker.global_worker.main_loop()
    elif (mode == ray.RESTORE_WORKER_MODE or mode == ray.SPILL_WORKER_MODE):
        # It is handled by another thread in the C++ core worker.
        # We just need to keep the worker alive.
        while True:
            time.sleep(100000)
    else:
        raise ValueError(f"Unexcepted worker mode: {mode}")
