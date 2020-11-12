import argparse
import base64
import json
import logging
import time
import sys
import os
from contextlib import redirect_stdout, redirect_stderr
from logging.handlers import RotatingFileHandler

import ray
import ray.actor
import ray.node
import ray.ray_constants as ray_constants
import ray.utils
from ray.parameter import RayParams
from ray.ray_logging import StandardStreamInterceptor

def setup_and_get_worker_interceptor_logger(is_for_stdout: bool):
    """Setup a logger to be used to intercept worker log messages.

    NOTE: The method is not idempotent.
    
    Ray worker logs should be treated in a special way because
    there's a need to intercept stdout and stderr to support various
    ray features. For example, ray will prepend 0 or 1 in the beggining
    of each log message to decide if logs should be streamed to driveres.

    This logger will also setup the RotatingFileHandler for
    ray workers processes.

    Args:
        is_for_stdout(bool): True if logger will be used to intercept stdout.
                             False otherwise.
    """
    file_extension = "out" if is_for_stdout else "err"
    logger = logging.getLogger("")
    logger.setLevel(logging.INFO)
    job_id = os.environ.get("RAY_JOB_ID")
    assert job_id is not None, (
        "RAY_JOB_ID should be set as an env "
        "variable within default_worker.py. If you see this error, "
        "please report it to Ray's Github issue.")
    worker_name = "worker" if args.worker_type == "WORKER" else "io_worker"
    # Make sure these values are set already.
    assert ray.worker._global_node is not None
    assert ray.worker.global_worker is not None
    handler = RotatingFileHandler(
        f"{ray.worker._global_node.get_session_dir_path()}/logs/"
        f"{worker_name}-"
        f"{ray.utils.binary_to_hex(ray.worker.global_worker.worker_id)}-"
        f"{job_id}-{os.getpid()}.{file_extension}")
    logger.addHandler(handler)
    # TODO(sang): Add 0 or 1 to decide whether
    # or not logs are streamed to drivers.
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.propagate = False
    # handler.terminator = ""
    return logger


def main(args):
    ray.ray_logging.setup_logger(args.logging_level, args.logging_format)

    if args.worker_type == "WORKER":
        mode = ray.WORKER_MODE
    elif args.worker_type == "IO_WORKER":
        mode = ray.IO_WORKER_MODE
    else:
        raise ValueError("Unknown worker type: " + args.worker_type)

    # NOTE(suquark): We must initialize the external storage before we
    # connect to raylet. Otherwise we may receive requests before the
    # external storage is intialized.
    if mode == ray.IO_WORKER_MODE:
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

    stdout_interceptor = StandardStreamInterceptor(
        setup_and_get_worker_interceptor_logger(True))
    # stderr_interceptor = StandardStreamInterceptor(
    #     setup_and_get_worker_interceptor_logger(False))

    # Redirect stdout and stderr to the default worker interceptor logger.
    with redirect_stdout(stdout_interceptor):
        with redirect_stderr(stdout_interceptor):
            if mode == ray.WORKER_MODE:
                ray.worker.global_worker.main_loop()
            elif mode == ray.IO_WORKER_MODE:
                # It is handled by another thread in the C++ core worker.
                # We just need to keep the worker alive.
                while True:
                    time.sleep(100000)
            else:
                raise ValueError(f"Unexcepted worker mode: {mode}")


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
    args = parser.parse_args()
    main(args)
