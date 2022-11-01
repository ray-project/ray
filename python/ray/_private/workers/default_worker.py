import argparse
import base64
import json
import os
import sys
import time

import ray
import ray._private.node
import ray._private.ray_constants as ray_constants
import ray._private.utils
import ray.actor
from ray._private.parameter import RayParams
from ray._private.ray_logging import configure_log_file, get_worker_log_file_name

parser = argparse.ArgumentParser(
    description=("Parse addresses for the worker to connect to.")
)
parser.add_argument(
    "--node-ip-address",
    required=True,
    type=str,
    help="the ip address of the worker's node",
)
parser.add_argument(
    "--node-manager-port", required=True, type=int, help="the port of the worker's node"
)
parser.add_argument(
    "--raylet-ip-address",
    required=False,
    type=str,
    default=None,
    help="the ip address of the worker's raylet",
)
parser.add_argument(
    "--redis-address", required=True, type=str, help="the address to use for Redis"
)
parser.add_argument(
    "--gcs-address", required=True, type=str, help="the address to use for GCS"
)
parser.add_argument(
    "--redis-password",
    required=False,
    type=str,
    default=None,
    help="the password to use for Redis",
)
parser.add_argument(
    "--object-store-name", required=True, type=str, help="the object store's name"
)
parser.add_argument("--raylet-name", required=False, type=str, help="the raylet's name")
parser.add_argument(
    "--logging-level",
    required=False,
    type=str,
    default=ray_constants.LOGGER_LEVEL,
    choices=ray_constants.LOGGER_LEVEL_CHOICES,
    help=ray_constants.LOGGER_LEVEL_HELP,
)
parser.add_argument(
    "--logging-format",
    required=False,
    type=str,
    default=ray_constants.LOGGER_FORMAT,
    help=ray_constants.LOGGER_FORMAT_HELP,
)
parser.add_argument(
    "--temp-dir",
    required=False,
    type=str,
    default=None,
    help="Specify the path of the temporary directory use by Ray process.",
)
parser.add_argument(
    "--storage",
    required=False,
    type=str,
    default=None,
    help="Specify the persistent storage path.",
)
parser.add_argument(
    "--load-code-from-local",
    default=False,
    action="store_true",
    help="True if code is loaded from local files, as opposed to the GCS.",
)
parser.add_argument(
    "--worker-type",
    required=False,
    type=str,
    default="WORKER",
    help="Specify the type of the worker process",
)
parser.add_argument(
    "--metrics-agent-port",
    required=True,
    type=int,
    help="the port of the node's metric agent.",
)
parser.add_argument(
    "--object-spilling-config",
    required=False,
    type=str,
    default="",
    help="The configuration of object spilling. Only used by I/O workers.",
)
parser.add_argument(
    "--logging-rotate-bytes",
    required=False,
    type=int,
    default=ray_constants.LOGGING_ROTATE_BYTES,
    help="Specify the max bytes for rotating "
    "log file, default is "
    f"{ray_constants.LOGGING_ROTATE_BYTES} bytes.",
)
parser.add_argument(
    "--logging-rotate-backup-count",
    required=False,
    type=int,
    default=ray_constants.LOGGING_ROTATE_BACKUP_COUNT,
    help="Specify the backup count of rotated log file, default is "
    f"{ray_constants.LOGGING_ROTATE_BACKUP_COUNT}.",
)
parser.add_argument(
    "--runtime-env-hash",
    required=False,
    type=int,
    default=0,
    help="The computed hash of the runtime env for this worker.",
)
parser.add_argument(
    "--startup-token",
    required=True,
    type=int,
    help="The startup token assigned to this worker process by the raylet.",
)
parser.add_argument(
    "--ray-debugger-external",
    default=False,
    action="store_true",
    help="True if Ray debugger is made available externally.",
)

if __name__ == "__main__":
    # NOTE(sang): For some reason, if we move the code below
    # to a separate function, tensorflow will capture that method
    # as a step function. For more details, check out
    # https://github.com/ray-project/ray/pull/12225#issue-525059663.
    args = parser.parse_args()
    ray._private.ray_logging.setup_logger(args.logging_level, args.logging_format)

    if args.worker_type == "WORKER":
        mode = ray.WORKER_MODE
    elif args.worker_type == "SPILL_WORKER":
        mode = ray.SPILL_WORKER_MODE
    elif args.worker_type == "RESTORE_WORKER":
        mode = ray.RESTORE_WORKER_MODE
    else:
        raise ValueError("Unknown worker type: " + args.worker_type)

    raylet_ip_address = args.raylet_ip_address
    if raylet_ip_address is None:
        raylet_ip_address = args.node_ip_address

    ray_params = RayParams(
        node_ip_address=args.node_ip_address,
        raylet_ip_address=raylet_ip_address,
        node_manager_port=args.node_manager_port,
        redis_address=args.redis_address,
        redis_password=args.redis_password,
        plasma_store_socket_name=args.object_store_name,
        raylet_socket_name=args.raylet_name,
        temp_dir=args.temp_dir,
        storage=args.storage,
        metrics_agent_port=args.metrics_agent_port,
        gcs_address=args.gcs_address,
    )

    node = ray._private.node.Node(
        ray_params,
        head=False,
        shutdown_at_exit=False,
        spawn_reaper=False,
        connect_only=True,
    )

    # NOTE(suquark): We must initialize the external storage before we
    # connect to raylet. Otherwise we may receive requests before the
    # external storage is intialized.
    if mode == ray.RESTORE_WORKER_MODE or mode == ray.SPILL_WORKER_MODE:
        from ray._private import external_storage, storage

        storage._init_storage(args.storage, is_head=False)
        if args.object_spilling_config:
            object_spilling_config = base64.b64decode(args.object_spilling_config)
            object_spilling_config = json.loads(object_spilling_config)
        else:
            object_spilling_config = {}
        external_storage.setup_external_storage(
            object_spilling_config, node.session_name
        )

    ray._private.worker._global_node = node
    ray._private.worker.connect(
        node,
        node.session_name,
        mode=mode,
        runtime_env_hash=args.runtime_env_hash,
        startup_token=args.startup_token,
        ray_debugger_external=args.ray_debugger_external,
    )

    # Add code search path to sys.path, set load_code_from_local.
    core_worker = ray._private.worker.global_worker.core_worker
    code_search_path = core_worker.get_job_config().code_search_path
    load_code_from_local = False
    if code_search_path:
        load_code_from_local = True
        for p in code_search_path:
            if os.path.isfile(p):
                p = os.path.dirname(p)
            sys.path.insert(0, p)
    ray._private.worker.global_worker.set_load_code_from_local(load_code_from_local)

    # Setup log file.
    out_file, err_file = node.get_log_file_handles(
        get_worker_log_file_name(args.worker_type)
    )
    configure_log_file(out_file, err_file)

    if mode == ray.WORKER_MODE:
        ray._private.worker.global_worker.main_loop()
    elif mode in [ray.RESTORE_WORKER_MODE, ray.SPILL_WORKER_MODE]:
        # It is handled by another thread in the C++ core worker.
        # We just need to keep the worker alive.
        while True:
            time.sleep(100000)
    else:
        raise ValueError(f"Unexcepted worker mode: {mode}")
