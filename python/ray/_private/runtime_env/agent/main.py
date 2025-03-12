import sys
import os
import argparse
import logging
import ray._private.ray_constants as ray_constants
from ray.core.generated import (
    runtime_env_agent_pb2,
)
from ray._private.utils import open_log
from ray._private.utils import (
    get_or_create_event_loop,
)
from ray._private.process_watcher import create_check_raylet_task
from ray._raylet import StreamRedirector


def import_libs():
    my_dir = os.path.abspath(os.path.dirname(__file__))
    sys.path.insert(0, os.path.join(my_dir, "thirdparty_files"))  # for aiohttp
    sys.path.insert(0, my_dir)  # for runtime_env_agent and runtime_env_consts


import_libs()

import runtime_env_consts  # noqa: E402
from runtime_env_agent import RuntimeEnvAgent  # noqa: E402
from aiohttp import web  # noqa: E402


def get_capture_filepaths(log_dir):
    """Get filepaths for the given [log_dir].

    log_dir:
        Logging directory to place output and error logs.
    """
    filename = "runtime_env_agent"
    return (
        f"{log_dir}/{filename}.out",
        f"{log_dir}/{filename}.err",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runtime env agent.")
    parser.add_argument(
        "--node-ip-address",
        required=True,
        type=str,
        help="the IP address of this node.",
    )
    parser.add_argument(
        "--runtime-env-agent-port",
        required=True,
        type=int,
        default=None,
        help="The port on which the runtime env agent will receive HTTP requests.",
    )

    parser.add_argument(
        "--gcs-address", required=True, type=str, help="The address (ip:port) of GCS."
    )
    parser.add_argument(
        "--cluster-id-hex", required=True, type=str, help="The cluster id in hex."
    )
    parser.add_argument(
        "--runtime-env-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of the resource directory used by runtime_env.",
    )

    parser.add_argument(
        "--logging-level",
        required=False,
        type=lambda s: logging.getLevelName(s.upper()),
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
        "--logging-filename",
        required=False,
        type=str,
        default=runtime_env_consts.RUNTIME_ENV_AGENT_LOG_FILENAME,
        help="Specify the name of log file, "
        'log to stdout if set empty, default is "{}".'.format(
            runtime_env_consts.RUNTIME_ENV_AGENT_LOG_FILENAME
        ),
    )
    parser.add_argument(
        "--logging-rotate-bytes",
        required=True,
        type=int,
        help="Specify the max bytes for rotating log file",
    )
    parser.add_argument(
        "--logging-rotate-backup-count",
        required=True,
        type=int,
        help="Specify the backup count of rotated log file",
    )
    parser.add_argument(
        "--log-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of log directory.",
    )
    parser.add_argument(
        "--temp-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of the temporary directory use by Ray process.",
    )

    args = parser.parse_args()

    # Disable log rotation for windows platform.
    logging_rotation_bytes = args.logging_rotate_bytes if sys.platform != "win32" else 0
    logging_rotation_backup_count = (
        args.logging_rotate_backup_count if sys.platform != "win32" else 1
    )

    logging_params = dict(
        logging_level=args.logging_level,
        logging_format=args.logging_format,
        log_dir=args.log_dir,
        filename=args.logging_filename,
        max_bytes=logging_rotation_bytes,
        backup_count=logging_rotation_backup_count,
    )

    # Setup stdout/stderr redirect files
    out_filepath, err_filepath = get_capture_filepaths(args.log_dir)
    StreamRedirector.redirect_stdout(
        out_filepath,
        logging_rotation_bytes,
        logging_rotation_backup_count,
        False,
        False,
    )
    StreamRedirector.redirect_stderr(
        err_filepath,
        logging_rotation_bytes,
        logging_rotation_backup_count,
        False,
        False,
    )

    # Setup python stdout/stderr stream.
    stdout_fileno = sys.stdout.fileno()
    stderr_fileno = sys.stderr.fileno()
    # We also manually set sys.stdout and sys.stderr because that seems to
    # have an effect on the output buffering. Without doing this, stdout
    # and stderr are heavily buffered resulting in seemingly lost logging
    # statements. We never want to close the stdout file descriptor, dup2 will
    # close it when necessary and we don't want python's GC to close it.
    sys.stdout = open_log(stdout_fileno, unbuffered=True, closefd=False)
    sys.stderr = open_log(stderr_fileno, unbuffered=True, closefd=False)

    agent = RuntimeEnvAgent(
        runtime_env_dir=args.runtime_env_dir,
        logging_params=logging_params,
        gcs_address=args.gcs_address,
        cluster_id_hex=args.cluster_id_hex,
        temp_dir=args.temp_dir,
        address=args.node_ip_address,
        runtime_env_agent_port=args.runtime_env_agent_port,
    )

    # POST /get_or_create_runtime_env
    # body is serialzied protobuf GetOrCreateRuntimeEnvRequest
    # reply is serialzied protobuf GetOrCreateRuntimeEnvReply
    async def get_or_create_runtime_env(request: web.Request) -> web.Response:
        data = await request.read()
        request = runtime_env_agent_pb2.GetOrCreateRuntimeEnvRequest()
        request.ParseFromString(data)
        reply = await agent.GetOrCreateRuntimeEnv(request)
        return web.Response(
            body=reply.SerializeToString(), content_type="application/octet-stream"
        )

    # POST /delete_runtime_env_if_possible
    # body is serialzied protobuf DeleteRuntimeEnvIfPossibleRequest
    # reply is serialzied protobuf DeleteRuntimeEnvIfPossibleReply
    async def delete_runtime_env_if_possible(request: web.Request) -> web.Response:
        data = await request.read()
        request = runtime_env_agent_pb2.DeleteRuntimeEnvIfPossibleRequest()
        request.ParseFromString(data)
        reply = await agent.DeleteRuntimeEnvIfPossible(request)
        return web.Response(
            body=reply.SerializeToString(), content_type="application/octet-stream"
        )

    # POST /get_runtime_envs_info
    # body is serialzied protobuf GetRuntimeEnvsInfoRequest
    # reply is serialzied protobuf GetRuntimeEnvsInfoReply
    async def get_runtime_envs_info(request: web.Request) -> web.Response:
        data = await request.read()
        request = runtime_env_agent_pb2.GetRuntimeEnvsInfoRequest()
        request.ParseFromString(data)
        reply = await agent.GetRuntimeEnvsInfo(request)
        return web.Response(
            body=reply.SerializeToString(), content_type="application/octet-stream"
        )

    app = web.Application()

    app.router.add_post("/get_or_create_runtime_env", get_or_create_runtime_env)
    app.router.add_post(
        "/delete_runtime_env_if_possible", delete_runtime_env_if_possible
    )
    app.router.add_post("/get_runtime_envs_info", get_runtime_envs_info)

    loop = get_or_create_event_loop()
    check_raylet_task = None
    if sys.platform not in ["win32", "cygwin"]:

        def parent_dead_callback(msg):
            agent._logger.info(
                "Raylet is dead! Exiting Runtime Env Agent. "
                f"addr: {args.node_ip_address}, "
                f"port: {args.runtime_env_agent_port}\n"
                f"{msg}"
            )

        # No need to await this task.
        check_raylet_task = create_check_raylet_task(
            args.log_dir, args.gcs_address, parent_dead_callback, loop
        )
    runtime_env_agent_ip = (
        "127.0.0.1" if args.node_ip_address == "127.0.0.1" else "0.0.0.0"
    )
    try:
        web.run_app(
            app,
            host=runtime_env_agent_ip,
            port=args.runtime_env_agent_port,
            loop=loop,
        )
    except SystemExit as e:
        agent._logger.info(f"SystemExit! {e}")
        # We have to poke the task exception, or there's an error message
        # "task exception was never retrieved".
        if check_raylet_task is not None:
            check_raylet_task.exception()
        sys.exit(e.code)
