import argparse
import logging
import os
import subprocess
import sys

from ray._private.ray_constants import LOGGER_FORMAT, LOGGER_LEVEL
from ray._private.ray_logging import setup_logger
from ray._private.runtime_env.context import RuntimeEnvContext
from ray.core.generated.common_pb2 import Language

logger = logging.getLogger(__name__)

_PARENT_PIPE_MONITOR_SCRIPT = """
import os
import select
import signal
import sys

target_pid = int(sys.argv[1])

while True:
    readable, _, _ = select.select([sys.stdin], [], [], 1.0)
    if readable:
        data = os.read(sys.stdin.fileno(), 1)
        if not data:
            try:
                os.kill(target_pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            sys.exit(0)
    try:
        os.kill(target_pid, 0)
    except ProcessLookupError:
        sys.exit(0)
"""

parser = argparse.ArgumentParser(
    description=("Set up the environment for a Ray worker and launch the worker.")
)

parser.add_argument(
    "--serialized-runtime-env-context",
    type=str,
    help="the serialized runtime env context",
)

parser.add_argument("--language", type=str, help="the language type of the worker")

parser.add_argument(
    "--monitor-parent-pipe",
    required=False,
    action="store_true",
    help="Internal: exit when inherited stdin pipe reaches EOF.",
)


def _start_parent_pipe_monitor(enabled: bool):
    if not enabled:
        return None
    stdin = getattr(sys.stdin, "buffer", sys.stdin)
    return subprocess.Popen(
        [sys.executable, "-c", _PARENT_PIPE_MONITOR_SCRIPT, str(os.getpid())],
        stdin=stdin,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


if __name__ == "__main__":
    setup_logger(LOGGER_LEVEL, LOGGER_FORMAT)
    args, remaining_args = parser.parse_known_args()
    _start_parent_pipe_monitor(args.monitor_parent_pipe)
    # NOTE(edoakes): args.serialized_runtime_env_context is only None when
    # we're starting the main Ray client proxy server. That case should
    # probably not even go through this codepath.
    runtime_env_context = RuntimeEnvContext.deserialize(
        args.serialized_runtime_env_context or "{}"
    )
    runtime_env_context.exec_worker(remaining_args, Language.Value(args.language))
