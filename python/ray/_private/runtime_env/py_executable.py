import asyncio
import json
import logging
import os
import shlex
import sys
from typing import List, Optional

from ray._private import ray_constants
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.runtime_env.utils import (
    SubprocessCalledProcessError,
    check_output_cmd,
)
from ray._private.utils import (
    check_version_info,
    compute_version_info,
    expand_env_value,
)
from ray._private.workers import py_executable_probe

default_logger = logging.getLogger(__name__)

# A slow probe (e.g. a package manager installing) must not block env creation.
PROBE_TIMEOUT_S = 60

# These plugins replace or wrap the worker command, so a bare probe would not
# represent the worker.
_PLUGINS_CHANGING_WORKER_COMMAND = (
    "pip",
    "uv",
    "conda",
    "image_uri",
    "container",
    "_nsight",
    "_rocprof_sys",
)


class PyExecutablePlugin(RuntimeEnvPlugin):
    """This plugin allows running Ray workers with a custom Python executable.

    You can use it with
    `ray.init(runtime_env={"py_executable": "<command> <args>"})`. If you specify
    a `working_dir` in the runtime environment, the executable will have access
    to the working directory, for example, to a requirements.txt for a package manager,
    a script for a debugger, or the executable could be a shell script in the
    working directory. You can also use this plugin to run worker processes
    in a custom profiler or use a custom Python interpreter or `python` with
    custom arguments.
    """

    name = "py_executable"

    def __init__(self):
        pass

    async def create(
        self,
        uri: Optional[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger = default_logger,
    ) -> int:
        if (
            ray_constants.RAY_PY_EXECUTABLE_CHECK_ENABLED
            and sys.platform != "win32"
            and not any(p in runtime_env for p in _PLUGINS_CHANGING_WORKER_COMMAND)
        ):
            await self._check_ray_in_py_executable(
                runtime_env.py_executable(), context, logger
            )
        return 0

    async def _check_ray_in_py_executable(
        self,
        py_executable: str,
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> None:
        """Fail env creation if the py_executable would run a mismatched Ray.

        Otherwise the worker dies while importing Ray, the raylet keeps restarting
        it, and the task only fails after many minutes with a generic error. Any
        probe failure that is not a Ray import or version problem is ignored, so
        that worker startup reports it as before.

        Args:
            py_executable: The user's `py_executable` command.
            context: The runtime env context created so far.
            logger: The logger of this runtime env setup.

        Raises:
            RuntimeError: If Ray cannot be imported or its version differs from
                the cluster's.
        """
        probe = os.path.abspath(py_executable_probe.__file__)
        # Mirrors RuntimeEnvContext.exec_worker so the probe sees the worker's
        # working directory and environment.
        command = " ".join(
            [*context.command_prefix, "exec", py_executable, shlex.quote(probe)]
        )
        env = {
            **os.environ,
            **{k: expand_env_value(v) for k, v in context.env_vars.items()},
        }
        try:
            output = await asyncio.wait_for(
                check_output_cmd(
                    ["bash", "-c", command],
                    logger=logger,
                    env=env,
                    stdin=asyncio.subprocess.DEVNULL,
                ),
                timeout=PROBE_TIMEOUT_S,
            )
        except SubprocessCalledProcessError as e:
            output = e.output or ""
        except (asyncio.TimeoutError, RuntimeError):
            logger.warning("Could not run py_executable to check its Ray install.")
            return

        probe_result = None
        for line in output.splitlines():
            if line.startswith(py_executable_probe.PROBE_MARKER):
                probe_result = line[len(py_executable_probe.PROBE_MARKER) :]
        try:
            info = json.loads(probe_result)
            build = "free-threaded" if info["free_threaded"] else "regular"
            executable = info["executable"]
            import_error = info.get("import_error")
            if import_error is None:
                worker_version_info = (info["ray_version"], info["python_version"])
        except (TypeError, ValueError, KeyError):
            logger.warning("Could not parse the py_executable probe output.")
            return

        if import_error is not None:
            raise RuntimeError(
                f"The py_executable `{py_executable}` ({executable}, a {build} "
                f"Python build) cannot import Ray: {import_error}\n"
                "The py_executable environment must have its own Ray installation of "
                "the same version as the cluster, built for that interpreter (for a "
                "free-threaded interpreter, a free-threaded build such as cp315t). "
                "Also make sure PYTHONPATH does not point at the cluster's Ray "
                "installation."
            )
        cluster_ray_version, cluster_python_version = compute_version_info()
        check_version_info(
            {
                "ray_version": cluster_ray_version,
                "python_version": cluster_python_version,
            },
            f"py_executable `{py_executable}` ({executable})",
            this_process_version_info=worker_version_info,
        )

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        logger.info("Running py_executable plugin")
        context.py_executable = runtime_env.py_executable()
