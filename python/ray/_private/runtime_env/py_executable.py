import logging

from typing import List, Optional

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin

default_logger = logging.getLogger(__name__)


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
        return 0

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        logger.info("Running py_executable plugin")
        context.py_executable = runtime_env.py_executable()
