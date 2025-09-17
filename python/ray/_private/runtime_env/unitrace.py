import asyncio
import copy
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ray._common.utils import (
    try_to_create_directory,
)
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray.exceptions import RuntimeEnvSetupError

default_logger = logging.getLogger(__name__)

# unitrace options used when runtime_env={"_unitrace": "default"}
UNITRACE_DEFAULT_CONFIG = {
    "chrome-kernel-logging": "",
    "teardown-on-signal": "0",
}

def parse_unitrace_config(unitrace_config: Dict[str, str]) -> List[str]:
    """
    Function to convert dictionary of unitrace options into
    unitrace command line

    The function returns:
    - List[str]: unitrace profile cmd line split into list of str
    """
    unitrace_cmd = ["unitrace"]
    for option, option_val in unitrace_config.items():
        # option standard based on
        # https://www.gnu.org/software/libc/manual/html_node/Argument-Syntax.html
        if len(option) > 1:
            unitrace_cmd.append(f"--{option}")
        else:
            unitrace_cmd.append(f"-{option}")

        if (len(option_val) > 0):
            unitrace_cmd.append(f"{option_val}")

    return unitrace_cmd


class UnitracePlugin(RuntimeEnvPlugin):
    name = "_unitrace"

    def __init__(self, resources_dir: str):
        self.unitrace_cmd = []

    async def _check_unitrace_script(
        self, unitrace_config: Dict[str, str]
    ) -> Tuple[bool, str]:
        """
        Function to validate if unitrace_config is a valid unitrace profile options
        Args:
            unitrace_config: dictionary mapping unitrace option to it's value
        Returns:
            a tuple consists of a boolean indicating if the unitrace_config
            is valid option and an error message if the unitrace_config is invalid
        """

        unitrace_cmd = ["unitrace"]
        try:
            process = await asyncio.create_subprocess_exec(
                *unitrace_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            error_msg = stderr.strip() if stderr.strip() != "" else stdout.strip()

            if (process.returncode == 0):
                if (isinstance(stdout, bytes) and ("--chrome-kernel-logging" in stdout.decode())):
                    return True, None
                else:
                    return True, ("wrong unitrace installed")
            else:
                return False, error_msg
        except FileNotFoundError:
            return False, ("unitrace is not installed")

    async def create(
        self,
        uri: Optional[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger = default_logger,
    ) -> int:
        unitrace_config = runtime_env.unitrace()
        if not unitrace_config:
            return 0

        if isinstance(unitrace_config, str):
            if unitrace_config == "default":
                unitrace_config = UNITRACE_DEFAULT_CONFIG
            else:
                raise RuntimeEnvSetupError(
                    f"Unsupported unitrace config: {unitrace_config}. "
                    "The supported config is 'default' or "
                    "Dictionary of unitrace options"
                )

        is_valid_unitrace_cmd, error_msg = await self._check_unitrace_script(unitrace_config)
        if not is_valid_unitrace_cmd:
            logger.warning(error_msg)
            raise RuntimeEnvSetupError(
                "unitrace profile failed to run with the following "
                f"error message:\n {error_msg}"
            )

        self.unitrace_cmd = parse_unitrace_config(unitrace_config)
        return 0

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        logger.info("Running unitrace profiler")
        context.py_executable = " ".join(self.unitrace_cmd) + " python"
