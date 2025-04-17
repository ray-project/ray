import os
import sys
import logging
import asyncio
import subprocess
import copy
from pathlib import Path
from typing import Tuple, List, Dict, Optional

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.utils import (
    try_to_create_directory,
)
from ray.exceptions import RuntimeEnvSetupError

default_logger = logging.getLogger(__name__)

# Omnitrace config used when runtime_env={"_omnitrace": "default"}
# Refer to the following link for more information on omnitrace options
# https://rocm.docs.amd.com/projects/omnitrace/en/latest/how-to/understanding-omnitrace-output.html
OMNITRACE_DEFAULT_CONFIG = {
    "env": {
        "OMNITRACE_TIME_OUTPUT": "false",
        "OMNITRACE_OUTPUT_PREFIX": "worker_process_%p",
    },
    "args": {
        "F": "true",
    },
}


def parse_omnitrace_config(
    omnitrace_config: Dict[str, str]
) -> Tuple[List[str], List[str]]:
    """
    Function to convert dictionary of omnitrace options into
    omnitrace-python command line

    The function returns:
    - List[str]: omnitrace-python cmd line split into list of str
    """
    omnitrace_cmd = ["omnitrace-python"]
    omnitrace_env = {}
    if "args" in omnitrace_config:
        # Parse omnitrace arg options
        for option, option_val in omnitrace_config["args"].items():
            # option standard based on
            # https://www.gnu.org/software/libc/manual/html_node/Argument-Syntax.html
            if len(option) > 1:
                omnitrace_cmd.append(f"--{option}={option_val}")
            else:
                omnitrace_cmd += [f"-{option}", option_val]
    if "env" in omnitrace_config:
        omnitrace_env = omnitrace_config["env"]
    omnitrace_cmd.append("--")
    return omnitrace_cmd, omnitrace_env


class OmnitracePlugin(RuntimeEnvPlugin):
    name = "_omnitrace"

    def __init__(self, resources_dir: str):
        self.omnitrace_cmd = []
        self.omnitrace_env = {}

        # replace this with better way to get logs dir
        session_dir, runtime_dir = os.path.split(resources_dir)
        self._omnitrace_dir = Path(session_dir) / "logs" / "omnitrace"
        try_to_create_directory(self._omnitrace_dir)

    async def _check_omnitrace_script(
        self, omnitrace_config: Dict[str, str]
    ) -> Tuple[bool, str]:
        """
        Function to validate if omnitrace_config is a valid omnitrace profile options
        Args:
            omnitrace_config: dictionary mapping omnitrace option to it's value
        Returns:
            a tuple consists of a boolean indicating if the omnitrace_config
            is valid option and an error message if the omnitrace_config is invalid
        """

        # use empty as omnitrace report test filename
        test_folder = str(Path(self._omnitrace_dir) / "test")
        omnitrace_cmd, omnitrace_env = parse_omnitrace_config(omnitrace_config)
        omnitrace_env_copy = copy.deepcopy(omnitrace_env)
        omnitrace_env_copy["OMNITRACE_OUTPUT_PATH"] = test_folder
        omnitrace_env_copy.update(os.environ)
        try_to_create_directory(test_folder)

        # Create a test python file to run omnitrace
        with open(f"{test_folder}/test.py", "w") as f:
            f.write("import time\n")
        try:
            omnitrace_cmd = omnitrace_cmd + [f"{test_folder}/test.py"]
            process = await asyncio.create_subprocess_exec(
                *omnitrace_cmd,
                env=omnitrace_env_copy,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            error_msg = stderr.strip() if stderr.strip() != "" else stdout.strip()

            # cleanup temp file
            clean_up_cmd = ["rm", "-r", test_folder]
            cleanup_process = await asyncio.create_subprocess_exec(
                *clean_up_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            _, _ = await cleanup_process.communicate()
            if process.returncode == 0:
                return True, None
            else:
                return False, error_msg
        except FileNotFoundError:
            return False, ("omnitrace is not installed")

    async def create(
        self,
        uri: Optional[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger = default_logger,
    ) -> int:
        omnitrace_config = runtime_env.omnitrace()
        if not omnitrace_config:
            return 0

        if omnitrace_config and sys.platform != "linux":
            raise RuntimeEnvSetupError(
                "omnitrace CLI is only available in Linux.\n"
            )

        if isinstance(omnitrace_config, str):
            if omnitrace_config == "default":
                omnitrace_config = OMNITRACE_DEFAULT_CONFIG
            else:
                raise RuntimeEnvSetupError(
                    f"Unsupported omnitrace config: {omnitrace_config}. "
                    "The supported config is 'default' or "
                    "Dictionary of omnitrace options"
                )

        is_valid_omnitrace_config, error_msg = await self._check_omnitrace_script(
            omnitrace_config
        )
        if not is_valid_omnitrace_config:
            logger.warning(error_msg)
            raise RuntimeEnvSetupError(
                "omnitrace profile failed to run with the following "
                f"error message:\n {error_msg}"
            )
        # add set output path to logs dir
        if "env" not in omnitrace_config:
            omnitrace_config["env"] = {}
        omnitrace_config["env"]["OMNITRACE_OUTPUT_PATH"] = str(
            Path(self._omnitrace_dir)
        )

        self.omnitrace_cmd, self.omnitrace_env = parse_omnitrace_config(
            omnitrace_config
        )
        return 0

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        logger.info("Running omnitrace profiler")
        context.py_executable = " ".join(self.omnitrace_cmd)
        context.env_vars.update(self.omnitrace_env)
