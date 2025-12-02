import asyncio
import copy
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ray._common.utils import try_to_create_directory
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray.exceptions import RuntimeEnvSetupError

default_logger = logging.getLogger(__name__)

# rocprof-sys config used when runtime_env={"_rocprof_sys": "default"}
# Refer to the following link for more information on rocprof-sys options
# https://rocm.docs.amd.com/projects/rocprofiler-systems/en/docs-6.4.0/how-to/understanding-rocprof-sys-output.html
ROCPROFSYS_DEFAULT_CONFIG = {
    "env": {
        "ROCPROFSYS_TIME_OUTPUT": "false",
        "ROCPROFSYS_OUTPUT_PREFIX": "worker_process_%p",
    },
    "args": {
        "F": "true",
    },
}


def parse_rocprof_sys_config(
    rocprof_sys_config: Dict[str, str]
) -> Tuple[List[str], List[str]]:
    """
    Function to convert dictionary of rocprof-sys options into
    rocprof-sys-python command line

    The function returns:
    - List[str]: rocprof-sys-python cmd line split into list of str
    """
    rocprof_sys_cmd = ["rocprof-sys-python"]
    rocprof_sys_env = {}
    if "args" in rocprof_sys_config:
        # Parse rocprof-sys arg options
        for option, option_val in rocprof_sys_config["args"].items():
            # option standard based on
            # https://www.gnu.org/software/libc/manual/html_node/Argument-Syntax.html
            if len(option) > 1:
                rocprof_sys_cmd.append(f"--{option}={option_val}")
            else:
                rocprof_sys_cmd += [f"-{option}", option_val]
    if "env" in rocprof_sys_config:
        rocprof_sys_env = rocprof_sys_config["env"]
    rocprof_sys_cmd.append("--")
    return rocprof_sys_cmd, rocprof_sys_env


class RocProfSysPlugin(RuntimeEnvPlugin):
    name = "_rocprof_sys"

    def __init__(self, resources_dir: str):
        self.rocprof_sys_cmd = []
        self.rocprof_sys_env = {}

        # replace this with better way to get logs dir
        session_dir, runtime_dir = os.path.split(resources_dir)
        self._rocprof_sys_dir = Path(session_dir) / "logs" / "rocprof_sys"
        try_to_create_directory(self._rocprof_sys_dir)

    async def _check_rocprof_sys_script(
        self, rocprof_sys_config: Dict[str, str]
    ) -> Tuple[bool, str]:
        """
        Function to validate if rocprof_sys_config is a valid rocprof_sys profile options
        Args:
            rocprof_sys_config: dictionary mapping rocprof_sys option to it's value
        Returns:
            a tuple consists of a boolean indicating if the rocprof_sys_config
            is valid option and an error message if the rocprof_sys_config is invalid
        """

        # use empty as rocprof_sys report test filename
        test_folder = str(Path(self._rocprof_sys_dir) / "test")
        rocprof_sys_cmd, rocprof_sys_env = parse_rocprof_sys_config(rocprof_sys_config)
        rocprof_sys_env_copy = copy.deepcopy(rocprof_sys_env)
        rocprof_sys_env_copy["ROCPROFSYS_OUTPUT_PATH"] = test_folder
        rocprof_sys_env_copy.update(os.environ)
        try_to_create_directory(test_folder)

        # Create a test python file to run rocprof_sys
        with open(f"{test_folder}/test.py", "w") as f:
            f.write("import time\n")
        try:
            rocprof_sys_cmd = rocprof_sys_cmd + [f"{test_folder}/test.py"]
            process = await asyncio.create_subprocess_exec(
                *rocprof_sys_cmd,
                env=rocprof_sys_env_copy,
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
            return False, ("rocprof_sys is not installed")

    async def create(
        self,
        uri: Optional[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger = default_logger,
    ) -> int:
        rocprof_sys_config = runtime_env.rocprof_sys()
        if not rocprof_sys_config:
            return 0

        if rocprof_sys_config and sys.platform != "linux":
            raise RuntimeEnvSetupError("rocprof-sys CLI is only available in Linux.\n")

        if isinstance(rocprof_sys_config, str):
            if rocprof_sys_config == "default":
                rocprof_sys_config = ROCPROFSYS_DEFAULT_CONFIG
            else:
                raise RuntimeEnvSetupError(
                    f"Unsupported rocprof_sys config: {rocprof_sys_config}. "
                    "The supported config is 'default' or "
                    "Dictionary of rocprof_sys options"
                )

        is_valid_rocprof_sys_config, error_msg = await self._check_rocprof_sys_script(
            rocprof_sys_config
        )
        if not is_valid_rocprof_sys_config:
            logger.warning(error_msg)
            raise RuntimeEnvSetupError(
                "rocprof-sys profile failed to run with the following "
                f"error message:\n {error_msg}"
            )
        # add set output path to logs dir
        if "env" not in rocprof_sys_config:
            rocprof_sys_config["env"] = {}
        rocprof_sys_config["env"]["ROCPROFSYS_OUTPUT_PATH"] = str(
            Path(self._rocprof_sys_dir)
        )

        self.rocprof_sys_cmd, self.rocprof_sys_env = parse_rocprof_sys_config(
            rocprof_sys_config
        )
        return 0

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        logger.info("Running rocprof-sys profiler")
        context.py_executable = " ".join(self.rocprof_sys_cmd)
        context.env_vars.update(self.rocprof_sys_env)
