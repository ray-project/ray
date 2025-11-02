import asyncio
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)

DARWIN_SET_CHOWN_CMD = "sudo chown root: `which {profiler}`"
LINUX_SET_CHOWN_CMD = "sudo chown root:root `which {profiler}`"

PROFILER_PERMISSIONS_ERROR_MESSAGE = """
Note that this command requires `{profiler}` to be installed with root permissions. You
can install `{profiler}` and give it root permissions as follows:
  $ pip install {profiler}
  $ {set_chown_command}
  $ sudo chmod u+s `which {profiler}`

Alternatively, you can start Ray with passwordless sudo / root permissions.

"""


def decode(string: Union[str, bytes]):
    if isinstance(string, bytes):
        return string.decode("utf-8")
    return string


def _format_failed_profiler_command(cmd, profiler, stdout, stderr) -> str:
    stderr_str = decode(stderr)
    extra_message = ""

    # If some sort of permission error returned, show a message about how
    # to set up permissions correctly.
    if "permission" in stderr_str.lower():
        set_chown_command = (
            DARWIN_SET_CHOWN_CMD.format(profiler=profiler)
            if sys.platform == "darwin"
            else LINUX_SET_CHOWN_CMD.format(profiler=profiler)
        )
        extra_message = PROFILER_PERMISSIONS_ERROR_MESSAGE.format(
            profiler=profiler, set_chown_command=set_chown_command
        )

    return f"""Failed to execute `{cmd}`.
{extra_message}
=== stderr ===
{decode(stderr)}

=== stdout ===
{decode(stdout)}
"""


# If we can sudo, always try that. Otherwise, py-spy will only work if the user has
# root privileges or has configured setuid on the py-spy script.
async def _can_passwordless_sudo() -> bool:
    try:
        process = await asyncio.create_subprocess_exec(
            "sudo",
            "-n",
            "true",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        return False
    else:
        _, _ = await process.communicate()
        return process.returncode == 0


class CpuProfilingManager:
    def __init__(self, profile_dir_path: str):
        self.profile_dir_path = Path(profile_dir_path)
        self.profile_dir_path.mkdir(exist_ok=True)
        self.profiler_name = "py-spy"

    async def trace_dump(self, pid: int, native: bool = False) -> (bool, str):
        """
        Capture and dump a trace for a specified process.

        Args:
            pid: The process ID (PID) of the target process for trace capture.
            native (bool, optional): If True, includes native (C/C++) stack frames.
                Default is False.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating the success
                of the trace capture operation and a string with the
                trace data or an error message.
        """
        pyspy = shutil.which(self.profiler_name)
        if pyspy is None:
            return False, "Failed to execute: py-spy is not installed"

        cmd = [pyspy, "dump", "-p", str(pid)]
        # We
        if sys.platform == "linux" and native:
            cmd.append("--native")
        if await _can_passwordless_sudo():
            cmd = ["sudo", "-n"] + cmd
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            return False, _format_failed_profiler_command(
                cmd, self.profiler_name, stdout, stderr
            )
        else:
            return True, decode(stdout)

    async def cpu_profile(
        self, pid: int, format="flamegraph", duration: float = 5, native: bool = False
    ) -> (bool, str):
        """
        Perform CPU profiling on a specified process.

        Args:
            pid: The process ID (PID) of the target process to be profiled.
                format (str, optional): The format of the CPU profile output.
                Default is "flamegraph".
            duration (float, optional): The duration of the profiling
                session in seconds. Default is 5 seconds.
            native (bool, optional): If True, includes native (C/C++) stack frames.
                Default is False.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating the success
                of the profiling operation and a string with the
                profile data or an error message.
        """
        pyspy = shutil.which(self.profiler_name)
        if pyspy is None:
            return False, "Failed to execute: py-spy is not installed"

        if format not in ("flamegraph", "raw", "speedscope"):
            return (
                False,
                f"Failed to execute: Invalid format {format}, "
                + "must be [flamegraph, raw, speedscope]",
            )

        if format == "flamegraph":
            extension = "svg"
        else:
            extension = "txt"
        profile_file_path = (
            self.profile_dir_path / f"{format}_{pid}_cpu_profiling.{extension}"
        )
        cmd = [
            pyspy,
            "record",
            "-o",
            profile_file_path,
            "-p",
            str(pid),
            "-d",
            str(duration),
            "-f",
            format,
        ]
        if sys.platform == "linux" and native:
            cmd.append("--native")
        if await _can_passwordless_sudo():
            cmd = ["sudo", "-n"] + cmd
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            return False, _format_failed_profiler_command(
                cmd, self.profiler_name, stdout, stderr
            )
        else:
            return True, open(profile_file_path, "rb").read()


class MemoryProfilingManager:
    def __init__(self, profile_dir_path: str):
        self.profile_dir_path = Path(profile_dir_path) / "memray"
        self.profile_dir_path.mkdir(exist_ok=True)
        self.profiler_name = "memray"

    async def get_profile_result(
        self,
        pid: int,
        profiler_filename: str,
        format: str = "flamegraph",
        leaks: bool = False,
    ) -> (bool, str):
        """
        Convert the Memray profile result to specified format.

        Args:
            pid: The process ID (PID) associated with the profiling operation.
                profiler_filename: The filename of the profiler output to
                be processed.
            format (str, optional): The format of the profile result.
                Default is "flamegraph".
            leaks (bool, optional): If True, include memory leak information in
                the profile result.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating the success
                of the operation and a string with the processed profile result
                or an error message.
        """
        memray = shutil.which(self.profiler_name)
        if memray is None:
            return False, "Failed to execute: memray is not installed"

        profile_file_path = self.profile_dir_path / profiler_filename
        if not Path(profile_file_path).is_file():
            return False, f"Failed to execute: process {pid} has not been profiled"

        profiler_name, _ = os.path.splitext(profiler_filename)
        profile_visualize_path = self.profile_dir_path / f"{profiler_name}.html"
        if format == "flamegraph":
            visualize_cmd = [
                memray,
                "flamegraph",
                "-o",
                profile_visualize_path,
                "-f",
            ]
        elif format == "table":
            visualize_cmd = [
                memray,
                "table",
                "-o",
                profile_visualize_path,
                "-f",
            ]
        else:
            return (
                False,
                f"Failed to execute: Report with format: {format} is not supported",
            )

        if leaks:
            visualize_cmd.append("--leaks")
        visualize_cmd.append(profile_file_path)

        process = await asyncio.create_subprocess_exec(
            *visualize_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            return False, _format_failed_profiler_command(
                visualize_cmd, self.profiler_name, stdout, stderr
            )

        return True, open(profile_visualize_path, "rb").read()

    async def attach_profiler(
        self,
        pid: int,
        native: bool = False,
        trace_python_allocators: bool = False,
        verbose: bool = False,
    ) -> (bool, str):
        """
        Attach a Memray profiler to a specified process.

        Args:
            pid: The process ID (PID) of the target process which
                the profiler attached to.
            native (bool, optional): If True, includes native (C/C++) stack frames.
                Default is False.
            trace_python_allocators (bool, optional): If True, includes Python
                stack frames. Default is False.
            verbose (bool, optional): If True, enables verbose output.
                Default is False.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating the success
                of the operation and a string of a success message or an error message.
        """
        memray = shutil.which(self.profiler_name)
        if memray is None:
            return False, None, "Failed to execute: memray is not installed"

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        profiler_filename = f"{pid}_memory_profiling_{timestamp}.bin"
        profile_file_path = self.profile_dir_path / profiler_filename
        cmd = [memray, "attach", str(pid), "-o", profile_file_path]

        if native:
            cmd.append("--native")
        if trace_python_allocators:
            cmd.append("--trace-python-allocators")
        if verbose:
            cmd.append("--verbose")
        if await _can_passwordless_sudo():
            cmd = ["sudo", "-n"] + cmd

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            return (
                False,
                None,
                _format_failed_profiler_command(
                    cmd, self.profiler_name, stdout, stderr
                ),
            )
        else:
            return (
                True,
                profiler_filename,
                f"Success attaching memray to process {pid}",
            )

    async def detach_profiler(
        self,
        pid: int,
        verbose: bool = False,
    ) -> (bool, str):
        """
        Detach a profiler from a specified process.

        Args:
            pid: The process ID (PID) of the target process the
                profiler detached from.
            verbose (bool, optional): If True, enables verbose output.
                Default is False.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating the success
                of the operation and a string of a success message or an error message.
        """
        memray = shutil.which(self.profiler_name)
        if memray is None:
            return False, "Failed to execute: memray is not installed"

        cmd = [memray, "detach"]
        if verbose:
            cmd.append("--verbose")
        cmd.append(str(pid))

        if await _can_passwordless_sudo():
            cmd = ["sudo", "-n"] + cmd

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            return False, _format_failed_profiler_command(
                cmd, self.profiler_name, stdout, stderr
            )
        else:
            return True, f"Success detaching memray from process {pid}"
