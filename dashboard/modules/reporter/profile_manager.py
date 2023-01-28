import asyncio
import subprocess
import sys
from pathlib import Path
from typing import Tuple
try:
    import memray
except ImportError:
    memray = None

import logging

logger = logging.getLogger(__name__)

PYSPY_PERMISSIONS_ERROR_MESSAGE = """
Note that this command requires `py-spy` to be installed with root permissions. You
can install `py-spy` and give it root permissions as follows:
  $ pip install py-spy
  $ sudo chown root:root `which py-spy`
  $ sudo chmod u+s `which py-spy`

Alternatively, you can start Ray with passwordless sudo / root permissions.

"""


def _format_failed_pyspy_command(cmd, stdout, stderr) -> str:
    stderr_str = stderr.decode("utf-8")

    # If some sort of permission error returned, show a message about how
    # to set up permissions correctly.
    extra_message = (
        PYSPY_PERMISSIONS_ERROR_MESSAGE if "permission" in stderr_str.lower() else ""
    )

    return f"""Failed to execute `{cmd}`.
{extra_message}
=== stderr ===
{stderr.decode("utf-8")}

=== stdout ===
{stdout.decode("utf-8")}
"""


def _format_failed_memray_command(cmd, stdout, stderr) -> str:
    return f"""Failed to execute `{cmd}`.

=== stdout ===
{stdout.decode("utf-8")}

=== stderr ===
{stderr.decode("utf-8")}
"""


# If we can sudo, always try that. Otherwise, py-spy will only work if the user has
# root privileges or has configured setuid on the py-spy script.
async def _can_passwordless_sudo() -> bool:
    process = await asyncio.create_subprocess_shell(
        "sudo -n true",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
    )
    _, _ = await process.communicate()
    return process.returncode == 0


class CpuProfilingManager:
    def __init__(self, profile_dir_path: str):
        self.profile_dir_path = Path(profile_dir_path) / "profile"
        self.profile_dir_path.mkdir(exist_ok=True)

    async def trace_dump(self, pid: int, native: bool = False) -> (bool, str):
        cmd = f"$(which py-spy) dump -p {pid}"
        # We
        if sys.platform == "linux" and native:
            cmd += " --native"
        if await _can_passwordless_sudo():
            cmd = "sudo -n " + cmd
        process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            return False, _format_failed_pyspy_command(cmd, stdout, stderr)
        else:
            return True, stdout.decode("utf-8")

    async def cpu_profile(
        self, pid: int, format="flamegraph", duration: float = 5, native: bool = False
    ) -> Tuple[bool, str]:
        if format == "flamegraph":
            extension = "svg"
        else:
            extension = "txt"
        profile_file_path = (
            self.profile_dir_path / f"{format}_{pid}_cpu_profiling.{extension}"
        )
        cmd = (
            f"$(which py-spy) record "
            f"-o {profile_file_path} -p {pid} -d {duration} -f {format}"
        )
        if sys.platform == "linux" and native:
            cmd += " --native"
        if await _can_passwordless_sudo():
            cmd = "sudo -n " + cmd
        process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            return False, _format_failed_pyspy_command(cmd, stdout, stderr)
        else:
            return True, open(profile_file_path, "rb").read()


class MemoryProfilingManager:
    def __init__(self, profile_dir_path: str):
        self.profile_dir_path = Path(profile_dir_path) / "profile"
        self.profile_dir_path.mkdir(exist_ok=True)

    def check_memray_python_version(self) -> Tuple[bool, str]:
        if tuple(map(int, (memray.__version__.split(".")))) < (1, 5, 0):
            return False, (
                f"Current memray version is {memray.__version__}. "
                "Memory profiler feature is only available for "
                "memray >= 1.5.0. Try pip install memray==1.5.0."
            )
        if sys.version_info < (3, 7):
            return False, (
                "Memray is not supported for python version < 3.7."
            )
        return True, ""

    async def attach(self, pid: int, native: bool = False) -> Tuple[bool, str]:
        version_correct, err = self.check_memray_python_version()
        if not version_correct:
            return False, err

        profile_file_path = (
            self.profile_dir_path / f"{pid}_memory_profiling.bin"
        )

        if not profile_file_path.exists():
            cmd = f"$(which memray) attach " f"-o {profile_file_path} {pid}"

            if sys.platform == "linux" and native:
                cmd += " --native"
            if await _can_passwordless_sudo():
                cmd = "sudo -n " + cmd
            process = await asyncio.create_subprocess_shell(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=True,
            )
            stdout, stderr = await process.communicate()
            if process.returncode != 0:
                return False, _format_failed_memray_command(cmd, stdout, stderr)
            return True, "Succeed"
        else:
            return False, (
                "Memory profiler has already attached "
                f"to the process with pid {pid}."
            )

    async def memory_profile(
        self, pid: int, format="flamegraph",
    ) -> Tuple[bool, str]:
        version_correct, err = self.check_memray_python_version()
        if not version_correct:
            return False, err

        profile_file_path = (
            self.profile_dir_path / f"{pid}_memory_profiling.bin"
        )
        # If the file doesn't exist, attach it.
        if not profile_file_path.exists():
            result, output = await self.attach(pid=pid)
            # If the attach fails, fail the API.
            if not result:
                return result, output

        result_file_path = (
            self.profile_dir_path / f"{pid}_{format}_memory_profiling.html"
        )
        cmd = (
            f"$(which memray) flamegraph -o {result_file_path} "
            f"--force {profile_file_path}"
        )
        if await _can_passwordless_sudo():
            cmd = "sudo -n " + cmd
        process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            return False, _format_failed_memray_command(cmd, stdout, stderr)
        return True, open(result_file_path, "rb").read()
