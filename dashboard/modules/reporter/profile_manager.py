import asyncio
import subprocess
import sys
from pathlib import Path

import logging

logger = logging.getLogger(__name__)


def _format_failed_pyspy_command(cmd, stdout, stderr) -> str:
    return f"""Failed to execute `{cmd}`.

Note that this command requires `py-spy` to be installed with root permissions. You
can install `py-spy` and give it root permissions as follows:
  $ pip install py-spy
  $ sudo chown root:root `which py-spy`
  $ sudo chmod u+s `which py-spy`

Alternatively, you can start Ray with passwordless sudo / root permissions.

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
        self.profile_dir_path = Path(profile_dir_path)
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
    ) -> (bool, str):
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
