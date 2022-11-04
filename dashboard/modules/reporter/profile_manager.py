import asyncio
import subprocess
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

=== stdout ===
{stdout.decode("utf-8")}

=== stderr ===
{stderr.decode("utf-8")}
"""


class CpuProfilingManager:
    def __init__(self, profile_dir_path: str):
        self.profile_dir_path = Path(profile_dir_path)
        self.profile_dir_path.mkdir(exist_ok=True)

    async def trace_dump(self, pid: int):
        cmd = f"$(which py-spy) dump --native -p {pid}"
        process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            return _format_failed_pyspy_command(cmd, stdout, stderr)
        else:
            return stdout.decode("utf-8")

    async def cpu_profile(self, pid: int, format="flamegraph", duration: float = 5):
        if format == "flamegraph":
            extension = "svg"
        else:
            extension = "txt"
        profile_file_path = (
            self.profile_dir_path / f"{format}_{pid}_cpu_profiling.{extension}"
        )
        cmd = (
            f"$(which py-spy) record --native "
            f"-o {profile_file_path} -p {pid} -d {duration} -f {format}"
        )
        process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            return _format_failed_pyspy_command(cmd, stdout, stderr)
        else:
            return open(profile_file_path).read()
