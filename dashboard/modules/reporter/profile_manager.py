import asyncio
import subprocess
from pathlib import Path

import logging

logger = logging.getLogger(__name__)


class CpuProfilingManager:
    def __init__(self, profile_dir_path: str):
        self.profile_dir_path = Path(profile_dir_path)
        self.profile_dir_path.mkdir(exist_ok=True)

    async def trace_dump(self, pid: int):
        process = await asyncio.create_subprocess_shell(
            f"$(which py-spy) dump --native -p {pid}",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )
        stdout, stderr = await process.communicate()
        return stdout.decode("utf-8")

    async def cpu_profile(self, pid: int, format="flamegraph", duration: float = 5):
        if format == "flamegraph":
            extension = "svg"
        else:
            extension = "txt"
        profile_file_path = (
            self.profile_dir_path / f"{format}_{pid}_cpu_profiling.{extension}"
        )
        process = await asyncio.create_subprocess_shell(
            f"$(which py-spy) record --native "
            f"-o {profile_file_path} -p {pid} -d {duration} -f {format}",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise Exception(f"Failed stdout: {stdout}\nstderr:{stderr}")
        else:
            return str(profile_file_path)
