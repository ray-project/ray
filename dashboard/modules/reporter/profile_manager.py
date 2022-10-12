import asyncio
import subprocess
from pathlib import Path

import ray
import logging

logger = logging.getLogger(__name__)

class CpuProfilingManager:
    def __init__(self, profile_dir_path: str):
        self.profile_dir_path = Path(profile_dir_path)
        self.profile_dir_path.mkdir(exist_ok=True)

    async def _check_sudo_required(self, password: str):
        sudo = "sudo" if ray._private.utils.get_user() != "root" else ""
        if sudo:
            if password:
                sudo = f"echo {password} | {sudo} -S"

        # Check is sudo requires a password
        if not password:
            command = f"{sudo} -n true"
        else:
            command = f"{sudo} true"
        sudo_p = await asyncio.create_subprocess_shell(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )
        await sudo_p.wait()
        if sudo_p.returncode != 0:
            stdout, stderr = await sudo_p.communicate()
            raise EnvironmentError(f"Running CPU profiler require a sudo access. Please provide the password or use passwordless sudo. {stdout}\n{stderr}")
        return sudo

    async def trace_dump(self, pid: int, password: str):
        sudo = await self._check_sudo_required(password)
        process = await asyncio.create_subprocess_shell(
            f"{sudo} $(which py-spy) dump -p {pid}",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )
        stdout, stderr = await process.communicate()
        return stdout.decode("utf-8")

    async def cpu_profile(self, pid: int, format="flamegraph", duration: float = 5, password: str = None):
        if format == "flamegraph":
            extension = "svg"
        else:
            extension = "txt"
        sudo = await self._check_sudo_required(password)
        profile_file_path = self.profile_dir_path / f"{format}_{pid}_cpu_profiling.{extension}"
        process = await asyncio.create_subprocess_shell(
            f"{sudo} $(which py-spy) record "
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
