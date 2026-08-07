import json
import logging
import os
import shutil
import subprocess
import time
import uuid
from typing import Dict, List, Optional, Union

from ray.experimental.sandbox._internal.image_utils import (
    pull_and_extract_container_image,
)
from ray.experimental.sandbox.backend.base import (
    BaseSandboxBackend,
    ExecResult,
    SandboxStatus,
)
from ray.experimental.sandbox.config import SandboxConfig, parse_memory_bytes
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxError,
    SandboxNotFoundError,
    SandboxTimeoutError,
)

logger = logging.getLogger(__name__)

# Directory where runsc keeps container state. Every runsc invocation for a
# sandbox must agree on this, otherwise the container cannot be looked up.
_RUNSC_ROOT = "/tmp/runsc"

# Directory to store sandbox states, container images and overlay filesystem.
_RAY_SANDBOX_DIR = "/tmp/ray/sandbox"


class GVisorSandboxBackend(BaseSandboxBackend):
    """gVisor sandbox backend running a single persistent container instance per sandbox locally via runsc."""

    def __init__(self):
        self._sandbox_metadata: Dict[str, Dict] = {}

    def create_sandbox(self, config: SandboxConfig) -> str:
        """Create a local directory structure and initialize a gVisor sandbox instance."""
        if not shutil.which("runsc"):
            raise SandboxCreationError(
                "gVisor executable 'runsc' not found in PATH. "
                "Please install gVisor (runsc) on the node."
            )

        sandbox_uuid = uuid.uuid4().hex[:12]
        sandbox_id = f"ray-sandbox-{sandbox_uuid}"
        root_dir = os.path.join(_RAY_SANDBOX_DIR, sandbox_id)

        try:
            os.makedirs(root_dir, mode=0o777, exist_ok=True)

            image_rootfs = self._pull_and_extract_image(config.image)
            if not config.workdir:
                config_json_path = os.path.join(image_rootfs, ".image_config.json")
                if os.path.exists(config_json_path):
                    with open(config_json_path, "r", encoding="utf-8") as f:
                        try:
                            image_cfg = json.load(f)
                            config.workdir = image_cfg.get("config", {}).get(
                                "WorkingDir"
                            )
                        except Exception:
                            pass
                if not config.workdir:
                    config.workdir = "/"

            work_dir_path = os.path.abspath(
                os.path.join(root_dir, config.workdir.lstrip("/"))
            )
            if not (
                work_dir_path == os.path.abspath(root_dir)
                or work_dir_path.startswith(os.path.abspath(root_dir) + os.sep)
            ):
                raise SandboxCreationError(
                    f"Invalid workdir '{config.workdir}': Path traversal detected."
                )
            os.makedirs(work_dir_path, mode=0o777, exist_ok=True)
        except Exception as err:
            raise SandboxCreationError(
                f"Failed to initialize local sandbox directory '{root_dir}': {err}"
            ) from err

        # Prepare OCI bundle config for long-running container process
        self._prepare_oci_bundle(
            root_dir=root_dir,
            work_dir_path=work_dir_path,
            container_cwd=config.workdir,
            image=config.image,
            env_dict=config.env,
            cpu=config.cpu,
            memory=config.memory,
            readonly=config.readonly,
            image_rootfs=image_rootfs,
        )
        run_args = self._runsc_base_args(config)
        if config.network:
            run_args.extend(["--network", config.network])
        overlay_dir = os.path.join(root_dir, "overlay")
        os.makedirs(overlay_dir, mode=0o777, exist_ok=True)
        run_args.append(f"--overlay2=root:dir={overlay_dir}")
        run_args.extend(["run", "--bundle", root_dir, sandbox_id])

        proc = subprocess.Popen(
            run_args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        start_time = time.time()
        timeout = config.timeout_seconds
        state_args = self._runsc_base_args(config) + ["state", sandbox_id]

        while True:
            if proc.poll() is not None:
                _, stderr_str = proc.communicate()
                raise SandboxCreationError(
                    f"gVisor container failed to start: {stderr_str.decode('utf-8', errors='replace')}"
                )

            res = subprocess.run(state_args, capture_output=True, text=True)
            if res.returncode == 0:
                try:
                    state_data = json.loads(res.stdout)
                    if state_data.get("status") == "running":
                        break
                except Exception:
                    pass

            if time.time() - start_time > timeout:
                proc.kill()
                proc.communicate()
                raise SandboxTimeoutError(
                    f"gVisor container '{sandbox_id}' failed to reach 'running' state within {timeout} seconds."
                )

            time.sleep(0.1)

        self._sandbox_metadata[sandbox_id] = {
            "root_dir": root_dir,
            "workdir": work_dir_path,
            "config": config,
            "proc": proc,
            "status": SandboxStatus.RUNNING,
        }
        return sandbox_id

    def delete_sandbox(self, sandbox_id: str) -> None:
        """Terminate the sandbox and remove its local directory structure."""
        meta = self._sandbox_metadata.pop(sandbox_id, None)
        if meta:
            root_dir = meta["root_dir"]
            config: SandboxConfig = meta["config"]
            proc = meta.get("proc")

            kill_args = self._runsc_base_args(config)
            kill_args.extend(["kill", sandbox_id, "SIGKILL"])
            subprocess.run(kill_args, capture_output=True)

            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.communicate(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()

            del_args = self._runsc_base_args(config)
            del_args.extend(["delete", sandbox_id])
            subprocess.run(del_args, capture_output=True)

            shutil.rmtree(root_dir, ignore_errors=True)

    def exec_command(
        self,
        sandbox_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> ExecResult:
        """Execute a process inside the running gVisor sandbox instance via runsc exec."""
        meta = self._get_metadata_or_raise(sandbox_id)
        config: SandboxConfig = meta["config"]

        if isinstance(command, list):
            cmd_str = " ".join(command)
        else:
            cmd_str = command

        exec_env = {}
        if env:
            exec_env.update(env)

        exec_cwd = cwd or config.workdir

        # Production execution against running container via `runsc exec`
        runsc_args = self._runsc_base_args(config)
        runsc_args.extend(["exec", "-cwd", exec_cwd])
        if env:
            for k, v in env.items():
                runsc_args.extend(["-env", f"{k}={v}"])
        if isinstance(command, list):
            runsc_args.extend([sandbox_id] + command)
        else:
            runsc_args.extend([sandbox_id, "/bin/sh", "-c", cmd_str])

        start_time = time.time()

        try:
            proc = subprocess.Popen(
                runsc_args,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=exec_env,
            )
            stdout_str, stderr_str = proc.communicate(timeout=timeout)
            duration = time.time() - start_time

            return ExecResult(
                exit_code=proc.returncode,
                stdout=stdout_str,
                stderr=stderr_str,
                duration_seconds=duration,
            )
        except subprocess.TimeoutExpired as err:
            proc.kill()
            proc.communicate()
            duration = time.time() - start_time

            raise SandboxTimeoutError(
                f"gVisor exec command timed out after {timeout} seconds."
            ) from err
        except Exception as err:
            duration = time.time() - start_time
            raise SandboxError(f"gVisor exec failed: {err}") from err

    def write_file(
        self, sandbox_id: str, path: str, content: Union[str, bytes]
    ) -> None:
        """Write content to a file inside the local gVisor sandbox directory."""
        meta = self._get_metadata_or_raise(sandbox_id)
        config: SandboxConfig = meta["config"]

        runsc_args = self._runsc_base_args(config)
        runsc_args.extend(
            ["exec", sandbox_id, "/bin/sh", "-c", 'cat > "$1"', "--", path]
        )

        content_bytes = content.encode("utf-8") if isinstance(content, str) else content

        proc = subprocess.Popen(
            runsc_args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _, stderr_str = proc.communicate(input=content_bytes)
        if proc.returncode != 0:
            raise SandboxError(
                f"Failed to write file '{path}': {stderr_str.decode('utf-8', errors='replace')}"
            )

    def read_file(self, sandbox_id: str, path: str) -> bytes:
        """Read binary content from a file inside the local gVisor sandbox directory."""
        meta = self._get_metadata_or_raise(sandbox_id)
        config: SandboxConfig = meta["config"]

        runsc_args = self._runsc_base_args(config)
        runsc_args.extend(["exec", sandbox_id, "cat", "--", path])

        proc = subprocess.Popen(
            runsc_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            err = stderr.decode("utf-8", errors="replace")
            raise SandboxError(
                f"File not found or read error '{path}' inside sandbox '{sandbox_id}': {err}"
            )
        return stdout

    def get_status(self, sandbox_id: str) -> SandboxStatus:
        """Get operational status of the gVisor sandbox."""
        meta = self._sandbox_metadata.get(sandbox_id)
        if meta and os.path.exists(meta["root_dir"]):
            return SandboxStatus.RUNNING
        return SandboxStatus.TERMINATED

    def _runsc_base_args(self, config: SandboxConfig) -> List[str]:
        """Build the runsc global flags shared by run/exec/kill/delete."""
        args = ["runsc"]
        if config.rootless:
            args.append("--rootless")
        args.extend(["--root", _RUNSC_ROOT])
        return args

    def _resolve_path(self, root_dir: str, relative_or_abs_path: str) -> str:
        clean_path = relative_or_abs_path.lstrip("/")
        return os.path.join(root_dir, clean_path)

    def _get_metadata_or_raise(self, sandbox_id: str) -> Dict:
        if sandbox_id not in self._sandbox_metadata:
            raise SandboxNotFoundError(f"Sandbox ID '{sandbox_id}' not found.")
        return self._sandbox_metadata[sandbox_id]

    def _pull_and_extract_image(self, image: str) -> str:
        """Pull a container image and extract rootfs to local directory."""
        return pull_and_extract_container_image(image)

    def _prepare_oci_bundle(
        self,
        root_dir: str,
        work_dir_path: str,
        container_cwd: str,
        image: str,
        env_dict: Optional[Dict[str, str]] = None,
        cpu: Optional[float] = None,
        memory: Optional[Union[str, int, float]] = None,
        readonly: bool = True,
        image_rootfs: Optional[str] = None,
    ) -> str:
        config_json_path = os.path.join(root_dir, "config.json")
        rootfs_dir = os.path.join(root_dir, "rootfs")
        os.makedirs(rootfs_dir, exist_ok=True)

        if not os.path.exists(config_json_path):
            subprocess.run(["runsc", "spec"], cwd=root_dir, check=True)

        with open(config_json_path, "r", encoding="utf-8") as f:
            spec = json.load(f)

        if not image_rootfs:
            image_rootfs = self._pull_and_extract_image(image)
        spec["root"]["path"] = image_rootfs
        spec["root"]["readonly"] = readonly

        spec["process"]["args"] = ["sleep", "infinity"]
        spec["process"]["cwd"] = container_cwd

        env_list = [
            "PATH=/home/ray/anaconda3/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        ]
        if env_dict:
            for k, v in env_dict.items():
                env_list.append(f"{k}={v}")
        spec["process"]["env"] = env_list

        mounts = spec.get("mounts", [])
        existing_dests = {m.get("destination") for m in mounts}

        default_binds = [
            (container_cwd, work_dir_path),
        ]
        for dest, src in default_binds:
            if dest != "/" and dest not in existing_dests and os.path.exists(src):
                mounts.append(
                    {
                        "destination": dest,
                        "type": "bind",
                        "source": src,
                        "options": ["rbind", "rw" if dest == container_cwd else "ro"],
                    }
                )

        spec["mounts"] = mounts

        # Configure OCI cgroup resource limits for CPU and memory
        linux_sec = spec.setdefault("linux", {})
        resources = linux_sec.setdefault("resources", {})

        if cpu is not None and cpu > 0:
            period = 100000
            quota = int(cpu * period)
            cpu_res = resources.setdefault("cpu", {})
            cpu_res["period"] = period
            cpu_res["quota"] = quota

        parsed_mem = parse_memory_bytes(memory)
        if parsed_mem is not None and parsed_mem > 0:
            mem_res = resources.setdefault("memory", {})
            mem_res["limit"] = parsed_mem

        config_json_str = json.dumps(spec, indent=2)
        with open(config_json_path, "w", encoding="utf-8") as f:
            f.write(config_json_str)

        return config_json_path
