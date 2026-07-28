import json
import logging
import os
import shutil
import subprocess
import time
import uuid
from typing import Dict, List, Optional, Union

from ray.experimental.sandbox.backend.base import (
    BaseSandboxBackend,
    ExecResult,
    SandboxStatus,
)
from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxError,
    SandboxNotFoundError,
    SandboxTimeoutError,
)

logger = logging.getLogger(__name__)


class GVisorSandboxBackend(BaseSandboxBackend):
    """gVisor sandbox backend running a single persistent container instance per sandbox via runsc."""

    def __init__(self, runsc_path_override: Optional[str] = None):
        self._runsc_path_override = runsc_path_override
        self._sandbox_meta: Dict[str, Dict] = {}

    def create_sandbox(self, config: SandboxConfig) -> str:
        """Create a local directory structure and initialize a gVisor sandbox instance."""
        runsc_path = self._runsc_path_override or config.runsc_path
        if self._runsc_path_override is None and not shutil.which(runsc_path):
            raise SandboxCreationError(
                f"gVisor executable '{runsc_path}' not found in PATH. "
                "Please install gVisor (runsc) on the node."
            )

        sandbox_uuid = uuid.uuid4().hex[:12]
        sandbox_id = f"ray-sb-gvisor-{sandbox_uuid}"
        root_dir = os.path.join("/tmp/ray/sandboxes", sandbox_id)

        try:
            os.makedirs(root_dir, mode=0o777, exist_ok=True)
            work_dir_path = os.path.join(root_dir, config.work_dir.lstrip("/"))
            os.makedirs(work_dir_path, mode=0o777, exist_ok=True)
        except Exception as err:
            raise SandboxCreationError(
                f"Failed to initialize local sandbox directory '{root_dir}': {err}"
            ) from err

        proc = None
        if self._runsc_path_override is None:
            # Prepare OCI bundle config for long-running container process
            self._prepare_oci_bundle(
                root_dir=root_dir,
                work_dir_path=work_dir_path,
                container_cwd=config.work_dir,
                runsc_path=runsc_path,
                env_dict=config.env,
            )
            run_args = [runsc_path]
            if config.rootless:
                run_args.append("--rootless")
            if config.network:
                run_args.extend(["--network", config.network])
            run_args.extend(["run", "--bundle", root_dir, sandbox_id])

            proc = subprocess.Popen(
                run_args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            time.sleep(1)  # TODO: remove before merging
            if proc.poll() is not None:
                _, stderr_str = proc.communicate()
                raise SandboxCreationError(
                    f"gVisor container failed to start: {stderr_str.decode('utf-8', errors='replace')}"
                )

        self._sandbox_meta[sandbox_id] = {
            "root_dir": root_dir,
            "work_dir": work_dir_path,
            "config": config,
            "runsc_path": runsc_path,
            "proc": proc,
            "status": SandboxStatus.RUNNING,
        }
        return sandbox_id

    def delete_sandbox(self, sandbox_id: str) -> None:
        """Terminate the sandbox and remove its local directory structure."""
        meta = self._sandbox_meta.pop(sandbox_id, None)
        if meta:
            root_dir = meta["root_dir"]
            runsc_path = meta["runsc_path"]
            config: SandboxConfig = meta["config"]
            proc = meta.get("proc")

            if self._runsc_path_override is None:
                kill_args = [runsc_path]
                if config.rootless:
                    kill_args.append("--rootless")
                kill_args.extend(["kill", sandbox_id, "SIGKILL"])
                subprocess.run(kill_args, capture_output=True)

                if proc and proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.communicate(timeout=2)
                    except subprocess.TimeoutExpired:
                        proc.kill()

                del_args = [runsc_path]
                if config.rootless:
                    del_args.append("--rootless")
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
        meta = self._get_meta_or_raise(sandbox_id)
        config: SandboxConfig = meta["config"]
        runsc_path = meta["runsc_path"]
        root_dir = meta["root_dir"]

        if isinstance(command, list):
            cmd_str = " ".join(command)
        else:
            cmd_str = command

        exec_env = os.environ.copy()
        exec_env.update(config.env)
        if env:
            exec_env.update(env)

        raw_cwd = cwd or config.work_dir
        resolved_cwd = self._resolve_path(root_dir, raw_cwd)
        os.makedirs(resolved_cwd, exist_ok=True)

        if self._runsc_path_override is not None:
            # Fallback for unit test mocking without running container
            cmd_str_resolved = cmd_str
            if raw_cwd != "/" and raw_cwd in cmd_str:
                cmd_str_resolved = cmd_str.replace(raw_cwd, resolved_cwd)
            env_prefix = " ".join(f"{k}='{v}'" for k, v in (env or {}).items())
            wrapped_cmd = (
                f"cd '{resolved_cwd}' && {env_prefix} {cmd_str_resolved}".strip()
            )
            run_args = ["/bin/sh", "-c", wrapped_cmd]
            start_time = time.time()
            proc = subprocess.Popen(
                run_args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=exec_env,
                cwd=root_dir,
            )
            stdout_str, stderr_str = proc.communicate(timeout=timeout)
            duration = time.time() - start_time
            return ExecResult(
                exit_code=proc.returncode,
                stdout=stdout_str,
                stderr=stderr_str,
                duration_seconds=duration,
            )

        # Production execution against running container via `runsc exec`
        exec_id = uuid.uuid4().hex[:8]
        container_out = f"{raw_cwd.rstrip('/')}/.exec_{exec_id}.stdout"
        container_err = f"{raw_cwd.rstrip('/')}/.exec_{exec_id}.stderr"
        host_out = self._resolve_path(root_dir, container_out)
        host_err = self._resolve_path(root_dir, container_err)

        wrapped_cmd = f"({cmd_str}) > '{container_out}' 2> '{container_err}'"

        runsc_args = [runsc_path]
        if config.rootless:
            runsc_args.append("--rootless")
        runsc_args.extend(["exec", "-cwd", raw_cwd])
        if env:
            for k, v in env.items():
                runsc_args.extend(["-env", f"{k}={v}"])
        runsc_args.extend([sandbox_id, "/bin/sh", "-c", wrapped_cmd])

        start_time = time.time()
        try:
            proc = subprocess.Popen(
                runsc_args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=exec_env,
            )
            proc.communicate(timeout=timeout)
            duration = time.time() - start_time

            stdout_str = open(host_out).read() if os.path.exists(host_out) else ""
            stderr_str = open(host_err).read() if os.path.exists(host_err) else ""

            if os.path.exists(host_out):
                try:
                    os.remove(host_out)
                except Exception:
                    pass
            if os.path.exists(host_err):
                try:
                    os.remove(host_err)
                except Exception:
                    pass

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
        meta = self._get_meta_or_raise(sandbox_id)
        target_file = self._resolve_path(meta["root_dir"], path)
        parent_dir = os.path.dirname(target_file)
        os.makedirs(parent_dir, mode=0o755, exist_ok=True)

        if isinstance(content, str):
            with open(target_file, "w", encoding="utf-8") as f:
                f.write(content)
        else:
            with open(target_file, "wb") as f:
                f.write(content)

    def read_file(self, sandbox_id: str, path: str) -> bytes:
        """Read binary content from a file inside the local gVisor sandbox directory."""
        meta = self._get_meta_or_raise(sandbox_id)
        target_file = self._resolve_path(meta["root_dir"], path)
        if not os.path.exists(target_file):
            raise SandboxError(
                f"File not found: '{path}' inside sandbox '{sandbox_id}'"
            )
        with open(target_file, "rb") as f:
            return f.read()

    def get_status(self, sandbox_id: str) -> SandboxStatus:
        """Get operational status of the gVisor sandbox."""
        meta = self._sandbox_meta.get(sandbox_id)
        if meta and os.path.exists(meta["root_dir"]):
            return SandboxStatus.RUNNING
        return SandboxStatus.TERMINATED

    def _resolve_path(self, root_dir: str, relative_or_abs_path: str) -> str:
        clean_path = relative_or_abs_path.lstrip("/")
        return os.path.join(root_dir, clean_path)

    def _get_meta_or_raise(self, sandbox_id: str) -> Dict:
        if sandbox_id not in self._sandbox_meta:
            raise SandboxNotFoundError(f"Sandbox ID '{sandbox_id}' not found.")
        return self._sandbox_meta[sandbox_id]

    def _prepare_oci_bundle(
        self,
        root_dir: str,
        work_dir_path: str,
        container_cwd: str,
        runsc_path: str,
        env_dict: Optional[Dict[str, str]] = None,
    ) -> str:
        config_json_path = os.path.join(root_dir, "config.json")
        rootfs_dir = os.path.join(root_dir, "rootfs")
        os.makedirs(rootfs_dir, exist_ok=True)

        if not os.path.exists(config_json_path):
            subprocess.run([runsc_path, "spec"], cwd=root_dir, check=True)

        with open(config_json_path, "r", encoding="utf-8") as f:
            spec = json.load(f)

        spec["process"]["args"] = ["sleep", "infinity"]
        spec["process"]["cwd"] = container_cwd

        env_list = ["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"]
        if env_dict:
            for k, v in env_dict.items():
                env_list.append(f"{k}={v}")
        spec["process"]["env"] = env_list

        mounts = spec.get("mounts", [])
        existing_dests = {m.get("destination") for m in mounts}

        default_binds = [
            ("/bin", "/bin"),
            ("/usr", "/usr"),
            ("/lib", "/lib"),
            ("/lib64", "/lib64"),
            (container_cwd, work_dir_path),
        ]
        for dest, src in default_binds:
            if dest not in existing_dests and os.path.exists(src):
                mounts.append(
                    {
                        "destination": dest,
                        "type": "bind",
                        "source": src,
                        "options": ["rbind", "rw" if dest == container_cwd else "ro"],
                    }
                )

        spec["mounts"] = mounts

        with open(config_json_path, "w", encoding="utf-8") as f:
            json.dump(spec, f, indent=2)

        return config_json_path
