import logging
import os
import shutil
import subprocess
import time
import uuid
from typing import Dict, List, Optional, Union

from ray.sandbox.backend.base import BaseSandboxBackend, ExecResult, SandboxStatus
from ray.sandbox.config import GVisorSandboxConfig, SandboxConfig
from ray.sandbox.exceptions import (
    SandboxCreationError,
    SandboxError,
    SandboxNotFoundError,
    SandboxTimeoutError,
)

logger = logging.getLogger(__name__)


class GVisorSandboxBackend(BaseSandboxBackend):
    """gVisor sandbox backend spawning isolated processes locally via the runsc OCI runtime CLI."""

    def __init__(self, runsc_path_override: Optional[str] = None):
        self._runsc_path_override = runsc_path_override
        self._sandbox_meta: Dict[str, Dict] = {}

    def create_sandbox(self, config: SandboxConfig) -> str:
        """Create a local directory structure and initialize a gVisor sandbox instance."""
        if not isinstance(config, GVisorSandboxConfig):
            gvisor_config = GVisorSandboxConfig(
                backend="gvisor",
                image=config.image,
                cpu=config.cpu,
                memory=config.memory,
                env=config.env,
                work_dir=config.work_dir,
                ttl_seconds=config.ttl_seconds,
                labels=config.labels,
                timeout_seconds=config.timeout_seconds,
            )
        else:
            gvisor_config = config

        runsc_path = self._runsc_path_override or gvisor_config.runsc_path
        # Verify runsc availability unless mock override is set
        if self._runsc_path_override is None and not shutil.which(runsc_path):
            raise SandboxCreationError(
                f"gVisor executable '{runsc_path}' not found in PATH. "
                "Please install gVisor (runsc) on the node to use the 'gvisor' backend."
            )

        sandbox_uuid = uuid.uuid4().hex[:12]
        sandbox_id = f"ray-sb-gvisor-{sandbox_uuid}"
        root_dir = os.path.join(
            "/usr/local/google/home/andrewsy/code/ray-sandboxing/sandboxes", sandbox_id
        )

        logger.debug(
            f"Creating gVisor sandbox '{sandbox_id}': root_dir='{root_dir}', runsc_path='{runsc_path}'"
        )
        print(f"Creating gVisor sandbox '{sandbox_id}': root_dir='{root_dir}'")

        try:
            curr = ""
            for part in root_dir.split(os.sep):
                if not part:
                    curr = os.sep
                    continue
                curr = os.path.join(curr, part)
                os.makedirs(curr, mode=0o777, exist_ok=True)
                try:
                    os.chmod(curr, 0o777)
                except Exception:
                    pass

            work_dir_path = os.path.join(root_dir, gvisor_config.work_dir.lstrip("/"))
            os.makedirs(work_dir_path, mode=0o777, exist_ok=True)
            os.chmod(work_dir_path, 0o777)
        except Exception as err:
            raise SandboxCreationError(
                f"Failed to initialize local sandbox directory '{root_dir}': {err}"
            ) from err

        self._sandbox_meta[sandbox_id] = {
            "root_dir": root_dir,
            "work_dir": work_dir_path,
            "config": gvisor_config,
            "runsc_path": runsc_path,
            "status": SandboxStatus.RUNNING,
        }
        return sandbox_id

    def delete_sandbox(self, sandbox_id: str) -> None:
        """Terminate the sandbox and remove its local directory structure."""
        meta = self._sandbox_meta.pop(sandbox_id, None)
        if meta:
            root_dir = meta["root_dir"]
            logger.debug(
                f"Deleting gVisor sandbox '{sandbox_id}': removing root_dir='{root_dir}'"
            )
            print(f"Deleting gVisor sandbox '{sandbox_id}': root_dir='{root_dir}'")
            shutil.rmtree(root_dir, ignore_errors=True)

    def exec_command(
        self,
        sandbox_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> ExecResult:
        """Spawn and execute a process inside the gVisor sandbox via runsc."""
        meta = self._get_meta_or_raise(sandbox_id)
        config: GVisorSandboxConfig = meta["config"]
        runsc_path = meta["runsc_path"]
        root_dir = meta["root_dir"]

        if isinstance(command, list):
            cmd_str = " ".join(command)
        else:
            cmd_str = command

        # Environment & working directory setup
        exec_env = os.environ.copy()
        exec_env.update(config.env)
        if env:
            exec_env.update(env)

        env_prefix = " ".join(f"{k}='{v}'" for k, v in (env or {}).items())
        raw_cwd = cwd or config.work_dir
        resolved_cwd = self._resolve_path(root_dir, raw_cwd)
        os.makedirs(resolved_cwd, exist_ok=True)

        cmd_str_resolved = cmd_str
        if raw_cwd != "/" and raw_cwd in cmd_str:
            cmd_str_resolved = cmd_str.replace(raw_cwd, resolved_cwd)

        # wrapped_cmd = f"cd '{resolved_cwd}' && {env_prefix} {cmd_str_resolved}".strip()
        wrapped_cmd = f"{env_prefix} {cmd_str_resolved}".strip()

        # Build runsc do command
        if self._runsc_path_override is not None:
            # Fallback for unit testing mock when runsc_path_override is provided
            run_args = ["/bin/sh", "-c", wrapped_cmd]
        else:
            run_args = [runsc_path]
            if config.rootless:
                run_args.append("--rootless")
            if config.network:
                run_args.extend(["--network", config.network])
            run_args.extend(
                ["do", "-cwd", resolved_cwd, "--", "/bin/sh", "-c", wrapped_cmd]
            )

        logger.debug(
            f"Executing command in gVisor sandbox '{sandbox_id}': command='{cmd_str}', cwd='{raw_cwd}', timeout={timeout}"
        )
        logger.debug(f"gVisor process arguments: {run_args}")
        print(
            f"Executing command in gVisor sandbox '{sandbox_id}': run_args={run_args}"
        )

        start_time = time.time()
        try:
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
            logger.debug(
                f"gVisor command finished: exit_code={proc.returncode}, duration={duration:.3f}s, stdout='{stdout_str}', stderr='{stderr_str}'"
            )
            print(
                f"gVisor command finished: exit_code={proc.returncode}, duration={duration:.3f}s\n"
                f"  stdout: {stdout_str}\n"
                f"  stderr: {stderr_str}"
            )
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
            logger.warning(
                f"gVisor command timed out after {timeout}s: command='{cmd_str}'"
            )
            print(f"gVisor command timed out after {timeout}s: command='{cmd_str}'")
            raise SandboxTimeoutError(
                f"gVisor exec command timed out after {timeout} seconds."
            ) from err
        except Exception as err:
            duration = time.time() - start_time
            logger.error(f"gVisor exec command failed: {err}")
            print(f"gVisor exec command failed: {err}")
            raise SandboxError(f"gVisor exec failed: {err}") from err

    def write_file(
        self, sandbox_id: str, path: str, content: Union[str, bytes]
    ) -> None:
        """Write content to a file inside the local gVisor sandbox directory."""
        meta = self._get_meta_or_raise(sandbox_id)
        target_file = self._resolve_path(meta["root_dir"], path)
        size_bytes = (
            len(content.encode("utf-8")) if isinstance(content, str) else len(content)
        )
        logger.debug(
            f"Writing file to gVisor sandbox '{sandbox_id}': path='{path}', target='{target_file}', size={size_bytes} bytes"
        )
        print(
            f"Writing file to gVisor sandbox '{sandbox_id}': target='{target_file}' ({size_bytes} bytes)"
        )
        parent_dir = os.path.dirname(target_file)
        os.makedirs(parent_dir, mode=0o755, exist_ok=True)
        try:
            os.chmod(parent_dir, 0o755)
        except Exception:
            pass

        if isinstance(content, str):
            with open(target_file, "w", encoding="utf-8") as f:
                f.write(content)
        else:
            with open(target_file, "wb") as f:
                f.write(content)
        try:
            os.chmod(target_file, 0o644)
        except Exception:
            pass

    def read_file(self, sandbox_id: str, path: str) -> bytes:
        """Read binary content from a file inside the local gVisor sandbox directory."""
        meta = self._get_meta_or_raise(sandbox_id)
        target_file = self._resolve_path(meta["root_dir"], path)
        logger.debug(
            f"Reading file from gVisor sandbox '{sandbox_id}': path='{path}', target='{target_file}'"
        )
        print(
            f"Reading file from gVisor sandbox '{sandbox_id}': target='{target_file}'"
        )
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
