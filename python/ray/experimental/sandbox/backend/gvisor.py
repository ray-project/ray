import json
import logging
import os
import shutil
import subprocess
import time
import uuid
from typing import Callable, Dict, List, Optional, Union

from ray.experimental.sandbox.backend.base import (
    BaseSandboxBackend,
    ExecResult,
    SandboxStatus,
)
from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxError,
    SandboxExecError,
    SandboxNotFoundError,
    SandboxTimeoutError,
)
from ray.experimental.sandbox.image_manager import BaseImageManager

logger = logging.getLogger(__name__)

# Directory where runsc keeps container state. Every runsc invocation for a
# sandbox must agree on this, otherwise the container cannot be looked up.
_RUNSC_ROOT = "/tmp/runsc"

# Directory to store sandbox states, container images and overlay filesystem.
_RAY_SANDBOX_DIR = "/tmp/ray/sandbox"


class GVisorSandboxBackend(BaseSandboxBackend):
    """gVisor sandbox backend running a single persistent container instance per sandbox locally via runsc."""

    def __init__(self, image_manager: Optional[BaseImageManager] = None):
        super().__init__(image_manager=image_manager)
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

            self._image_manager.pull_image(
                config.image, timeout_seconds=config.timeout_seconds
            )
            # The process cwd: an explicit workdir, else the image's WORKDIR.
            container_cwd = (
                config.workdir or self._image_manager.get_workdir(config.image) or "/"
            )

            # A host-backed scratch directory exists only for an *explicitly*
            # requested workdir on a readonly rootfs — the sandbox's single
            # writable path there. A writable rootfs needs none (the overlay
            # covers writes), and an inherited image WORKDIR is never
            # silently made writable.
            workdir_path = None
            if config.workdir and config.readonly:
                workdir_path = os.path.abspath(
                    os.path.join(root_dir, config.workdir.lstrip("/"))
                )
                if not (
                    workdir_path == os.path.abspath(root_dir)
                    or workdir_path.startswith(os.path.abspath(root_dir) + os.sep)
                ):
                    raise SandboxCreationError(
                        f"Invalid workdir '{config.workdir}': Path traversal detected."
                    )
                os.makedirs(workdir_path, mode=0o777, exist_ok=True)
        except Exception as err:
            raise SandboxCreationError(
                f"Failed to initialize local sandbox directory '{root_dir}': {err}"
            ) from err

        # Prepare OCI bundle config for long-running container process
        self._image_manager.prepare_oci_bundle(
            root_dir=root_dir,
            workdir_path=workdir_path,
            container_cwd=container_cwd,
            image=config.image,
            env_dict=config.env,
            cpu=config.cpu,
            memory=config.memory,
            readonly=config.readonly,
            capabilities=config.capabilities,
            network=config.network,
            dns=config.dns,
            _oci_spec_transform_fn=config._oci_spec_transform_fn,
        )
        run_args = self._runsc_base_args(config)
        if config.network:
            # "public" = host egress + generated resolv.conf (handled in the
            # OCI bundle); runsc itself just sees host networking.
            runsc_network = "host" if config.network == "public" else config.network
            run_args.extend(["--network", runsc_network])
        overlay_dir = os.path.join(root_dir, "overlay")
        os.makedirs(overlay_dir, mode=0o777, exist_ok=True)
        run_args.append(f"--overlay2=root:dir={overlay_dir}")
        run_args.extend(["run", "--bundle", root_dir, sandbox_id])

        stderr_log_path = os.path.join(root_dir, "runsc.stderr.log")
        stderr_file = open(stderr_log_path, "w+", encoding="utf-8")
        proc = subprocess.Popen(
            run_args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=stderr_file,
        )
        start_time = time.time()
        timeout = config.timeout_seconds
        state_args = self._runsc_base_args(config) + ["state", sandbox_id]

        try:
            while True:
                if proc.poll() is not None:
                    stderr_file.seek(0)
                    stderr_str = stderr_file.read()
                    raise SandboxCreationError(
                        f"gVisor container failed to start: {stderr_str}"
                    )

                res = subprocess.run(state_args, capture_output=True, text=True)
                if res.returncode == 0:
                    try:
                        state_data = json.loads(res.stdout)
                        status = state_data.get("status")
                        if status == "running":
                            break
                        elif status in ("stopped", "error"):
                            raise SandboxCreationError(
                                f"gVisor container stopped unexpectedly during initialization (status: {status})."
                            )
                    except Exception as e:
                        if isinstance(e, SandboxCreationError):
                            raise
                        pass

                if time.time() - start_time > timeout:
                    proc.kill()
                    proc.communicate()
                    raise SandboxTimeoutError(
                        f"gVisor container '{sandbox_id}' failed to reach 'running' state within {timeout} seconds."
                    )

                time.sleep(0.1)
        except Exception:
            if proc and proc.poll() is None:
                proc.kill()
                try:
                    proc.communicate(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
            stderr_file.close()
            del_args = self._runsc_base_args(config) + ["delete", sandbox_id]
            subprocess.run(del_args, capture_output=True)
            shutil.rmtree(root_dir, ignore_errors=True)
            raise

        self._sandbox_metadata[sandbox_id] = {
            "root_dir": root_dir,
            "workdir": workdir_path,
            "cwd": container_cwd,
            "config": config,
            "proc": proc,
            "stderr_file": stderr_file,
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
            stderr_file = meta.get("stderr_file")

            kill_args = self._runsc_base_args(config)
            kill_args.extend(["kill", sandbox_id, "SIGKILL"])
            try:
                subprocess.run(kill_args, capture_output=True, timeout=5)
            except subprocess.TimeoutExpired:
                pass

            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.communicate(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()

            if stderr_file:
                try:
                    stderr_file.close()
                except Exception:
                    pass

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
        shell: Optional[str] = None,
    ) -> ExecResult:
        """Execute a process inside the running gVisor sandbox instance via runsc exec."""
        meta = self._get_metadata_or_raise(sandbox_id)
        config: SandboxConfig = meta["config"]

        exec_env = {}
        if env:
            exec_env.update(env)

        exec_cwd = cwd or meta["cwd"]

        # Production execution against running container via `runsc exec`
        runsc_args = self._runsc_base_args(config)
        runsc_args.extend(["exec", "-cwd", exec_cwd])
        if env:
            for k, v in env.items():
                runsc_args.extend(["-env", f"{k}={v}"])
        if isinstance(command, list):
            runsc_args.extend([sandbox_id] + command)
        else:
            exec_shell = shell or config.shell
            runsc_args.extend([sandbox_id, exec_shell, "-c", command])

        start_time = time.time()

        try:
            proc = subprocess.Popen(
                runsc_args,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
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
            raise SandboxExecError(f"gVisor exec failed: {err}") from err

    def write_file(
        self, sandbox_id: str, path: str, content: Union[str, bytes]
    ) -> None:
        """Write content to a file inside the local gVisor sandbox directory."""
        meta = self._get_metadata_or_raise(sandbox_id)
        config: SandboxConfig = meta["config"]

        runsc_args = self._runsc_base_args(config)
        exec_cwd = meta["cwd"]
        runsc_args.extend(
            [
                "exec",
                "-cwd",
                exec_cwd,
                sandbox_id,
                "/bin/sh",
                "-c",
                'mkdir -p "$(dirname "$1")" && cat > "$1"',
                "--",
                path,
            ]
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
        exec_cwd = meta["cwd"]
        runsc_args.extend(["exec", "-cwd", exec_cwd, sandbox_id, "cat", "--", path])

        proc = subprocess.Popen(
            runsc_args,
            stdin=subprocess.DEVNULL,
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
        if (
            getattr(config, "_ignore_cgroups", False)
            or os.environ.get("RAY_SANDBOX_IGNORE_CGROUPS") == "1"
        ):
            args.append("--ignore-cgroups")
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
        return self._image_manager.pull_image(image)

    def _prepare_oci_bundle(
        self,
        root_dir: str,
        workdir_path: str,
        container_cwd: str,
        image: str,
        env_dict: Optional[Dict[str, str]] = None,
        cpu: Optional[float] = None,
        memory: Optional[Union[str, int, float]] = None,
        readonly: bool = True,
        capabilities: Optional[List[str]] = None,
        network: str = "none",
        dns: Optional[List[str]] = None,
        _oci_spec_transform_fn: Optional[Callable[[Dict], Optional[Dict]]] = None,
    ) -> str:
        return self._image_manager.prepare_oci_bundle(
            root_dir=root_dir,
            workdir_path=workdir_path,
            container_cwd=container_cwd,
            image=image,
            env_dict=env_dict,
            cpu=cpu,
            memory=memory,
            readonly=readonly,
            capabilities=capabilities,
            network=network,
            dns=dns,
            _oci_spec_transform_fn=_oci_spec_transform_fn,
        )
