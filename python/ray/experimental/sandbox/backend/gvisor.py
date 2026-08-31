import json
import logging
import os
import shlex
import shutil
import signal
import subprocess
import time
import uuid
from pathlib import Path
from typing import Callable, Dict, List, Optional, Union

from ray.experimental.sandbox.backend.base import (
    BaseSandboxBackend,
    ExecResult,
    SandboxStatus,
)
from ray.experimental.sandbox._internal.idmap import IdMap, detect_idmap
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

# network="public" gives each sandbox a private user+net namespace pair,
# bridged by pasta (passt) user-mode networking. Topology (the rootless
# Podman shape): a tiny holder process unshares the namespaces and sleeps;
# pasta, launched from the pod side so its uplink is the pod's real
# interface, attaches to the holder's namespaces and daemonizes; runsc runs
# inside via nsenter, mapped to root in the user namespace (so no
# --rootless — runsc would nest a second user namespace whose /proc
# magic-link derefs the gofer cannot perform). runsc still gets
# --network=host, but "host" is now private to this sandbox: a bind on
# 0.0.0.0 cannot collide with or be reached by the pod or other sandboxes,
# while egress flows out through pasta's tap. Mount and pid namespaces stay
# shared, so the bundle and the runsc control sockets under _RUNSC_ROOT
# keep working for pod-side state/exec/kill/delete. The namespaces are
# anonymous — held by the process group, freed with it.
# These flags are the isolation property; tests pin the exact list:
#   --config-net  copy the pod interface's addressing/routes onto the tap.
#   -t/-u none    never republish namespace binds on the pod (the "auto"
#                 default would recreate the cross-sandbox collision).
#   -T/-U none    no loopback splicing: pod-local services stay
#                 unreachable from the sandbox's 127.0.0.1.
#   --no-map-gw   don't remap gateway-addressed traffic to the pod
#                 loopback (closes the remaining sandbox->pod path).
#   -4            IPv4 only, matching the generated resolv.conf.
_PASTA_FLAGS = [
    "--config-net",
    "-t",
    "none",
    "-u",
    "none",
    "-T",
    "none",
    "-U",
    "none",
    "--no-map-gw",
    "-4",
]

# Kill switch: set to "1" on workers to run network="public" sandboxes in
# the worker's own network namespace (the pre-pasta behavior, where binds
# are shared across sandboxes) without a code deploy.
_PUBLIC_HOST_NETNS_ENV = "RAY_SANDBOX_PUBLIC_HOST_NETNS"


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
        use_pasta = self._uses_pasta_netns(config)
        # Multi-uid mapping when the node provides subuid ranges and the
        # setuid helpers; None degrades to the single-uid holder (warn-once
        # inside detect_idmap).
        idmap = detect_idmap() if use_pasta else None
        if use_pasta:
            missing = [b for b in ("pasta", "nsenter") if not shutil.which(b)]
            if missing:
                raise SandboxCreationError(
                    "network='public' isolates each sandbox in its own "
                    "network namespace via pasta (passt), but "
                    f"{', '.join(repr(b) for b in missing)} was not found in "
                    "PATH. Install the passt package (and util-linux) on the "
                    "node image, enable the auto_install_pasta server "
                    f"setting, or set {_PUBLIC_HOST_NETNS_ENV}=1 on workers "
                    "to restore the previous shared-host-network behavior."
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

        # Multi-uid sandboxes mount the ownership-true rootfs variant so
        # ownership baked into the image (distinct uids, setuid dirs)
        # survives; everyone else keeps the shared worker-owned rootfs.
        rootfs_override = None
        if use_pasta and idmap is not None:
            rootfs_override = self._image_manager.ensure_idmapped_rootfs(
                config.image, idmap, timeout_seconds=config.timeout_seconds
            )

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
            rootfs_path=rootfs_override,
            _oci_spec_transform_fn=config._oci_spec_transform_fn,
        )
        overlay_dir = os.path.join(root_dir, "overlay")
        os.makedirs(overlay_dir, mode=0o777, exist_ok=True)
        run_args = self._build_run_command(
            config, root_dir, overlay_dir, sandbox_id, idmap=idmap
        )

        stderr_log_path = os.path.join(root_dir, "runsc.stderr.log")
        stderr_file = open(stderr_log_path, "w+", encoding="utf-8")
        # start_new_session puts the namespace holder, pasta, and runsc run
        # in one process group so cleanup can kill the whole tree; they share
        # the stderr log so startup failures (missing /dev/net/tun, no
        # uplink) surface through the SandboxCreationError path below.
        proc = subprocess.Popen(
            run_args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=stderr_file,
            start_new_session=True,
        )
        start_time = time.time()
        timeout = config.timeout_seconds

        try:
            while True:
                if proc.poll() is not None:
                    stderr_file.seek(0)
                    stderr_str = stderr_file.read()
                    raise SandboxCreationError(
                        f"gVisor container failed to start: {stderr_str}"
                    )

                if time.time() - start_time > timeout:
                    raise SandboxTimeoutError(
                        f"gVisor container '{sandbox_id}' failed to reach 'running' state within {timeout} seconds."
                    )

                state_args = self._runsc_base_args(config) + ["state", sandbox_id]
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

                time.sleep(0.1)
        except Exception:
            # Delete runsc's container state, then kill the whole group:
            # under pasta, a bare proc.kill() would orphan the namespace
            # holder and the pasta daemon.
            self._delete_container_state(config, sandbox_id)
            self._terminate_tree(proc)
            stderr_file.close()
            shutil.rmtree(root_dir, ignore_errors=True)
            raise

        self._sandbox_metadata[sandbox_id] = {
            "root_dir": root_dir,
            "workdir": workdir_path,
            "cwd": container_cwd,
            "config": config,
            # The process group leader whose tree holds the sandbox — and,
            # for network="public", the namespace holder and pasta daemon.
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

            self._delete_container_state(config, sandbox_id)

            # Always take the whole group: after `runsc run` exits, the
            # namespace holder and pasta daemon (network="public") are
            # still alive in it.
            if proc:
                self._terminate_tree(proc)

            if stderr_file:
                try:
                    stderr_file.close()
                except Exception:
                    pass

            shutil.rmtree(root_dir, ignore_errors=True)

    def _resolve_exec_user(self, user: str, image: str) -> str:
        """Turn a user name or uid[:gid] into runsc exec's numeric form.

        runsc only accepts numeric ids; names are resolved against the
        image's own /etc/passwd (and the login group via /etc/group).

        Args:
            user: Numeric uid, "uid:gid", or a user name from the image.
            image: The sandbox's image, locating the extracted rootfs.

        Returns:
            A "uid" or "uid:gid" string runsc accepts.

        Raises:
            SandboxExecError: When a named user is not in the image's
                /etc/passwd.
        """
        head = user.split(":", 1)[0]
        if head.isdigit():
            return user
        rootfs = os.path.join(self._image_manager.get_image_dir(image), "rootfs")
        try:
            passwd = Path(os.path.join(rootfs, "etc", "passwd")).read_text(
                encoding="utf-8", errors="replace"
            )
        except OSError:
            passwd = ""
        for line in passwd.splitlines():
            parts = line.split(":")
            if len(parts) >= 4 and parts[0] == user:
                return f"{parts[2]}:{parts[3]}"
        raise SandboxExecError(
            f"user {user!r} not found in the image's /etc/passwd; "
            "pass a numeric uid or uid:gid instead"
        )

    def exec_command(
        self,
        sandbox_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        shell: Optional[str] = None,
        user: Optional[str] = None,
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
        if user is not None:
            runsc_args.extend(["-user", self._resolve_exec_user(user, config.image)])
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
        self,
        sandbox_id: str,
        path: str,
        content: Union[str, bytes],
        append: bool = False,
    ) -> None:
        """Write (or append) content to a file inside the sandbox."""
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
                'mkdir -p "$(dirname "$1")" && cat >> "$1"'
                if append
                else 'mkdir -p "$(dirname "$1")" && cat > "$1"',
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

    def _uses_pasta_netns(self, config: SandboxConfig) -> bool:
        """Whether this sandbox gets a private network namespace via pasta."""
        return (
            config.network == "public" and os.environ.get(_PUBLIC_HOST_NETNS_ENV) != "1"
        )

    def _delete_container_state(self, config: SandboxConfig, sandbox_id: str) -> None:
        """Best-effort ``runsc delete -force`` for teardown paths."""
        del_args = self._runsc_base_args(config) + ["delete", "-force", sandbox_id]
        subprocess.run(del_args, capture_output=True)

    def _build_run_command(
        self,
        config: SandboxConfig,
        root_dir: str,
        overlay_dir: str,
        sandbox_id: str,
        idmap: Optional[IdMap] = None,
    ) -> List[str]:
        """Build the full `runsc run` argv, pasta-wrapped for network="public".

        Pure argv construction (no filesystem side effects) so tests can
        assert the exact command without runsc or pasta installed. With
        ``idmap``, the holder namespace gets a multi-uid mapping via the
        setuid newuidmap/newgidmap helpers instead of ``--map-root-user``,
        so in-sandbox files can be owned by distinct uids; ``idmap=None``
        keeps the single-uid script byte-identical to before.
        """
        args = self._runsc_base_args(config)
        use_pasta = self._uses_pasta_netns(config)
        if use_pasta:
            # runsc runs as mapped root inside the holder's user namespace;
            # --rootless would nest a second user namespace whose
            # /proc/<pid>/root magic links the gofer cannot dereference.
            args = [a for a in args if a != "--rootless"]
        if config.network:
            # "public" = host egress + generated resolv.conf (handled in the
            # OCI bundle); runsc itself just sees host networking — of the
            # per-sandbox namespace when wrapped, of the worker otherwise.
            runsc_network = "host" if config.network == "public" else config.network
            args.extend(["--network", runsc_network])
        args.append(f"--overlay2=root:dir={overlay_dir}")
        args.extend(["run", "--bundle", root_dir, sandbox_id])
        if use_pasta:
            pidfile = shlex.quote(os.path.join(root_dir, "netns.pid"))
            runsc = " ".join(shlex.quote(a) for a in args)
            pasta = " ".join(["pasta", *_PASTA_FLAGS])
            if idmap is not None:
                # The holder starts unmapped (DAC is kuid-based, so writing
                # the pidfile into the 0777 root_dir and sleeping both work;
                # ids merely read as the overflow uid until mapped). The
                # maps are then written exactly once into the fresh empty
                # uid_map/gid_map — container root onto the worker's own
                # ids, 1..count onto the subordinate range — before pasta
                # and nsenter join as mapped root. Plain --user never
                # writes setgroups=deny, so newgidmap works. &&-chaining
                # surfaces a map failure through runsc.stderr.log. On
                # nodes whose setuid helpers don't elevate (stripped bits),
                # detection selects privileged direct map-file writes
                # instead — shadow's helpers refuse cross-user targets, so
                # sudo-ing them is never an option.
                holder = "unshare --user --net --fork --kill-child "
                if idmap.sudo_mapfile:
                    maps = (
                        'sudo -n sh -c "'
                        f"printf '0 {idmap.euid} 1\n1 {idmap.subuid_base}"
                        f" {idmap.subuid_count}\n' > /proc/$NSPID/uid_map"
                        f" && printf '0 {idmap.egid} 1\n1"
                        f" {idmap.subgid_base} {idmap.subgid_count}\n'"
                        ' > /proc/$NSPID/gid_map" && '
                    )
                else:
                    maps = (
                        f"newuidmap $NSPID 0 {idmap.euid} 1"
                        f" 1 {idmap.subuid_base} {idmap.subuid_count} && "
                        f"newgidmap $NSPID 0 {idmap.egid} 1"
                        f" 1 {idmap.subgid_base} {idmap.subgid_count} && "
                    )
            else:
                holder = "unshare --user --map-root-user --net --fork --kill-child "
                maps = ""
            script = (
                # The holder pins the namespaces for the sandbox's lifetime;
                # --kill-child ties it to this script's process group.
                f"{holder}"
                f"bash -c 'echo $$ > {pidfile}; exec sleep infinity' & "
                f"for i in $(seq 1 100); do [ -s {pidfile} ] && break; sleep 0.1; done; "
                f"NSPID=$(cat {pidfile}); "
                f"{maps}"
                # pasta runs from the pod side (its uplink is the pod's real
                # interface), attaches to the holder's namespaces, and
                # daemonizes; it exits when the namespaces empty.
                f"{pasta} --netns /proc/$NSPID/ns/net --userns /proc/$NSPID/ns/user && "
                f"exec nsenter --preserve-credentials -U -n -t $NSPID -- {runsc}"
            )
            return ["bash", "-c", script]
        return args

    def _terminate_tree(self, proc: subprocess.Popen) -> None:
        """SIGKILL the sandbox process group and reap the Popen.

        The run Popen is started with ``start_new_session=True``, so its pid
        is the group id for pasta, runsc run, and the sandbox process.
        """
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            if proc.poll() is None:
                proc.kill()
        try:
            proc.communicate(timeout=2)
        except (subprocess.TimeoutExpired, ValueError):
            pass

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
