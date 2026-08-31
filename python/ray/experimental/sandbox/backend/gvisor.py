import json
import logging
import os
import shlex
import shutil
import signal
import subprocess
import time
import uuid
from typing import Callable, Dict, List, Optional, Union

from ray.experimental.sandbox._internal.idmap import (
    IdMap,
    detect_idmap,
    remove_tree_as_mapped_root,
)
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

# network="public" gives each sandbox a private user+network namespace pair
# bridged by pasta (passt) user-mode networking, the rootless Podman shape:
# a holder process (`unshare --user --map-root-user --net`) pins the
# namespaces; pasta attaches to them from the pod side, so its uplink is the
# pod's interface, and runs in the foreground inside the sandbox's process
# group; runsc runs inside via nsenter as mapped root. runsc still gets
# --network=host, but "host" is now private to the sandbox: binds cannot
# collide with or be reached by the pod or other sandboxes, while egress
# leaves through pasta's tap. Mount and pid namespaces stay shared, so the
# bundle and runsc's control sockets under _RUNSC_ROOT keep working for
# pod-side state/exec/kill/delete.
#
# Multi-uid nodes (see _internal/idmap.py) use the same holder + nsenter
# shape for every rootless sandbox, network namespace or not, so that runsc
# runs as mapped root in a user namespace carrying the node's subordinate id
# range; single-uid nodes wrap only network="public".
#
# pasta relays every outbound connection through the pod's own sockets, so
# the sandbox can reach any address the pod can reach: other Ray nodes
# (including the head node's GCS and dashboard), other pods, and internal
# services. pasta has no destination filter; network="none" remains the
# boundary for untrusted code.
#
# These flags are the isolation property; tests pin the exact list:
#   --config-net  copy the pod interface's addressing/routes onto the tap.
#   -t/-u none    never republish namespace binds on the pod.
#   -T/-U none    no loopback splicing: pod-local services stay
#                 unreachable from the sandbox's 127.0.0.1.
#   --no-map-gw   don't remap gateway-addressed traffic to the pod loopback.
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
        use_pasta = config.network == "public"
        # Rootless sandboxes map a subordinate id range into their user
        # namespace when the node can (warn-once inside detect_idmap);
        # privileged runsc needs no namespace of ours.
        idmap = detect_idmap() if config.rootless else None
        if use_pasta:
            missing = [b for b in ("pasta", "nsenter") if not shutil.which(b)]
            if missing:
                raise SandboxCreationError(
                    "network='public' isolates each sandbox in its own network "
                    "namespace via pasta (passt), but "
                    f"{', '.join(repr(b) for b in missing)} was not found in "
                    "PATH. Install the passt package (and util-linux) on the "
                    "node image."
                )

        sandbox_uuid = uuid.uuid4().hex[:12]
        sandbox_id = f"ray-sandbox-{sandbox_uuid}"
        root_dir = os.path.join(_RAY_SANDBOX_DIR, sandbox_id)

        try:
            os.makedirs(root_dir, mode=0o777, exist_ok=True)

            # The instance id pins the image in the cache while this sandbox
            # lives (its extracted rootfs is the overlay lower layer).
            self._image_manager.pull_image(
                config.image,
                timeout_seconds=config.timeout_seconds,
                instance_id=sandbox_id,
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
            self._image_manager.release_image(config.image, sandbox_id)
            raise SandboxCreationError(
                f"Failed to initialize local sandbox directory '{root_dir}': {err}"
            ) from err

        # Prepare OCI bundle config for long-running container process
        try:
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
        except Exception:
            self._image_manager.release_image(config.image, sandbox_id)
            raise
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

                # Check the deadline only after polling state, so a sandbox
                # that reached 'running' just as the deadline passed is
                # observed and kept rather than torn down unpolled.
                if time.time() - start_time > timeout:
                    raise SandboxTimeoutError(
                        f"gVisor container '{sandbox_id}' failed to reach 'running' state within {timeout} seconds."
                    )

                time.sleep(0.1)
        except Exception:
            # Delete runsc's container state, then kill the whole group:
            # under pasta, a bare proc.kill() would orphan the namespace
            # holder and pasta.
            self._delete_container_state(config, sandbox_id)
            self._terminate_tree(proc)
            stderr_file.close()
            self._remove_root_dir(root_dir, idmap)
            # The sandbox never registered, so delete_sandbox will not run
            # for it: release the image here to keep it evictable.
            self._image_manager.release_image(config.image, sandbox_id)
            raise

        self._sandbox_metadata[sandbox_id] = {
            "root_dir": root_dir,
            "workdir": workdir_path,
            "cwd": container_cwd,
            "config": config,
            # The process group leader whose tree holds the sandbox and,
            # for network="public", the namespace holder and pasta.
            "proc": proc,
            "stderr_file": stderr_file,
            "status": SandboxStatus.RUNNING,
            "idmap": idmap,
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
            # namespace holder and pasta (network="public") are still alive
            # in it.
            if proc:
                self._terminate_tree(proc)

            if stderr_file:
                try:
                    stderr_file.close()
                except Exception:
                    pass

            self._remove_root_dir(root_dir, meta.get("idmap"))
            # Only now is the overlay's lower layer unused.
            self._image_manager.release_image(config.image, sandbox_id)

    def _remove_root_dir(self, root_dir: str, idmap: Optional[IdMap]) -> None:
        """Remove a sandbox's directory, including subordinate-owned files.

        A multi-uid sandbox can chown files under its workdir bind to ids the
        worker cannot delete from the initial namespace; those go through a
        namespace mapped with the sandbox's ``idmap``.
        """
        shutil.rmtree(root_dir, ignore_errors=True)
        if idmap is not None and os.path.lexists(root_dir):
            remove_tree_as_mapped_root(root_dir, idmap)

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

    def _delete_container_state(self, config: SandboxConfig, sandbox_id: str) -> None:
        """Best-effort ``runsc delete -force`` for teardown paths.

        Bounded by a timeout: a wedged gVisor (the usual reason a create
        timed out) must not block the process-group kill and pasta reap
        that follow, which is what actually frees the sandbox.
        """
        del_args = self._runsc_base_args(config) + ["delete", "-force", sandbox_id]
        try:
            subprocess.run(del_args, capture_output=True, timeout=10)
        except subprocess.TimeoutExpired:
            pass

    def _build_run_command(
        self,
        config: SandboxConfig,
        root_dir: str,
        overlay_dir: str,
        sandbox_id: str,
        idmap: Optional[IdMap] = None,
    ) -> List[str]:
        """Build the full `runsc run` argv, wrapped in a user namespace when needed.

        Pure argv construction (no filesystem side effects) so tests can
        assert the exact command without runsc or pasta installed.

        A bare ``runsc --rootless`` invocation is enough for a single-uid
        sandbox on the worker's network. Two features need runsc to run as
        mapped root inside a user namespace we own: ``network="public"``
        (the namespace also carries the private network namespace pasta
        bridges) and multi-uid ``idmap`` (the namespace maps the node's
        subordinate id range via newuidmap/newgidmap). runsc drops
        ``--rootless`` there because nesting a second user namespace breaks
        the gofer's /proc magic-link derefs, and gains ``--ignore-cgroups``
        to keep rootless mode's tolerance of cgroup permission failures.
        """
        args = self._runsc_base_args(config)
        use_pasta = config.network == "public"
        wrap = config.rootless and (use_pasta or idmap is not None)
        if wrap:
            args = [a for a in args if a != "--rootless"]
            if "--ignore-cgroups" not in args:
                args.insert(1, "--ignore-cgroups")
        if config.network:
            # "public" = host egress + generated resolv.conf (handled in the
            # OCI bundle); runsc itself just sees host networking: of the
            # per-sandbox namespace when pasta wraps it, of the worker
            # otherwise.
            runsc_network = "host" if config.network == "public" else config.network
            args.extend(["--network", runsc_network])
        args.append(f"--overlay2=root:dir={overlay_dir}")
        args.extend(["run", "--bundle", root_dir, sandbox_id])
        if not wrap:
            return args

        netns_pidfile = shlex.quote(os.path.join(root_dir, "netns.pid"))
        runsc = " ".join(shlex.quote(a) for a in args)
        net_flag = " --net" if use_pasta else ""
        if idmap is not None:
            # The holder starts unmapped (DAC is kuid-based, so writing the
            # pidfile into the 0777 root_dir and sleeping both work; ids
            # merely read as the overflow uid until mapped). The maps are
            # written exactly once into the fresh uid_map/gid_map: container
            # root onto the worker's own ids, 1..count onto the subordinate
            # range. Plain --user never writes setgroups=deny, so newgidmap
            # works. &&-chaining surfaces a map failure through
            # runsc.stderr.log.
            holder = f"unshare --user{net_flag} --fork --kill-child "
            maps = (
                f"newuidmap $NSPID 0 {idmap.euid} 1"
                f" 1 {idmap.subuid_base} {idmap.subuid_count} && "
                f"newgidmap $NSPID 0 {idmap.egid} 1"
                f" 1 {idmap.subgid_base} {idmap.subgid_count} && "
            )
        else:
            holder = f"unshare --user --map-root-user{net_flag} --fork --kill-child "
            maps = ""
        pasta_part = ""
        if use_pasta:
            pasta_pidfile = shlex.quote(os.path.join(root_dir, "pasta.pid"))
            pasta = " ".join(["pasta", *_PASTA_FLAGS])
            pasta_part = (
                # pasta attaches from the pod side and stays in the
                # foreground, so it lives and dies with this process group.
                # It writes --pid once initialised: that is the go signal.
                f"{pasta} --foreground --pid {pasta_pidfile} "
                "--netns /proc/$NSPID/ns/net --userns /proc/$NSPID/ns/user & "
                "PASTA=$!; "
                f"for i in $(seq 1 100); do [ -s {pasta_pidfile} ] && break; "
                "kill -0 $PASTA 2>/dev/null || break; sleep 0.1; done; "
                f'[ -s {pasta_pidfile} ] || {{ echo "pasta failed to start" >&2; exit 1; }}; '
            )
        enter = "-U -n" if use_pasta else "-U"
        script = (
            # The holder pins the namespaces for the sandbox's lifetime;
            # --kill-child ties it to this script's process group.
            f"{holder}"
            f"bash -c 'echo $$ > {netns_pidfile}; exec sleep infinity' & "
            "HOLDER=$!; "
            # Stop waiting as soon as the holder dies, and refuse an empty
            # NSPID (which would resolve to /proc//ns/net).
            f"for i in $(seq 1 100); do [ -s {netns_pidfile} ] && break; "
            "kill -0 $HOLDER 2>/dev/null || break; sleep 0.1; done; "
            f"NSPID=$(cat {netns_pidfile} 2>/dev/null); "
            '[ -n "$NSPID" ] || { echo "netns holder failed to start" >&2; exit 1; }; '
            f"{maps}"
            f"{pasta_part}"
            f"exec nsenter --preserve-credentials {enter} -t $NSPID -- {runsc}"
        )
        return ["bash", "-c", script]

    def _terminate_tree(self, proc: subprocess.Popen) -> None:
        """SIGKILL the sandbox process group and reap the Popen.

        The run Popen is started with ``start_new_session=True``, so its pid
        is the group id for the namespace holder, pasta, runsc run, and the
        sandbox process.
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
