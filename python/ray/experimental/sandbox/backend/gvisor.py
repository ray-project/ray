import fcntl
import glob
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import threading
import time
import uuid
from typing import IO, Callable, Dict, List, Optional, Union

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

# How long teardown waits for a sandbox's launch process to exit on its own
# after the container stops, before it kills that process.
_TREE_EXIT_GRACE_SECONDS = 2.0

# Each bundle holds a file that its owner keeps open and locked while the
# sandbox lives. The kernel drops the lock when the owner exits or crashes, so
# a backend in any process can find the bundles that nobody owns anymore.
_OWNER_FILE = ".owner"
_BUNDLE_NAME = re.compile(r"ray-sandbox-[0-9a-f]{12}")
# How often a process looks for bundles nobody owns.
_RECLAIM_INTERVAL_SECONDS = 60.0
_reclaim_lock = threading.Lock()
_last_reclaim = float("-inf")

# Directory for sandbox bundles, cached container images, and per-sandbox
# overlay state.
_RAY_SANDBOX_DIR = "/tmp/ray/sandbox"

# network="public" gives each sandbox a private user+network namespace pair
# bridged by slirp4netns user-mode networking, the rootless-container shape:
# a holder process (`unshare --user --map-root-user --net`) pins the
# namespaces; slirp4netns attaches to them from the pod side, so its uplink
# is the pod's network, and runs in the foreground under the sandbox's
# launch shell; runsc runs inside via nsenter as mapped root. runsc still
# gets --network=host, but "host" is now private to the sandbox: binds cannot
# collide with or be reached by the pod or other sandboxes, while egress
# leaves through slirp4netns's tap. Mount and pid namespaces stay shared, so
# the bundle and runsc's control sockets under _RUNSC_ROOT keep working for
# pod-side state/exec/kill/delete.
#
# slirp4netns NATs every flow through a fresh, kernel-assigned host port, so
# flows from different sandboxes can never share a host socket (pasta, which
# preserves UDP source ports with SO_REUSEADDR, delivered one sandbox's
# replies to another when their source ports collided). It relays through
# the pod's own sockets, so the sandbox can reach any address the pod can
# reach: other Ray nodes (including the head node's GCS and dashboard),
# other pods, and internal services. There is no destination filter;
# network="none" remains the boundary for untrusted code.
#
# These flags are the isolation property; tests pin the exact list:
#   --configure              bring the tap up: network + 100, gateway + 2.
#   --cidr=198.18.0.0/24     the RFC 2544 benchmarking range: it is never
#                            routed on the internet and, unlike the
#                            slirp4netns default 10.0.2.0/24, does not
#                            overlap pod or service CIDRs, which would be
#                            on-link in the sandbox instead of NAT'd.
#   --mtu=65520              the largest MTU slirp4netns supports.
#   --disable-host-loopback  no path from the sandbox to the pod's loopback.
#   --disable-dns            no built-in resolver: the sandbox sees only the
#                            generated resolv.conf, nothing of the host's.
#   --enable-seccomp         a syscall filter on the slirp4netns process.
#                            (--enable-sandbox is not used: its setegid(0)
#                            fails inside a --map-root-user namespace.)
_SLIRP4NETNS_FLAGS = [
    "--configure",
    "--cidr=198.18.0.0/24",
    "--mtu=65520",
    "--disable-host-loopback",
    "--disable-dns",
    "--enable-seccomp",
]

# The launch process stays in its owner's process group (see create_sandbox),
# so a Ctrl-C in a driver's terminal, or a Jupyter kernel interrupt, signals
# it too. Neither should stop a sandbox, so every launch command starts with
# this: the shell ignores SIGINT and SIGQUIT, and so do the commands it runs,
# across exec. runsc keeps SIGINT ignored, but Go installs its own SIGQUIT
# handler, so a SIGQUIT (Ctrl-\) still ends runsc and its sandbox, cleanly.
_IGNORE_INTERRUPTS = "trap '' INT QUIT; "


def _runsc_args(rootless: bool, ignore_cgroups: bool) -> List[str]:
    """runsc and the global flags that its run/exec/kill/delete share."""
    args = ["runsc"]
    if rootless:
        args.append("--rootless")
    if ignore_cgroups:
        args.append("--ignore-cgroups")
    args.extend(["--root", _RUNSC_ROOT])
    return args


def _lookup_db_entry(text: str, name: str) -> Optional[List[str]]:
    """Return the fields of the ``name`` entry in passwd- or group-style text."""
    for line in text.splitlines():
        fields = line.split(":")
        if len(fields) >= 3 and fields[0] == name:
            return fields
    return None


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
        if config.network == "public":
            missing = [
                b for b in ("slirp4netns", "nsenter", "setpriv") if not shutil.which(b)
            ]
            if missing:
                raise SandboxCreationError(
                    "network='public' isolates each sandbox in its own network "
                    "namespace via slirp4netns, but "
                    f"{', '.join(repr(b) for b in missing)} was not found in "
                    "PATH. Install slirp4netns (distro package, or a static "
                    "build from github.com/rootless-containers/slirp4netns) "
                    "and util-linux on the node image."
                )

        self._maybe_reclaim_unowned_bundles()
        sandbox_uuid = uuid.uuid4().hex[:12]
        sandbox_id = f"ray-sandbox-{sandbox_uuid}"
        root_dir = os.path.join(_RAY_SANDBOX_DIR, sandbox_id)

        owner_file = None
        try:
            os.makedirs(root_dir, mode=0o777, exist_ok=True)
            owner_file = self._own_bundle(config, root_dir)

            # The instance id pins the image in the cache while this sandbox
            # lives (its EROFS image is the sandbox's root filesystem).
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
            shutil.rmtree(root_dir, ignore_errors=True)
            self._image_manager.release_image(config.image, sandbox_id)
            if owner_file:
                owner_file.close()
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
                disk=config.disk,
            )
        except Exception:
            shutil.rmtree(root_dir, ignore_errors=True)
            self._image_manager.release_image(config.image, sandbox_id)
            owner_file.close()
            raise
        run_args = self._build_run_command(config, root_dir, sandbox_id)

        stderr_log_path = os.path.join(root_dir, "runsc.stderr.log")
        stderr_file = open(stderr_log_path, "w+", encoding="utf-8")
        # The launch process stays in this process's group, so the sandbox
        # can't outlive a Ray worker that owns it: when a worker exits or
        # crashes, the raylet kills the worker's process group. Everything
        # the launch process started dies with it, runsc's sandbox through
        # runsc's parent-death signal and a network="public" sandbox's
        # helpers through setpriv. It ignores SIGINT, so a driver's Ctrl-C
        # doesn't stop it (see _IGNORE_INTERRUPTS). The tree shares the stderr
        # log so startup failures (missing /dev/net/tun, no uplink) surface
        # through the SandboxCreationError path below.
        proc = subprocess.Popen(
            run_args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=stderr_file,
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
            # Delete runsc's container state, then stop the launch process
            # and everything it started.
            self._delete_container_state(config, sandbox_id)
            self._terminate_tree(proc)
            stderr_file.close()
            shutil.rmtree(root_dir, ignore_errors=True)
            # The sandbox never registered, so delete_sandbox will not run
            # for it: release the image here to keep it evictable.
            self._image_manager.release_image(config.image, sandbox_id)
            owner_file.close()
            raise

        self._sandbox_metadata[sandbox_id] = {
            "root_dir": root_dir,
            "workdir": workdir_path,
            "cwd": container_cwd,
            "config": config,
            # The launch process whose tree holds the sandbox and, for
            # network="public", the namespace holder and slirp4netns.
            "proc": proc,
            "stderr_file": stderr_file,
            "owner_file": owner_file,
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

            # After `runsc run` exits, a network="public" sandbox's launch
            # shell still stops and reaps the namespace holder and slirp4netns.
            if proc:
                self._terminate_tree(proc)

            if stderr_file:
                try:
                    stderr_file.close()
                except Exception:
                    pass

            shutil.rmtree(root_dir, ignore_errors=True)
            # Only now is the cached image unused.
            self._image_manager.release_image(config.image, sandbox_id)
            # Last, so no other backend reclaims the bundle while it's
            # being deleted.
            if meta.get("owner_file"):
                meta["owner_file"].close()

    def _read_account_file(self, sandbox_id: str, path: str) -> str:
        """``/etc/passwd`` or ``/etc/group`` as the running sandbox sees it."""
        try:
            return self.read_file(sandbox_id, path).decode("utf-8", errors="replace")
        except SandboxError as err:
            raise SandboxExecError(
                f"cannot read {path} inside the sandbox to resolve a user name "
                f"(pass a numeric uid instead): {err}"
            ) from err

    def _resolve_exec_user(self, sandbox_id: str, user: str) -> str:
        """Turn ``user`` into the numeric ``uid[:gid]`` form runsc exec accepts.

        Names resolve against the ``/etc/passwd`` and ``/etc/group`` inside
        the running sandbox, read through a first exec: that covers users the
        image ships and users added since, and needs no host copy of the root
        filesystem. A named user with no explicit group gets its login group.

        Args:
            sandbox_id: The running sandbox.
            user: ``uid``, ``uid:gid``, or ``name[:group]``.

        Returns:
            A ``uid`` or ``uid:gid`` string runsc accepts.

        Raises:
            SandboxExecError: When a named user or group is unknown to the
                sandbox, or its account files cannot be read.
        """
        name, _, group = user.partition(":")
        if name.isdigit() and (not group or group.isdigit()):
            return user
        uid, login_gid = name, None
        if not name.isdigit():
            passwd = self._read_account_file(sandbox_id, "/etc/passwd")
            entry = _lookup_db_entry(passwd, name)
            if entry is None:
                raise SandboxExecError(
                    f"user {name!r} not found in the sandbox's /etc/passwd; "
                    "pass a numeric uid instead"
                )
            uid = entry[2]
            login_gid = entry[3] if len(entry) > 3 else None
        if group and not group.isdigit():
            groups = self._read_account_file(sandbox_id, "/etc/group")
            entry = _lookup_db_entry(groups, group)
            if entry is None:
                raise SandboxExecError(
                    f"group {group!r} not found in the sandbox's /etc/group; "
                    "pass a numeric gid instead"
                )
            group = entry[2]
        gid = group or login_gid
        return uid if gid is None else f"{uid}:{gid}"

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
            runsc_args.extend(["-user", self._resolve_exec_user(sandbox_id, user)])
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
                'mkdir -p -- "$(dirname -- "$1")" && cat >> "$1"'
                if append
                else 'mkdir -p -- "$(dirname -- "$1")" && cat > "$1"',
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
        return _runsc_args(
            rootless=config.rootless,
            ignore_cgroups=(
                getattr(config, "_ignore_cgroups", False)
                or os.environ.get("RAY_SANDBOX_IGNORE_CGROUPS") == "1"
            ),
        )

    def _delete_container_state(self, config: SandboxConfig, sandbox_id: str) -> None:
        """Best-effort ``runsc delete -force`` for teardown paths.

        Bounded by a timeout: a wedged gVisor (the usual reason a create
        timed out) must not block stopping the launch process, which is what
        actually frees the sandbox.
        """
        del_args = self._runsc_base_args(config) + ["delete", "-force", sandbox_id]
        try:
            subprocess.run(del_args, capture_output=True, timeout=10)
        except subprocess.TimeoutExpired:
            pass

    def _build_run_command(
        self, config: SandboxConfig, root_dir: str, sandbox_id: str
    ) -> List[str]:
        """Build the launch argv: `runsc run` under a shell that ignores SIGINT
        and SIGQUIT, namespace-wrapped for network="public".

        Pure argv construction (no filesystem side effects) so tests can
        assert the exact command without runsc or slirp4netns installed. The
        rootfs and its writable overlay come from the bundle's gVisor
        annotations (see ``ImageManager.create_oci_spec``), so runsc gets no
        ``--overlay2`` flag.
        """
        args = self._runsc_base_args(config)
        use_netns = config.network == "public"
        if use_netns and "--rootless" in args:
            # runsc runs as mapped root inside the holder's user namespace;
            # --rootless would nest a second user namespace whose
            # /proc/<pid>/root magic links the gofer cannot dereference.
            # Rootless mode also tolerates cgroup permission failures, so
            # keep that behavior explicitly.
            args = [a for a in args if a != "--rootless"]
            if "--ignore-cgroups" not in args:
                args.insert(1, "--ignore-cgroups")
        if config.network:
            # "public" = host egress + generated resolv.conf (handled in the
            # OCI bundle); runsc itself just sees host networking — of the
            # per-sandbox namespace when wrapped, of the worker otherwise.
            runsc_network = "host" if config.network == "public" else config.network
            args.extend(["--network", runsc_network])
        args.extend(["run", "--bundle", root_dir, sandbox_id])
        if use_netns:
            netns_pidfile = shlex.quote(os.path.join(root_dir, "netns.pid"))
            ready_file = shlex.quote(os.path.join(root_dir, "slirp4netns.ready"))
            runsc = " ".join(shlex.quote(a) for a in args)
            slirp = " ".join(["slirp4netns", *_SLIRP4NETNS_FLAGS])
            script = (
                _IGNORE_INTERRUPTS
                # This shell stays the parent of the holder, slirp4netns and
                # runsc, and its exit trap kills and reaps the helpers. When it
                # exec'd runsc instead, the helpers outlived their parent and
                # became zombies of the pod's init, which often never reaps
                # them. Killing the holder's sleep (NSPID) lets unshare reap it.
                + "trap 'kill -KILL ${NSPID:-$HOLDER} $SLIRP 2>/dev/null; wait' EXIT; "
                # setpriv --pdeathsig kills each child with this shell, which
                # is what Ray kills when the owning worker exits.
                # The holder pins the namespaces for the sandbox's lifetime;
                # --kill-child ties its sleep to it.
                "setpriv --pdeathsig KILL -- "
                "unshare --user --map-root-user --net --fork --kill-child "
                f"bash -c 'echo $$ > {netns_pidfile}; exec sleep infinity' & "
                "HOLDER=$!; "
                # Stop waiting as soon as the holder dies, and refuse an
                # empty NSPID (which would resolve to /proc//ns/net).
                f"for i in $(seq 1 100); do [ -s {netns_pidfile} ] && break; "
                "kill -0 $HOLDER 2>/dev/null || break; sleep 0.1; done; "
                f"NSPID=$(cat {netns_pidfile} 2>/dev/null); "
                '[ -n "$NSPID" ] || { echo "netns holder failed to start" >&2; exit 1; }; '
                # slirp4netns attaches from the pod side and stays in the
                # foreground, so it lives and dies with this shell.
                # It writes "1" to --ready-fd once the tap is configured:
                # that is the go signal.
                f"setpriv --pdeathsig KILL -- {slirp} --ready-fd=3 --netns-type=path "
                "/proc/$NSPID/ns/net tap0 --userns-path /proc/$NSPID/ns/user "
                f"3>{ready_file} & "
                "SLIRP=$!; "
                f"for i in $(seq 1 100); do [ -s {ready_file} ] && break; "
                "kill -0 $SLIRP 2>/dev/null || break; sleep 0.1; done; "
                f'[ -s {ready_file} ] || {{ echo "slirp4netns failed to start" >&2; exit 1; }}; '
                "nsenter --preserve-credentials -U -n -t $NSPID -- "
                f"setpriv --pdeathsig KILL -- {runsc} & "
                "wait $!"
            )
            return ["bash", "-c", script]
        # The shell execs runsc, so the launch process is runsc itself.
        return ["sh", "-c", _IGNORE_INTERRUPTS + 'exec "$@"', "sh", *args]

    def _terminate_tree(self, proc: subprocess.Popen) -> None:
        """Stop a sandbox's launch process, and everything it started, and reap it.

        Callers stop the container first, so the launch process normally exits
        on its own; a network="public" sandbox's shell reaps its helpers first.
        Otherwise it gets SIGKILL, and the processes it started die with it
        through their parent-death signals.
        """
        try:
            proc.wait(timeout=_TREE_EXIT_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            proc.kill()
        try:
            proc.communicate(timeout=2)
        except (subprocess.TimeoutExpired, ValueError):
            pass

    def _own_bundle(self, config: SandboxConfig, root_dir: str) -> IO[str]:
        """Lock a new bundle's owner file for this backend and return it open.

        The backend keeps the file, and so its lock, until it deletes the
        sandbox. The file is locked before it takes its name, so a backend
        looking for unowned bundles never takes one that's being created. It
        also records what reclaiming the bundle takes.
        """
        path = os.path.join(root_dir, _OWNER_FILE)
        runsc_args = self._runsc_base_args(config)
        owner_file = open(f"{path}.tmp", "x", encoding="utf-8")
        try:
            fcntl.flock(owner_file, fcntl.LOCK_EX)
            # Flags rather than a command line: a backend never runs one that
            # it reads from disk.
            json.dump(
                {
                    "pid": os.getpid(),
                    "image": config.image,
                    "rootless": "--rootless" in runsc_args,
                    "ignore_cgroups": "--ignore-cgroups" in runsc_args,
                },
                owner_file,
            )
            owner_file.flush()
            os.rename(f"{path}.tmp", path)
        except BaseException:
            owner_file.close()
            raise
        return owner_file

    def _maybe_reclaim_unowned_bundles(self) -> None:
        """Start ``_reclaim_unowned_bundles`` in the background, at most once a
        minute per process.

        A create never waits for it: with many dead owners, or a wedged runsc,
        a pass can take a while.
        """
        global _last_reclaim
        with _reclaim_lock:
            now = time.monotonic()
            if now - _last_reclaim < _RECLAIM_INTERVAL_SECONDS:
                return
            _last_reclaim = now
        threading.Thread(
            target=self._reclaim_in_background, name="sandbox-reclaim", daemon=True
        ).start()

    def _reclaim_in_background(self) -> None:
        try:
            self._reclaim_unowned_bundles()
        except Exception:
            logger.exception("Failed to reclaim sandboxes whose owner is gone")

    def _reclaim_unowned_bundles(self) -> List[str]:
        """Delete what sandboxes whose owner is gone left on this node.

        A sandbox that a Ray worker owns stops with the worker (see
        ``create_sandbox``), but its runsc state, its bundle (with the
        writable overlay's backing file) and its image pin stay on the node,
        and the pin keeps the image from being evicted. A sandbox whose owner
        wasn't a Ray worker can still be running. Either way its owner file
        is unlocked, and this backend holds the lock while it reclaims the
        bundle, so no other backend reclaims it too.

        Returns:
            The IDs of the sandboxes reclaimed.
        """
        reclaimed = []
        for root_dir in glob.glob(os.path.join(_RAY_SANDBOX_DIR, "ray-sandbox-*")):
            sandbox_id = os.path.basename(root_dir)
            if not _BUNDLE_NAME.fullmatch(sandbox_id):
                continue
            try:
                if self._reclaim_if_unowned(root_dir, sandbox_id):
                    reclaimed.append(sandbox_id)
            except Exception:
                logger.exception("Failed to reclaim sandbox %s", sandbox_id)
        if reclaimed:
            logger.info("Reclaimed sandboxes whose owner is gone: %s", reclaimed)
        return reclaimed

    def _reclaim_if_unowned(self, root_dir: str, sandbox_id: str) -> bool:
        """Reclaim one bundle if nobody owns it, and return whether it did."""
        try:
            owner_file = open(os.path.join(root_dir, _OWNER_FILE), encoding="utf-8")
        except OSError:
            # Being created, or made by a Ray that didn't lock it.
            return False
        with owner_file:
            try:
                fcntl.flock(owner_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return False  # Its owner is alive.
            if os.fstat(owner_file.fileno()).st_nlink == 0:
                return False  # Its owner deleted it since we opened the file.
            try:
                owner = json.load(owner_file)
            except ValueError:
                owner = None
            if not isinstance(owner, dict):
                owner = {}
            runsc_args = _runsc_args(
                rootless=owner.get("rootless") is True,
                ignore_cgroups=owner.get("ignore_cgroups") is True,
            )
            for action in (
                ["kill", sandbox_id, "SIGKILL"],
                ["delete", "-force", sandbox_id],
            ):
                try:
                    subprocess.run(
                        [*runsc_args, *action], capture_output=True, timeout=10
                    )
                except (OSError, subprocess.TimeoutExpired):
                    pass
            shutil.rmtree(root_dir, ignore_errors=True)
            if isinstance(owner.get("image"), str):
                self._image_manager.release_image(owner["image"], sandbox_id)
        return True

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
        disk: Optional[Union[str, int, float]] = None,
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
            disk=disk,
        )
