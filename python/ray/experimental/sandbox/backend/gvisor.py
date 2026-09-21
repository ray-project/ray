import ipaddress
import json
import logging
import os
import shlex
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import uuid
from typing import Callable, Dict, List, Optional, Union

from ray.experimental.sandbox.backend.base import (
    BaseSandboxBackend,
    ExecResult,
    SandboxStatus,
)
from ray.experimental.sandbox.config import (
    DEFAULT_PUBLIC_DNS,
    SandboxConfig,
    normalize_cidr_allowlist,
)
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

# Directory for sandbox bundles, cached container images, and per-sandbox
# overlay state.
_RAY_SANDBOX_DIR = "/tmp/ray/sandbox"

# network="public" gives each sandbox a private user+network namespace pair
# bridged by slirp4netns user-mode networking, the rootless-container shape:
# a holder process (`unshare --user --map-root-user --net`) pins the
# namespaces; slirp4netns attaches to them from the pod side, so its uplink
# is the pod's network, and runs in the foreground inside the sandbox's
# process group; runsc runs inside via nsenter as mapped root. runsc still
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
# other pods, and internal services.
#
# cidr_allowlist narrows that. An nftables ruleset installed in the
# sandbox's network namespace (see _render_egress_ruleset) drops every
# locally generated packet whose destination is outside the listed networks
# before it reaches the tap. It is installed from the pod side, between
# slirp4netns coming up and runsc starting, and code inside the sandbox
# cannot reach it: with --network=host gVisor's hostinet passes only
# AF_INET/AF_INET6 stream and datagram sockets to the host, AF_PACKET
# sockets cannot send, and the Sentry's own netlink implementation has no
# netfilter path, so not even CAP_NET_ADMIN inside the sandbox touches the
# rules. The output hook is interface-independent, so added routes or
# addresses do not bypass it either. Without an allowlist there is no
# destination filter; network="none" remains the boundary for untrusted
# code that needs no network at all.
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

# The egress allowlist ruleset lives in the sandbox's root_dir and is loaded
# with `nft -f` inside the sandbox's network namespace.
_EGRESS_RULES_FILE = "egress.nft"
_EGRESS_TABLE = "ray_sandbox_egress"


def _render_egress_ruleset(cidrs: List[str], dns_servers: List[str]) -> str:
    """Render the nftables ruleset that restricts a sandbox's egress to ``cidrs``.

    The ruleset is one transaction: ``flush ruleset`` (the namespace is
    private to the sandbox, so this only ever clears this sandbox's own
    rules) followed by a single ``inet`` table whose output chain drops by
    default and accepts loopback, the listed networks, and DNS (UDP and TCP
    port 53) to the sandbox's resolvers. One rule per network and a numeric
    hook priority keep it valid on every nft from 0.9 on, and nothing
    depends on conntrack, so only nf_tables itself is needed in the kernel.

    Args:
        cidrs: Allowed destination networks (IPv4 or IPv6, any form
            ``normalize_cidr_allowlist`` accepts).
        dns_servers: Resolver addresses the generated resolv.conf lists.

    Returns:
        The ruleset text, ready for ``nft -f``.

    Raises:
        ValueError: If an entry is not an IP address or network.
    """
    lines = [
        "flush ruleset",
        f"table inet {_EGRESS_TABLE} {{",
        "    chain output {",
        "        type filter hook output priority 0; policy drop;",
        '        oif "lo" accept',
    ]
    for cidr in normalize_cidr_allowlist(cidrs):
        family = "ip6" if ipaddress.ip_network(cidr).version == 6 else "ip"
        lines.append(f"        {family} daddr {cidr} accept")
    for server in dns_servers:
        try:
            address = ipaddress.ip_address(str(server).strip())
        except ValueError:
            raise ValueError(
                f"dns entry {server!r} is not an IP address; cidr_allowlist "
                "needs resolver addresses to keep DNS reachable"
            ) from None
        family = "ip6" if address.version == 6 else "ip"
        for proto in ("udp", "tcp"):
            lines.append(f"        {family} daddr {address} {proto} dport 53 accept")
    lines.extend(["    }", "}", ""])
    return "\n".join(lines)


def _write_egress_ruleset(
    root_dir: str, cidrs: List[str], dns: Optional[List[str]]
) -> str:
    """Write the egress ruleset for ``cidrs`` into ``root_dir`` atomically.

    Args:
        root_dir: The sandbox's per-instance directory.
        cidrs: Allowed destination networks.
        dns: The sandbox's ``dns`` setting; None means the public defaults,
            the same rule the generated resolv.conf follows.

    Returns:
        The path of the written ruleset.
    """
    resolvers = list(dns) if dns else list(DEFAULT_PUBLIC_DNS)
    text = _render_egress_ruleset(cidrs, resolvers)
    path = os.path.join(root_dir, _EGRESS_RULES_FILE)
    fd, tmp_path = tempfile.mkstemp(dir=root_dir, prefix=_EGRESS_RULES_FILE + ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    return path


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
            missing = [b for b in ("slirp4netns", "nsenter") if not shutil.which(b)]
            if missing:
                raise SandboxCreationError(
                    "network='public' isolates each sandbox in its own network "
                    "namespace via slirp4netns, but "
                    f"{', '.join(repr(b) for b in missing)} was not found in "
                    "PATH. Install slirp4netns (distro package, or a static "
                    "build from github.com/rootless-containers/slirp4netns) "
                    "and util-linux on the node image."
                )
            if config.cidr_allowlist is not None and not shutil.which("nft"):
                raise SandboxCreationError(
                    "cidr_allowlist restricts a sandbox's egress with an "
                    "nftables ruleset in its network namespace, but 'nft' was "
                    "not found in PATH. Install nftables (for example "
                    "`apt-get install nftables`) on the node image."
                )

        sandbox_uuid = uuid.uuid4().hex[:12]
        sandbox_id = f"ray-sandbox-{sandbox_uuid}"
        root_dir = os.path.join(_RAY_SANDBOX_DIR, sandbox_id)

        try:
            os.makedirs(root_dir, mode=0o777, exist_ok=True)

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
        if config.cidr_allowlist is not None:
            # Loaded by the run command inside the sandbox's network
            # namespace once slirp4netns has brought the tap up.
            try:
                _write_egress_ruleset(root_dir, config.cidr_allowlist, config.dns)
            except Exception as err:
                self._image_manager.release_image(config.image, sandbox_id)
                raise SandboxCreationError(
                    f"Failed to write the egress allowlist for '{root_dir}': {err}"
                ) from err
        run_args = self._build_run_command(config, root_dir, sandbox_id)

        stderr_log_path = os.path.join(root_dir, "runsc.stderr.log")
        stderr_file = open(stderr_log_path, "w+", encoding="utf-8")
        # start_new_session puts the namespace holder, slirp4netns, and runsc run
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
            # under slirp4netns, a bare proc.kill() would orphan the namespace
            # holder and slirp4netns.
            self._delete_container_state(config, sandbox_id)
            self._terminate_tree(proc)
            stderr_file.close()
            shutil.rmtree(root_dir, ignore_errors=True)
            # The sandbox never registered, so delete_sandbox will not run
            # for it: release the image here to keep it evictable.
            self._image_manager.release_image(config.image, sandbox_id)
            raise

        self._sandbox_metadata[sandbox_id] = {
            "root_dir": root_dir,
            "workdir": workdir_path,
            "cwd": container_cwd,
            "config": config,
            # The egress allowlist in force (None: unrestricted); updated by
            # set_egress_allowlist.
            "egress_allowlist": (
                list(config.cidr_allowlist)
                if config.cidr_allowlist is not None
                else None
            ),
            # Serializes set_egress_allowlist: the ruleset file, the nft
            # load, and egress_allowlist must change together.
            "egress_lock": threading.Lock(),
            # The process group leader whose tree holds the sandbox and,
            # for network="public", the namespace holder and slirp4netns.
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
            # namespace holder and slirp4netns (network="public") are still alive
            # in it.
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

    def set_egress_allowlist(self, sandbox_id: str, cidrs: List[str]) -> None:
        """Replace the egress allowlist of a running network="public" sandbox.

        Only a sandbox created with ``cidr_allowlist`` can be changed: its
        ruleset is already in place, so the update is one atomic ``nft -f``
        (flush and reinstall in a single transaction) run inside the
        sandbox's own network namespace, entered the same way the run
        command entered it. Flows the new list no longer permits are not
        reset; they stall from their next packet on.

        Args:
            sandbox_id: Unique string identifier of the sandbox.
            cidrs: The new allowlist; ``ALLOW_ALL_CIDRS`` reopens egress.

        Raises:
            SandboxNotFoundError: If the sandbox is unknown.
            SandboxError: If the sandbox has no allowlist to replace, or the
                ruleset could not be installed.
            ValueError: If an entry is not an IP address or network.
        """
        meta = self._get_metadata_or_raise(sandbox_id)
        if meta.get("egress_allowlist") is None:
            raise SandboxError(
                f"Sandbox '{sandbox_id}' was created without a cidr_allowlist, "
                "so its egress policy is fixed. Create it with cidr_allowlist "
                "(['0.0.0.0/0', '::/0'] for open egress) to change it later."
            )
        normalized = normalize_cidr_allowlist(cidrs)
        config: SandboxConfig = meta["config"]
        root_dir = meta["root_dir"]

        # `flush ruleset` must only ever run inside the sandbox's namespace:
        # refuse without the holder pid rather than fall through anywhere.
        nspid = ""
        try:
            with open(os.path.join(root_dir, "netns.pid"), encoding="utf-8") as f:
                nspid = f.read().strip()
        except OSError:
            pass
        if not nspid.isdigit():
            raise SandboxError(
                f"Sandbox '{sandbox_id}' has no network namespace holder; "
                "cannot update its egress allowlist."
            )

        # One update at a time per sandbox, so two overlapping updates can
        # never leave one list loaded while the other is recorded.
        with meta.setdefault("egress_lock", threading.Lock()):
            rules_path = os.path.join(root_dir, _EGRESS_RULES_FILE)
            try:
                with open(rules_path, encoding="utf-8") as f:
                    previous_rules = f.read()
            except OSError:
                previous_rules = None
            _write_egress_ruleset(root_dir, normalized, config.dns)
            try:
                proc = subprocess.run(
                    [
                        "nsenter",
                        "--preserve-credentials",
                        "-U",
                        "-n",
                        "-t",
                        nspid,
                        "--",
                        "nft",
                        "-f",
                        rules_path,
                    ],
                    capture_output=True,
                    timeout=10,
                )
                failure = (
                    proc.stderr.decode("utf-8", errors="replace").strip()
                    if proc.returncode != 0
                    else None
                )
            except subprocess.TimeoutExpired:
                failure = "nft did not finish within 10 seconds"
            if failure is not None:
                # Keep the file describing the ruleset that is actually in force.
                if previous_rules is not None:
                    with open(rules_path, "w", encoding="utf-8") as f:
                        f.write(previous_rules)
                raise SandboxError(
                    f"Failed to update the egress allowlist of sandbox "
                    f"'{sandbox_id}': {failure}"
                )
            meta["egress_allowlist"] = normalized

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
        timed out) must not block the process-group kill and slirp4netns reap
        that follow, which is what actually frees the sandbox.
        """
        del_args = self._runsc_base_args(config) + ["delete", "-force", sandbox_id]
        try:
            subprocess.run(del_args, capture_output=True, timeout=10)
        except subprocess.TimeoutExpired:
            pass

    def _build_run_command(
        self, config: SandboxConfig, root_dir: str, sandbox_id: str
    ) -> List[str]:
        """Build the full `runsc run` argv, namespace-wrapped for network="public".

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
            egress = ""
            if config.cidr_allowlist is not None:
                rules_file = shlex.quote(os.path.join(root_dir, _EGRESS_RULES_FILE))
                egress = (
                    "nsenter --preserve-credentials -U -n -t $NSPID -- "
                    f"nft -f {rules_file} || "
                    '{ echo "egress allowlist install failed" >&2; exit 1; }; '
                )
            script = (
                # The holder pins the namespaces for the sandbox's lifetime;
                # --kill-child ties it to this script's process group.
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
                # foreground, so it lives and dies with this process group.
                # It writes "1" to --ready-fd once the tap is configured:
                # that is the go signal.
                f"{slirp} --ready-fd=3 --netns-type=path "
                "/proc/$NSPID/ns/net tap0 --userns-path /proc/$NSPID/ns/user "
                f"3>{ready_file} & "
                "SLIRP=$!; "
                f"for i in $(seq 1 100); do [ -s {ready_file} ] && break; "
                "kill -0 $SLIRP 2>/dev/null || break; sleep 0.1; done; "
                f'[ -s {ready_file} ] || {{ echo "slirp4netns failed to start" >&2; exit 1; }}; '
                # The egress allowlist goes in after the tap exists and
                # before anything runs behind it; a failed install aborts
                # the start rather than leaving egress open.
                f"{egress}"
                f"exec nsenter --preserve-credentials -U -n -t $NSPID -- {runsc}"
            )
            return ["bash", "-c", script]
        return args

    def _terminate_tree(self, proc: subprocess.Popen) -> None:
        """SIGKILL the sandbox process group and reap the Popen.

        The run Popen is started with ``start_new_session=True``, so its pid
        is the group id for the namespace holder, slirp4netns, runsc run, and the
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
