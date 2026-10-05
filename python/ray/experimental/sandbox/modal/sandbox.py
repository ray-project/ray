"""A Modal-compatible Sandbox backed by a Ray actor and a gVisor container."""

import asyncio
import logging
import re
import time
import typing
from pathlib import PurePosixPath
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import ray
from ray.experimental.sandbox.config import DOCKER_DEFAULT_CAPABILITIES
from ray.experimental.sandbox.modal._actor import (
    STDERR_FD,
    STDOUT_FD,
    _SandboxActor,
    build_actor_options,
)
from ray.experimental.sandbox.modal._command import (  # noqa: F401
    ARG_MAX_BYTES,
    validate_exec_args as _validate_exec_args,
)
from ray.experimental.sandbox.modal._sync import (
    needs_ray,
    require_ray_connection,
    synchronize_api,
)
from ray.experimental.sandbox.modal.app import App
from ray.experimental.sandbox.modal.container_process import _ContainerProcess
from ray.experimental.sandbox.modal.exception import (
    ClientClosed,
    ConflictError,
    InvalidError,
    NotFoundError,
    NotSupportedError,
    SandboxTerminatedError,
    SandboxTimeoutError,
    TimeoutError,
    _sandbox_gone_as_not_found,
)
from ray.experimental.sandbox.modal.image import resolve_image
from ray.experimental.sandbox.modal.io_streams import _StreamReader, _StreamWriter
from ray.experimental.sandbox.modal.probe import Probe
from ray.experimental.sandbox.modal.sandbox_fs import _SandboxFilesystem
from ray.experimental.sandbox.modal.stream_type import StreamType
from ray.experimental.sandbox.modal.types import (
    FileWatchEvent,
    FileWatchEventType,
    # Modal's runtime names. Aliased: SandboxRuntime otherwise means Ray's
    # sandbox runtime class, which this module's comments refer to.
    SandboxRuntime as _RuntimeName,
)

logger = logging.getLogger(__name__)

# Used when no image is given, matching the spirit of Modal's default image.
DEFAULT_IMAGE = "python:3.13-slim"

# Modal's default Sandbox lifetime, in seconds.
DEFAULT_TIMEOUT = 300

# Modal's default soft CPU limit sits this many cores above the request.
_DEFAULT_CPU_HEADROOM = 16

# The startup bounds of a sandbox: the timeout of each network request of its
# image pull, and how long runsc may take to report the container running.
# Neither bounds the creation as a whole -- a large image takes as long as its
# layers take to download. Deliberately not derived from Modal's `timeout`,
# which is the Sandbox's lifetime and answers a different question -- a
# sandbox meant to live ten seconds still needs a cold image pull to finish,
# and one meant to live an hour should not wait an hour to start.
# SandboxRuntime.create defaults this to 30s, which is tight for a first pull;
# 120s is what the image layer itself uses when asked directly.
_CREATE_TIMEOUT_SECONDS = 120


def _unsupported(name: str, alternative: str = ""):
    # NotSupportedError subclasses NotImplementedError, so this stays catchable
    # both ways while joining the modal.Error hierarchy.
    suffix = f" {alternative}" if alternative else ""
    return NotSupportedError(
        f"{name} is not supported by the Ray sandbox backend.{suffix}"
    )


def _validate_runtime(runtime: Optional[_RuntimeName]) -> None:
    """Accept Modal's ``runtime`` values that mean gVisor, refuse the rest."""
    runtimes = list(typing.get_args(_RuntimeName))
    if runtime is not None and runtime not in runtimes:
        # Modal's own message, so a typo reads the same on both.
        raise InvalidError(f"runtime must be one of {runtimes}, got {runtime!r}")
    if runtime == "vm":
        raise _unsupported(
            "runtime='vm'", "Every Sandbox here runs under gVisor; use 'gvisor'."
        )


def _reject_unsupported(**kwargs) -> None:
    """Raise for any Modal parameter that has no Ray equivalent."""
    for name, value in kwargs.items():
        if value:
            raise _unsupported(f"The '{name}' parameter")


# How many startup files are pushed at once. Each is its own `runsc exec`, and
# a spawn inside gVisor costs enough that pushing a source tree one file at a
# time is dominated by waiting for the previous one. Bounded rather than
# unlimited: `Image.add_local_dir` expands a tree into one entry per file, so
# an unbounded gather would try to spawn a process per file in the tree.
_STARTUP_PUSH_CONCURRENCY = 16


async def _push_startup_files(sandbox, startup_files) -> None:
    """Copy every ``copy=False`` file into the Sandbox before it runs."""
    if not startup_files:
        return
    limit = asyncio.Semaphore(_STARTUP_PUSH_CONCURRENCY)

    async def push(item):
        async with limit:
            await sandbox.filesystem.copy_from_local(item.local_path, item.remote_path)

    # gather rather than a loop: the files are independent, and the first
    # failure still propagates -- the caller tears the whole sandbox down.
    await asyncio.gather(*(push(item) for item in startup_files))


def _scalar(value: Union[float, int, Tuple, None]) -> Optional[float]:
    """Take the request half of Modal's ``(request, limit)`` resource tuples."""
    if value is None:
        return None
    if isinstance(value, (tuple, list)):
        return value[0] if value else None
    return value


def _limit(value: Union[float, int, Tuple, None]) -> Optional[float]:
    """Take the limit half of Modal's ``(request, limit)`` resource tuples.

    None for a bare value: Modal treats that as a request only, a guaranteed
    minimum the container may burst above, with no hard cap of its own.
    """
    if isinstance(value, (tuple, list)) and len(value) > 1:
        return value[1]
    return None


def _validate_resource(name: str, value: Union[float, int, Tuple, None]) -> None:
    """Modal's checks on a ``(request, limit)`` tuple, with its messages."""
    if not isinstance(value, (tuple, list)) or len(value) < 2:
        return
    request, limit = value[0], value[1]
    if name == "cpu":
        if not request:
            raise InvalidError("CPU request must be a positive number")
        if not limit:
            raise InvalidError("CPU limit must be a positive number")
        if limit < request:
            raise InvalidError(
                f"Cannot specify a CPU limit lower than request: {limit} < {request}"
            )
    elif limit < request:
        raise InvalidError(
            f"Cannot specify a memory limit lower than request: {limit} < {request}"
        )


def _clean_env(env: Optional[Dict[str, Optional[str]]]) -> Dict[str, str]:
    """Drop the None values Modal uses to mean 'leave unset'."""
    if not env:
        return {}
    return {k: v for k, v in env.items() if v is not None}


# Modal's rule for an environment variable name passed to Sandbox.create
# (`_SECRET_KEYNAME_REGEX`). fullmatch where Modal uses `^...$`, which also
# accepts a trailing newline.
_ENV_NAME = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")


def _validate_env(env: Any, what: str, *, check_names: bool) -> None:
    """Refuse an env mapping that the backend would mangle rather than reject.

    The backend writes each pair as ``f"{key}={value}"`` -- into the OCI spec
    for create(), onto the ``runsc exec -env`` command line for exec() -- so a
    key containing ``=`` would set a different variable from the one named,
    and a value that is not a string would be stringified, or fail far from
    the call that passed it.

    Args:
        env: The ``env`` argument as given. None values are dropped before
            checking, as they are before use.
        what: The callee, for Modal's type error message.
        check_names: Apply Modal's name rule, which Modal enforces for
            ``Sandbox.create`` only. Without it, only names that can never
            work are refused.

    Raises:
        InvalidError: ``env`` is not usable.
    """
    if not env:
        return
    type_error = f"the env argument to {what} must be a dict[str, str | None]"
    if not isinstance(env, dict):
        raise InvalidError(type_error)
    kept = {k: v for k, v in env.items() if v is not None}
    if not all(isinstance(k, str) and isinstance(v, str) for k, v in kept.items()):
        raise InvalidError(type_error)
    for key, value in kept.items():
        if check_names:
            if not key:
                raise InvalidError("Secret key name cannot be empty")
            if not _ENV_NAME.fullmatch(key):
                raise InvalidError(
                    f"Secret key name {key!r} is invalid for environment "
                    "variables. Only letters, numbers, and underscores are "
                    "allowed."
                )
        elif not key or "=" in key or "\0" in key:
            raise InvalidError(
                f"Environment variable name {key!r} is invalid: it must be "
                "non-empty and contain no '=' or NUL character."
            )
        if "\0" in value:
            raise InvalidError(
                f"The value of environment variable {key!r} contains a NUL "
                "character, which an environment cannot hold."
            )


# Said once per process. The condition is the default, so a program that
# creates sandboxes in a loop would otherwise repeat this on every one.
_host_network_warned = False


def _warn_host_network() -> None:
    """Warn that the default network mode is not the isolation Modal gives.

    The default is kept at Modal's -- flipping it would silently cut egress out
    from under every ported program -- but the two are not equivalent, and the
    difference is the kind that matters. `block_network=False` maps to
    network="public": the Sandbox gets a network namespace of its own, with
    private ports and loopback, but its egress leaves through this node's own
    sockets with no destination filter. So it still reaches the private ranges
    this node can reach -- other Ray nodes, the head node's GCS among them --
    and the cloud instance-metadata endpoint, which hands out the node's
    credentials. On Modal the same default is an isolated network.
    """
    global _host_network_warned
    if _host_network_warned:
        return
    _host_network_warned = True
    logger.warning(
        "Sandbox egress leaves through this node's network, which is what "
        "block_network=False means on this backend. Code in the Sandbox can "
        "reach private network ranges (including other Ray nodes) and the "
        "cloud instance-metadata endpoint -- unlike Modal, where the default "
        "network is isolated. Pass block_network=True to cut off the network "
        "entirely when running code you do not trust. This is logged once "
        "per process."
    )


class _Sandbox:
    """An isolated container you can run commands in and read files from.

    Create one with :meth:`Sandbox.create`; the constructor is not public.
    """

    def __init__(
        self,
        actor,
        instance_id: str,
        main_exec_id: Optional[str],
        readiness_probe: Optional[Probe] = None,
    ):
        self._actor = actor
        self._object_id = instance_id
        self._main_exec_id = main_exec_id
        self._returncode: Optional[int] = None
        self._filesystem: Optional[_SandboxFilesystem] = None
        # Kept client-side so wait_until_ready() can answer the two cheap cases
        # -- no probe configured, and already ready -- without an RPC.
        self._readiness_probe = readiness_probe
        self._ready = False
        # Why the sandbox ended, as the actor recorded it. Cached so the answer
        # survives the actor being reclaimed by terminate().
        self._exit_reason: Optional[str] = None
        self._actor_released = False
        self._finish_logged = False
        self._detached = False

        self._stdout = None
        self._stderr = None
        self._stdin = None
        if main_exec_id is not None:
            self._bind_main_process(main_exec_id)

    def _bind_main_process(self, main_exec_id: Optional[str]) -> None:
        """Point the Sandbox's own streams at its main process.

        Called once the main process exists, which is after any
        ``copy=False`` files have been pushed into the sandbox.
        """
        if main_exec_id is None:
            return
        self._main_exec_id = main_exec_id
        self._stdout = _StreamReader(
            self._actor, main_exec_id, STDOUT_FD, text=True, by_line=True
        )
        self._stderr = _StreamReader(
            self._actor, main_exec_id, STDERR_FD, text=True, by_line=True
        )
        self._stdin = _StreamWriter(self._actor, main_exec_id)

    def __repr__(self) -> str:
        return f"Sandbox(object_id={self._object_id!r})"

    # -- construction ------------------------------------------------------

    @staticmethod
    @needs_ray
    async def create(
        *args: str,
        # Only None is accepted; see the rejection below.
        app: Optional[Any] = None,
        name: Optional[str] = None,
        image: Optional[Any] = None,
        env: Optional[Dict[str, Optional[str]]] = None,
        timeout: Optional[int] = DEFAULT_TIMEOUT,
        workdir: Optional[str] = None,
        gpu: Optional[str] = None,
        cpu: Optional[Union[float, Tuple[float, float]]] = None,
        memory: Optional[Union[int, Tuple[int, int]]] = None,
        runtime: Optional[_RuntimeName] = None,
        block_network: bool = False,
        verbose: bool = False,
        # Accepted so that ported code fails loudly rather than silently. This
        # list is exhaustive over Modal's Sandbox.create signature on purpose:
        # there is no **kwargs behind it, so a Modal-only parameter raises
        # NotSupportedError here rather than surfacing as an unexpected-keyword
        # TypeError from SandboxConfig three layers down.
        tags: Optional[Dict[str, str]] = None,
        secrets: Optional[Sequence[Any]] = None,
        network_file_systems: Optional[Dict[Any, Any]] = None,
        volumes: Optional[Dict[Any, Any]] = None,
        idle_timeout: Optional[int] = None,
        cloud: Optional[str] = None,
        region: Optional[Union[str, Sequence[str]]] = None,
        pty: bool = False,
        encrypted_ports: Sequence[int] = (),
        h2_ports: Sequence[int] = (),
        unencrypted_ports: Sequence[int] = (),
        custom_domain: Optional[str] = None,
        proxy: Optional[Any] = None,
        readiness_probe: Optional[Any] = None,
        outbound_cidr_allowlist: Optional[Sequence[str]] = None,
        outbound_domain_allowlist: Optional[Sequence[str]] = None,
        inbound_cidr_allowlist: Optional[Sequence[str]] = None,
        cidr_allowlist: Optional[Sequence[str]] = None,
        _experimental_outbound_policy: Optional[Any] = None,
        include_oidc_identity_token: bool = False,
        experimental_options: Optional[Dict[str, Any]] = None,
        _experimental_enable_snapshot: bool = False,
        client: Optional[Any] = None,
        environment_name: Optional[str] = None,
        pty_info: Optional[Any] = None,
    ) -> "_Sandbox":
        """Create a Sandbox to run untrusted code in.

        Args:
            *args: Command to run as the Sandbox's main process. When given,
                the Sandbox's ``stdout``/``stderr``/``stdin`` and its exit code
                belong to this command. When omitted, the Sandbox stays idle
                until its timeout or an explicit ``terminate()``.
            app: An App from ``App.lookup()``. Accepted, as Modal requires
                one, and ignored: the handle owns the sandbox here.
            name: Accepted for Modal compatibility and ignored.
            image: Container image, as an :class:`Image` or a reference string.
            env: Environment variables for the Sandbox. None values are
                dropped, matching Modal. Names must be letters, digits and
                underscores, not starting with a digit, as on Modal.
            timeout: Maximum lifetime in seconds. Defaults to 300. Pass None to
                disable.
            workdir: Working directory for commands. Must be absolute.
            gpu: Unsupported: any value raises. The backend cannot pass a
                device into the container, so a reservation would be wasted.
            cpu: CPU cores, as a number or a ``(request, limit)`` tuple. The
                request is reserved from Ray and the sandbox may burst above
                it, up to the limit if given, else 16 cores above the request,
                as on Modal.
            memory: Memory in MiB, as a number or a ``(request, limit)`` tuple.
                The request is reserved from Ray; only a given limit caps the
                container, as on Modal.
            runtime: ``"gvisor"`` or None, which mean the same here: every
                Sandbox runs under gVisor. ``"vm"`` is unsupported.
            block_network: Cut off network access entirely. When False (the
                default) the Sandbox gets a network namespace and loopback of
                its own, with no path to the node's loopback, but its egress
                leaves through this node's sockets with no destination filter:
                beyond the public internet, it reaches the private ranges this
                node can reach -- Ray nodes' ports on their network addresses
                among them -- and any cloud instance-metadata endpoint. Set
                this to True when running code you do not trust.
            verbose: Log sandbox setup at INFO level.
            tags: Unsupported.
            secrets: Unsupported.
            network_file_systems: Unsupported.
            volumes: Unsupported.
            idle_timeout: Unsupported.
            cloud: Unsupported.
            region: Unsupported.
            pty: Unsupported.
            encrypted_ports: Unsupported.
            h2_ports: Unsupported.
            unencrypted_ports: Unsupported.
            custom_domain: Unsupported.
            proxy: Unsupported.
            readiness_probe: A :class:`Probe` deciding when the Sandbox is
                ready. The check runs inside the Sandbox from the moment its
                main process starts; block on it with
                :meth:`wait_until_ready`.
            outbound_cidr_allowlist: Unsupported: network access is
                all-or-nothing here.
            outbound_domain_allowlist: Unsupported, as above.
            inbound_cidr_allowlist: Unsupported, as above.
            cidr_allowlist: Unsupported, as above. Modal's older spelling of
                ``outbound_cidr_allowlist``.
            _experimental_outbound_policy: Unsupported, as above: outbound
                requests are not rewritten here.
            include_oidc_identity_token: Unsupported.
            experimental_options: Unsupported.
            _experimental_enable_snapshot: Unsupported.
            client: Unsupported.
            environment_name: Unsupported.
            pty_info: Unsupported. Deprecated in Modal in favour of ``pty``.

        The rootfs is always writable, as on Modal. Writes land in a
        per-sandbox copy-on-write overlay, so the base image is never modified
        and sandboxes sharing an image cannot see each other's changes.

        Every parameter marked unsupported exists only to reject a Modal
        feature this backend does not implement; see the package docstring.

        Returns:
            A running :class:`Sandbox`.

        Raises:
            InvalidError: An argument is not usable, such as an ``env`` that
                is not a dict of strings or names a variable Modal would refuse,
                or a ``runtime`` Modal does not know.
            NotImplementedError: A Modal-only parameter was passed.
            ValueError: ``app`` was never initialized with ``App.lookup()``.
        """
        # Modal requires an App, so every Modal Sandbox program passes one.
        # It is accepted and then ignored, as name= is: an App is Modal's unit
        # of ownership and billing, and here the handle owns the sandbox. What
        # Modal refuses is refused the same way -- an App that was never
        # initialized, which Modal can only get from lookup() or run().
        if app is not None:
            if not isinstance(app, App):
                raise InvalidError(
                    f"app must be a modal.App, got {type(app).__name__}."
                )
            if app.app_id is None:
                raise ValueError(
                    "App has not been initialized yet. To create an App "
                    "lazily, use `App.lookup`: \n"
                    "app = modal.App.lookup('my-app', create_if_missing=True)\n"
                    "modal.Sandbox.create('echo', 'hi', app=app)"
                )
        _reject_unsupported(
            tags=tags,
            secrets=secrets,
            network_file_systems=network_file_systems,
            volumes=volumes,
            idle_timeout=idle_timeout,
            cloud=cloud,
            region=region,
            encrypted_ports=tuple(encrypted_ports),
            h2_ports=tuple(h2_ports),
            unencrypted_ports=tuple(unencrypted_ports),
            custom_domain=custom_domain,
            proxy=proxy,
            include_oidc_identity_token=include_oidc_identity_token,
            experimental_options=experimental_options,
            _experimental_enable_snapshot=_experimental_enable_snapshot,
            client=client,
            environment_name=environment_name,
            pty_info=pty_info,
        )
        # Separated from the block above only for the message. Silently
        # ignoring an egress restriction is the one rejection with a security
        # consequence: block_network=False egresses through the node with no
        # destination filter, so an unenforced allowlist leaves the sandbox
        # reaching private ranges and cloud instance metadata.
        # `allowlist_name`, not `name`: `name` is one of this method's own
        # parameters, and rebinding it here left it holding a leftover string
        # for the rest of the call.
        #
        # `is not None`, not truthiness: an empty allowlist is Modal's way of
        # allowing nothing, and treating it as absent granted open egress.
        for allowlist_name, value in (
            ("outbound_cidr_allowlist", outbound_cidr_allowlist),
            ("outbound_domain_allowlist", outbound_domain_allowlist),
            ("inbound_cidr_allowlist", inbound_cidr_allowlist),
            ("cidr_allowlist", cidr_allowlist),
            ("_experimental_outbound_policy", _experimental_outbound_policy),
        ):
            if value is not None:
                raise _unsupported(
                    f"The '{allowlist_name}' parameter",
                    "Network access is all-or-nothing here: egress would be "
                    "left unrestricted rather than filtered. Use "
                    "block_network=True to cut off the network entirely.",
                )
        if pty:
            raise _unsupported(
                "The 'pty' parameter",
                "The gVisor backend does not allocate a terminal for exec'd "
                "commands.",
            )
        _validate_runtime(runtime)
        if gpu:
            # Reserving num_gpus without plumbing a device into the container is
            # the worst of both worlds: the GPU is taken from Ray's scheduler and
            # the workload still cannot see it. Fail rather than waste it.
            raise _unsupported(
                "The 'gpu' parameter",
                "Ray does not currently pass a GPU device into the "
                "sandbox, so the reservation would be consumed but unusable.",
            )
        if args:
            _validate_exec_args(args)
        if workdir is not None and not workdir.startswith("/"):
            raise InvalidError("workdir must be an absolute path.")
        _validate_env(env, "Sandbox", check_names=True)
        if readiness_probe is not None and not isinstance(readiness_probe, Probe):
            raise InvalidError(
                "readiness_probe must be a Probe, got "
                f"{type(readiness_probe).__name__}. Build one with "
                "Probe.with_exec(...)."
            )

        # Note this is not restored: raising the level for one sandbox raises it
        # for every later one in the process. Logging a single extra line
        # directly keeps `verbose` scoped to the call that asked for it.
        if verbose:
            logger.info("Creating sandbox (verbose): image=%r", image)

        # Every check above is pure, so nothing has happened yet that a refused
        # call should have paid for -- which is the whole reason the connection
        # is asked for here and not before validation. On the blocking surface
        # this hands the connect back to the caller's thread and replays the
        # call; see _sync.needs_ray.
        require_ray_connection()

        if not block_network:
            _warn_host_network()

        # Modal's resource model: a value is a *request* -- what Ray reserves
        # to schedule the sandbox -- and the container may burst above it.
        # Only the limit half of a (request, limit) tuple is a hard cap. CPU
        # without one gets Modal's documented soft default of 16 cores above
        # the request; memory without one gets no cap at all, as on Modal.
        _validate_resource("cpu", cpu)
        _validate_resource("memory", memory)
        cpu_request = _scalar(cpu)
        memory_request = _scalar(memory)
        cpu_limit = _limit(cpu)
        if cpu_limit is None and cpu_request is not None:
            cpu_limit = cpu_request + _DEFAULT_CPU_HEADROOM
        memory_limit = _limit(memory)

        resolved = resolve_image(image) or resolve_image(DEFAULT_IMAGE)

        create_kwargs: Dict[str, Any] = {
            "image": resolved.reference,
            # Explicit create(env=) wins over the image's own env, matching how
            # Modal layers a later ENV over an earlier one.
            "env": {**resolved.env, **_clean_env(env)},
            "workdir": workdir or resolved.workdir,
            "network": "none" if block_network else "public",
            # Explicit: SandboxConfig defaults readonly to True, whereas a
            # Modal sandbox always has a writable filesystem.
            "readonly": False,
            # Modal sandboxes run as root with Docker's default capability set.
            # runsc's own default is far narrower and breaks ordinary images:
            # apt-get needs CAP_SETUID/CAP_SETGID, and tar-as-root needs
            # CAP_CHOWN.
            "capabilities": DOCKER_DEFAULT_CAPABILITIES,
            "timeout_seconds": _CREATE_TIMEOUT_SECONDS,
        }
        if resolved.shell:
            create_kwargs["shell"] = resolved.shell
        # The container's cgroup caps.
        if cpu_limit is not None:
            create_kwargs["cpu"] = cpu_limit
        if memory_limit is not None:
            # Modal expresses memory in MiB.
            create_kwargs["memory"] = f"{int(memory_limit)}Mi"

        # Ray's reservation: the requests.
        actor_options = build_actor_options(
            cpu_request,
            f"{int(memory_request)}Mi" if memory_request is not None else None,
            # Always None: a truthy gpu= is rejected above, because the backend
            # cannot pass a device into the container.
            None,
            # Custom Ray resources are not part of Modal's surface; callers
            # needing them go through SandboxRuntime directly.
            None,
        )
        actor = _SandboxActor.options(**actor_options).remote(
            create_kwargs, timeout, resolved.force_pull, readiness_probe
        )

        logger.info("Creating sandbox from image %s", resolved.reference)
        started = await actor.start.remote()
        sandbox = _Sandbox(actor, started["instance_id"], None, readiness_probe)

        # Modal's precedence: create(*args) overrides the image's CMD, and the
        # image's ENTRYPOINT prefixes whichever wins -- with the *base image's*
        # own Cmd/Entrypoint beneath both. That config is only readable once
        # the image exists on the node, which happens during start() as part of
        # the pull -- which is why start() hands it back rather than making
        # this a second round trip.
        main_argv = resolved.main_argv(list(args), started["image_config"])

        # From here the container is running, so any failure has to tear it
        # down. Letting the handles fall out of scope is not enough: Ray
        # reclaiming the actor kills its process without running _teardown,
        # stranding the runsc container and its /tmp/ray/sandbox/<id> tree.
        try:
            # Files added with copy=False are pushed from the client, so they
            # never enter the image. They must land before the main process runs.
            await _push_startup_files(sandbox, resolved.startup_files)

            if main_argv:
                main_exec_id = await actor.launch_main.remote(main_argv)
                sandbox._bind_main_process(main_exec_id)
        except BaseException:
            try:
                await sandbox.terminate()
            except Exception:
                logger.warning(
                    "Failed to clean up sandbox %s after a failed start",
                    started["instance_id"],
                    exc_info=True,
                )
            raise
        return sandbox

    # -- properties --------------------------------------------------------

    @property
    def object_id(self) -> str:
        """The Sandbox's unique id."""
        return self._object_id

    @property
    def stdout(self) -> _StreamReader:
        """Reader for the main process's stdout stream."""
        self._ensure_attached()
        return self._require_main_process("stdout", self._stdout)

    @property
    def stderr(self) -> _StreamReader:
        """Reader for the main process's stderr stream."""
        self._ensure_attached()
        return self._require_main_process("stderr", self._stderr)

    @property
    def stdin(self) -> _StreamWriter:
        """Writer for the main process's stdin stream."""
        self._ensure_attached()
        return self._require_main_process("stdin", self._stdin)

    @property
    def returncode(self) -> Optional[int]:
        """The Sandbox's exit code, or None while it is still running.

        Reflects the last ``wait()`` or ``poll()``. A Sandbox that hit its
        timeout reports 124; one that was terminated reports 137.
        """
        return self._returncode

    @property
    def filesystem(self) -> _SandboxFilesystem:
        """Namespace for the Sandbox's filesystem operations."""
        self._ensure_attached()
        if self._filesystem is None:
            self._filesystem = _SandboxFilesystem(self)
        return self._filesystem

    def _require_main_process(self, name: str, value):
        if value is None:
            raise InvalidError(
                f"This Sandbox has no main process, so it has no {name}. "
                f"Pass a command to Sandbox.create() to give it one, or use "
                f"exec() and read the returned process's streams."
            )
        return value

    # -- execution ---------------------------------------------------------

    async def exec(
        self,
        *args: str,
        stdout: StreamType = StreamType.PIPE,
        stderr: StreamType = StreamType.PIPE,
        timeout: Optional[int] = None,
        workdir: Optional[str] = None,
        env: Optional[Dict[str, Optional[str]]] = None,
        text: bool = True,
        bufsize: int = -1,
        pty: bool = False,
        secrets: Optional[Sequence[Any]] = None,
        pty_info: Optional[Any] = None,
        _pty_info: Optional[Any] = None,
    ) -> _ContainerProcess:
        """Run a command in the Sandbox and return a handle on it.

        Args:
            *args: Command and arguments to run.
            stdout: Where the command's stdout goes.
            stderr: Where the command's stderr goes.
            timeout: Seconds to allow the command. On expiry the command is
                killed and its exit code is -1; no exception is raised. As on
                Modal, its output is readable only until then: a read still
                running at the deadline ends there, and a later one returns
                nothing. 0 or None means no timeout.
            workdir: Working directory for the command. Must be absolute.
            env: Extra environment variables for this command only. None
                values are dropped.
            text: Decode the streams as UTF-8 text. When False they yield bytes.
            bufsize: ``-1`` for unbuffered output, ``1`` for line-buffered.
                Line buffering requires ``text=True``.
            pty: Unsupported.
            secrets: Unsupported.
            pty_info: Unsupported. Deprecated in Modal in favour of ``pty``;
                accepted here so ported code reaches the same rejection
                instead of an unexpected-keyword ``TypeError``.
            _pty_info: Unsupported. The older spelling of ``pty_info``.

        Returns:
            A :class:`ContainerProcess` for the running command.

        Raises:
            InvalidError: The arguments are not usable.
            NotImplementedError: ``pty`` or ``secrets`` was requested.
        """
        self._ensure_attached()
        _validate_exec_args(args)
        _reject_unsupported(secrets=secrets)
        if pty or pty_info is not None or _pty_info is not None:
            raise _unsupported(
                "The 'pty' parameter",
                "The gVisor backend does not allocate a terminal for exec'd "
                "commands.",
            )
        if bufsize not in (-1, 1):
            raise InvalidError("bufsize must be -1 (unbuffered) or 1 (line-buffered).")
        if bufsize == 1 and not text:
            raise ValueError("line-buffering is only supported when text=True")
        if workdir is not None and not workdir.startswith("/"):
            raise InvalidError("workdir must be an absolute path.")
        _validate_env(env, "Sandbox.exec", check_names=False)
        if timeout is not None and timeout < 0:
            # Modal's request builder refuses this outright. Accepted, it
            # would leave the handle reporting -1 while the actor -- which
            # enforces only a positive timeout -- let the command run on.
            raise ValueError(f"timeout must not be negative, got {timeout}.")

        with self._sandbox_gone_as_not_found():
            exec_id = await self._actor.exec_start.remote(
                list(args),
                cwd=workdir,
                env=_clean_env(env),
                stdout_devnull=stdout == StreamType.DEVNULL,
                stderr_devnull=stderr == StreamType.DEVNULL,
                # Enforced inside the actor, which kills the command when the
                # deadline passes. A client-side deadline alone would only bound
                # this handle's wait() -- the command would keep running, poll()
                # would never report it, and a reader would block until the
                # sandbox itself ended.
                timeout=timeout,
            )
        # A zero timeout means none, as Modal's client reads it; the actor
        # already ignores it, and a deadline of "now" would kill the command on
        # the first poll().
        deadline = time.monotonic() + timeout if timeout else None
        return _ContainerProcess(
            self._actor,
            exec_id,
            stdout=stdout,
            stderr=stderr,
            text=text,
            by_line=bufsize == 1,
            exec_deadline=deadline,
        )

    # -- lifecycle ---------------------------------------------------------

    async def wait(self, raise_on_termination: bool = True) -> None:
        """Block until the Sandbox finishes.

        Args:
            raise_on_termination: Raise if the Sandbox was terminated rather
                than exiting on its own.

        Raises:
            SandboxTimeoutError: The Sandbox hit its timeout.
            SandboxTerminatedError: The Sandbox was terminated and
                ``raise_on_termination`` is set.
        """
        if self._actor_released:
            # terminate() already reclaimed the actor, so there is nothing left
            # to wait on -- but *why* it ended still matters. A sandbox that ran
            # to completion before being terminated keeps its own outcome, which
            # is what the actor recorded and terminate() cached; reporting it as
            # terminated regardless would contradict the live path below.
            self._raise_for_exit_reason(self._exit_reason, raise_on_termination)
            return
        try:
            with self._sandbox_gone_as_not_found():
                outcome = await self._actor.wait_sandbox.remote(None)
        except NotFoundError:
            if not self._actor_released:
                raise
            # terminate() from another thread got there first and killed the
            # actor under this call; it recorded how the sandbox ended.
            outcome = {"returncode": self._returncode, "exit_reason": self._exit_reason}
        self._returncode = outcome["returncode"]
        self._exit_reason = outcome["exit_reason"]
        self._note_finished()
        self._raise_for_exit_reason(self._exit_reason, raise_on_termination)

    @staticmethod
    def _raise_for_exit_reason(
        exit_reason: Optional[str], raise_on_termination: bool
    ) -> None:
        """Translate a recorded exit reason into Modal's exception, if any."""
        if exit_reason == "timeout":
            raise SandboxTimeoutError("Sandbox exceeded its timeout.")
        if exit_reason == "terminated" and raise_on_termination:
            raise SandboxTerminatedError("Sandbox was terminated.")

    async def poll(self) -> Optional[int]:
        """Check whether the Sandbox has finished.

        Returns:
            None while it is still running, otherwise its exit code.
        """
        self._ensure_attached()
        if self._actor_released:
            return self._returncode
        with self._sandbox_gone_as_not_found():
            state = await self._actor.get_state.remote()
        self._returncode = state["returncode"]
        # Cached so a later wait() can report *why* it ended without another
        # round trip, and so _note_finished can tell a first observation of the
        # end from every call after it.
        self._exit_reason = state["exit_reason"]
        self._note_finished()
        return self._returncode

    def _note_finished(self) -> None:
        """Say once that a finished Sandbox is still holding its reservation.

        The actor outlives the container: a Sandbox that hits its timeout has
        its container torn down, but the actor stays up holding the cpu and
        memory Ray reserved for it, and nothing reclaims that until the handle
        is dropped or terminate() is called.

        Releasing it here instead would be wrong. The actor is also where the
        Sandbox's buffered output lives, and ``wait()`` then reading ``stdout``
        is ordinary Modal usage -- killing the actor at the end of wait() would
        turn that into a dead-actor error.
        """
        if self._finish_logged or self._returncode is None:
            return
        self._finish_logged = True
        logger.info(
            "Sandbox %s has finished (exit code %s). Its output stays readable "
            "until terminate() is called or this handle is dropped, which is "
            "also what gives its cpu/memory reservation back.",
            self._object_id,
            self._returncode,
        )

    def _sandbox_gone_as_not_found(self):
        """Report a vanished actor the way the filesystem namespace does.

        Without this an actor that died outside terminate() -- the node went
        away, or Ray reclaimed it -- reaches the caller as a raw RayActorError,
        while the exact same failure during a filesystem call is already
        translated. One backend, one exception for one condition; the process
        and stream handles share the same helper.
        """
        return _sandbox_gone_as_not_found()

    async def wait_until_ready(self, *, timeout: int = 300) -> None:
        """Block until the readiness probe reports the Sandbox is ready.

        The Sandbox must have been created with a ``readiness_probe``. The probe
        starts running when the Sandbox's main process does, so a Sandbox that
        became ready before this call returns immediately.

        Args:
            timeout: Seconds to wait for readiness. Defaults to 300.

        Raises:
            InvalidError: The Sandbox has no ``readiness_probe``, or ``timeout``
                is not positive.
            ConflictError: The Sandbox was already gone when this was called.
            TimeoutError: The probe did not pass within ``timeout``.
            SandboxTimeoutError: The Sandbox hit its own timeout while probing.
            SandboxTerminatedError: The Sandbox was terminated while probing.
        """
        if timeout <= 0:
            raise InvalidError(f"`timeout` must be positive, got: {timeout}")
        self._ensure_attached()
        if self._readiness_probe is None:
            # ConflictError, as measured on Modal ("Sandbox does not have a
            # readiness probe configured"). It derives from InvalidError, so
            # callers catching that still catch this.
            raise ConflictError(
                "This Sandbox has no readiness probe, so there is nothing to "
                "wait for. Pass readiness_probe=Probe.with_exec(...) to "
                "Sandbox.create() to give it one."
            )
        # Readiness is monotonic: once the probe has passed it stays passed, so
        # a second call is answered here rather than over the wire.
        if self._ready:
            return
        if self._actor_released:
            # ConflictError, not SandboxTerminatedError or SandboxTimeoutError:
            # Modal reports a Sandbox that was already gone when
            # wait_until_ready() was called as a conflict with its current
            # state, whatever ended it. A Sandbox that dies *while* this is
            # parked still raises SandboxTerminatedError below, which is the
            # distinction Modal draws too.
            raise ConflictError(
                "Sandbox was terminated, so it will never become ready."
            )

        try:
            result = await self._actor.wait_ready.remote(timeout)
        except ray.exceptions.RayActorError:
            # terminate() tears the sandbox down and then kills the actor, so a
            # wait parked here races that kill: the actor sets the outcome on
            # its way out, but the reply may not beat ray.kill(). Either way the
            # sandbox is gone and will never be ready, which is what a caller
            # blocked on readiness needs to be told.
            raise SandboxTerminatedError(
                "Sandbox was terminated while waiting for it to become ready."
            )
        outcome = result["outcome"]
        if outcome == "ready":
            self._ready = True
            return
        if outcome == "timeout":
            raise TimeoutError(
                f"Sandbox did not become ready within {timeout} seconds."
            )
        # "ended": the sandbox stopped before the probe passed. The exit reason
        # came back with the outcome, so this reports the same exception wait()
        # would without a second round trip to fetch it.
        self._exit_reason = result["exit_reason"]
        self._raise_for_exit_reason(self._exit_reason, True)
        if self._exit_reason == "completed":
            # Its main process exited, which ends a Sandbox. Modal reports a
            # finished Sandbox as a conflict with its state.
            raise ConflictError(
                "Sandbox already finished before its readiness probe passed."
            )
        raise SandboxTerminatedError(
            "Sandbox stopped before its readiness probe passed."
        )

    async def terminate(self, *, wait: bool = False) -> Optional[int]:
        """Terminate the Sandbox.

        A no-op if it has already finished.

        Args:
            wait: Block until teardown completes and return the exit code.

        Returns:
            The exit code when ``wait`` is set, otherwise None.
        """
        # Idempotent, and it has to stay that way now that terminating also
        # kills the actor: a second call must not RPC a handle whose actor is
        # already gone, which raises ActorDiedError.
        if self._actor_released:
            return self._returncode if wait else None

        # The reason comes back with the code: once the actor is killed the
        # handle can no longer ask, and wait()/poll() still have to distinguish
        # "terminated" from "finished on its own, then torn down".
        outcome = await self._actor.terminate.remote()
        returncode = self._returncode = outcome["returncode"]
        self._exit_reason = outcome["exit_reason"]
        # Tearing down the container is not the whole job: the actor holds the
        # cpu, memory and gpu reserved for this sandbox, and would keep holding
        # them until the caller happened to drop every handle.
        self._release_actor()
        if wait:
            return returncode
        return None

    def _release_actor(self) -> None:
        """Give the actor's Ray resources back. Safe to call more than once.

        The handle is kept rather than cleared: the exit code is already
        cached, and callers may still poll or wait after terminating.
        """
        if self._actor_released:
            return
        self._actor_released = True
        try:
            ray.kill(self._actor)
        except Exception:
            # Already gone, or the cluster is shutting down underneath us.
            logger.debug("Could not kill sandbox actor", exc_info=True)

    # -- unsupported Modal surface ----------------------------------------

    @staticmethod
    async def from_id(sandbox_id: str, client: Optional[Any] = None) -> "_Sandbox":
        """Unsupported. Sandboxes live only as long as the handle that made them."""
        raise _unsupported(
            "Sandbox.from_id()",
            "A Sandbox is reachable only through the handle that created it.",
        )

    # Each of these keeps Modal's full parameter list even though the body only
    # raises. A `**kwargs` stub costs nothing to write but leaves
    # `inspect.signature`, `help()` and editor completion blind, and it accepts
    # a misspelled keyword in silence rather than reporting it.

    @staticmethod
    async def from_name(
        app_name: str,
        name: str,
        *,
        environment_name: Optional[str] = None,
        client: Optional[Any] = None,
    ) -> "_Sandbox":
        """Unsupported. Sandboxes are not registered by name."""
        raise _unsupported("Sandbox.from_name()")

    @staticmethod
    def list(
        *,
        app_id: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        client: Optional[Any] = None,
    ) -> Iterator["_Sandbox"]:
        """Unsupported. There is no registry of running Sandboxes."""
        # Not a coroutine, because Modal's is not: it hands back an iterator and
        # does no work until that is consumed. Raising from a plain function
        # fails at the same point in a ported program.
        raise _unsupported("Sandbox.list()")

    async def get_tags(self) -> Dict[str, str]:
        """Unsupported. Sandboxes carry no server-side tags."""
        raise _unsupported("Sandbox.get_tags()")

    async def set_tags(
        self, tags: Dict[str, str], *, client: Optional[Any] = None
    ) -> None:
        """Unsupported. Sandboxes carry no server-side tags."""
        raise _unsupported("Sandbox.set_tags()")

    async def tunnels(self, timeout: int = 50) -> Dict[int, Any]:
        """Unsupported. The gVisor backend publishes no ports."""
        raise _unsupported("Sandbox.tunnels()")

    async def create_connect_token(
        self,
        user_metadata: Union[str, Dict[str, Any], None] = None,
        port: int = 8080,
    ) -> Any:
        """Unsupported. The gVisor backend publishes no ports."""
        raise _unsupported("Sandbox.create_connect_token()")

    async def snapshot_filesystem(
        self, timeout: int = 55, *, ttl: Optional[int] = 2592000
    ) -> Any:
        """Unsupported. The backend cannot build an image from a Sandbox."""
        raise _unsupported("Sandbox.snapshot_filesystem()")

    async def snapshot_directory(
        self,
        path: Union[PurePosixPath, str],
        *,
        timeout: int = 55,
        ttl: Optional[int] = 2592000,
        _experimental_encryption_key: Optional[bytes] = None,
    ) -> Any:
        """Unsupported. The backend cannot build an image from a Sandbox."""
        raise _unsupported("Sandbox.snapshot_directory()")

    async def mount_image(
        self,
        path: Union[PurePosixPath, str],
        image: Any,
        *,
        _experimental_encryption_key: Optional[bytes] = None,
    ) -> None:
        """Unsupported. Images cannot be mounted into a running Sandbox."""
        raise _unsupported("Sandbox.mount_image()")

    async def unmount_image(self, path: Union[PurePosixPath, str]) -> None:
        """Unsupported. Images cannot be mounted into a running Sandbox."""
        raise _unsupported("Sandbox.unmount_image()")

    async def reload_volumes(self, *, timeout: int = 55) -> None:
        """Unsupported. Sandboxes mount no volumes."""
        raise _unsupported("Sandbox.reload_volumes()")

    async def detach(self):
        """Disconnect this handle from the Sandbox, which keeps running.

        As on Modal, later operations through the handle raise
        :class:`ClientClosed`, while ``wait()`` and ``returncode`` keep
        working. Unlike Modal, ``terminate()`` keeps working too: with no
        ``Sandbox.from_id()`` here, it is the only way left to end the Sandbox
        early -- short of dropping the handle, which ends it as well.
        """
        self._detached = True

    def _ensure_attached(self) -> None:
        if self._detached:
            raise ClientClosed("Unable to perform operation on a detached sandbox")

    # -- Modal's legacy filesystem aliases ---------------------------------
    #
    # Removed from Modal's client in favour of Sandbox.filesystem (deprecated
    # first, then dropped in September 2026). Kept so code written against an
    # older client still runs, or gets a directed error, rather than failing
    # with AttributeError.

    async def open(self, path: str, mode: str = "r"):
        """Unsupported. Use the ``filesystem`` namespace.

        Defined rather than left off so that ported code gets a directed error
        instead of ``AttributeError``. Modal has removed this in favour of
        ``Sandbox.filesystem``, which is implemented here in full, so there is
        no reason to grow a ``FileIO`` handle to match it.

        Args:
            path: Unsupported. The file that would be opened.
            mode: Unsupported. The mode it would be opened in.

        Raises:
            NotImplementedError: Always.
        """
        raise _unsupported(
            "Sandbox.open()",
            "Modal replaced it with Sandbox.filesystem. Use "
            "filesystem.read_bytes()/read_text() or "
            "filesystem.write_bytes()/write_text() instead.",
        )

    async def ls(self, path: str) -> List[str]:
        """Legacy alias, removed from Modal. Use ``filesystem.list_files()``."""
        entries = await self.filesystem.list_files(path)
        return [entry.name for entry in entries]

    async def mkdir(self, path: str, parents: bool = False) -> None:
        """Legacy alias, removed from Modal. Use ``filesystem.make_directory()``."""
        await self.filesystem.make_directory(path, create_parents=parents)

    async def rm(self, path: str, recursive: bool = False) -> None:
        """Legacy alias, removed from Modal. Use ``filesystem.remove()``."""
        await self.filesystem.remove(path, recursive=recursive)

    def watch(
        self,
        path: str,
        filter: Optional[List[FileWatchEventType]] = None,
        recursive: Optional[bool] = None,
        timeout: Optional[int] = None,
    ) -> Iterator[FileWatchEvent]:
        """Unsupported. Use ``filesystem.watch()`` for the same message.

        Modal spells the parameters differently on the two surfaces -- ``path``
        and ``recursive=None`` here, ``remote_path`` and ``recursive=False`` on
        the filesystem namespace -- so the two are declared separately rather
        than forwarded blindly. Passing ``path=`` used to reach a ``TypeError``
        instead of the intended refusal.
        """
        raise _unsupported("Sandbox.watch()")


Sandbox = synchronize_api(_Sandbox)
