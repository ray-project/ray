"""A Modal-compatible API for Ray Sandboxes.

This package presents `Modal's Sandbox API <https://modal.com/docs/guide/sandbox>`_
on top of :mod:`ray.experimental.sandbox`, so that code written against Modal
runs on a Ray cluster with a gVisor-isolated container underneath::

    from ray.experimental.sandbox import modal

    sandbox = modal.Sandbox.create(image="python:3.13-slim")
    process = sandbox.exec("bash", "-c", "echo hello")
    print(process.stdout.read())
    sandbox.terminate()

Every method blocks by default and also carries an ``.aio`` variant for use
inside an event loop::

    sandbox = await modal.Sandbox.create.aio(image="python:3.13-slim")
    process = await sandbox.exec.aio("bash", "-c", "echo hello")
    async for line in process.stdout:
        print(line, end="")

Modal features that depend on its hosted control plane -- tunnels, volumes,
secrets, snapshots, tags, and looking Sandboxes up by id or name -- raise
``NotImplementedError``. A Sandbox here lives only as long as the handle that
created it.

Divergences from Modal
----------------------

The API surface is Modal's: no Ray-only parameters are accepted, so anything
that runs here runs there. Behaviour differs in these places.

Image building is not implemented.
    An ``Image`` carries a base reference plus runtime metadata and nothing
    that mutates a filesystem. ``Image.debian_slim()``, ``run_commands()``,
    ``pip_install()``, ``apt_install()``, ``pip_install_from_requirements()``,
    ``pip_install_from_pyproject()`` and ``add_local_*(copy=True)`` all raise
    ``NotSupportedError``. Start from a published image with
    ``Image.from_registry("python:3.13-slim")`` -- any public registry works,
    not just Docker Hub -- and do setup work with ``Sandbox.exec()`` once the
    sandbox is running. ``env()``, ``workdir()``, ``shell()``, ``cmd()``,
    ``entrypoint()`` and ``add_local_*(copy=False)`` work in full, because
    those are applied when the sandbox is created rather than baked into a
    layer. Building is planned; every stub above is a body away from working.

Images are pulled and cached per node.
    Each node keeps images in ``/tmp/ray/sandbox/images`` as EROFS root
    filesystems, shared with ``ray.experimental.sandbox.Sandbox``; building
    them needs ``mkfs.erofs`` 1.7 or later on the node. As on Modal, a tag is
    pinned once pulled: later sandboxes on that node reuse the cached image
    until ``force_build=True``, which pulls again rather than rebuilding.
    Unlike Modal, the pin is per node, so two nodes can hold different
    versions of a tag that moved between their pulls; pass a digest
    (``repo@sha256:...``) to pin one everywhere. A sandbox keeps the image it
    started on whatever happens to the cache. The cache is bounded by least
    recently used eviction (``RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES``), which
    never removes an image a sandbox is running on.

``Image.env()`` accepts ``None`` to mean "unset".
    Modal rejects any non-string value here; the None-means-unset convention
    is Modal's for the build-step ``env=`` parameters. Accepting it is a
    widening, so no Modal program behaves differently.

Reading consumes, as on Modal; unread output is held in memory, up to a bound.
    As in Modal's client from 1.6, a ``StreamReader`` keeps one position:
    ``read()`` and every loop over it draw from it, so each byte is delivered
    once, and a second ``read()`` after the end returns empty. Stopping
    part-way -- a ``break``, ``aclose()`` -- keeps the position, and the next
    read or loop resumes exactly there. No stream ever makes the process wait
    for a reader.

    The difference is where unread output lives. Modal's servers keep an
    ``exec``'d command's whole output until it is read (4 GiB came back
    intact, measured); here it is in the sandbox actor's memory, freed as it
    is read, and bounded while it is not: the newest 64 MiB of each exec
    stream, the newest 256 MiB of the Sandbox's own stdout and stderr (Modal
    keeps the same 256 MiB of those), and 512 MiB across the whole sandbox,
    where finished commands' oldest unread output goes first. Output read as
    it is produced is never dropped. What is dropped is reported rather than
    spliced: the reader sets ``truncated`` and ``bytes_lost`` and logs once,
    and never raises, so a program that runs clean on Modal still runs here.

    A finished command's output stays readable until it is read, as on Modal
    -- a batch of commands can all finish before any of them is read. Once
    both its streams have been read to the end its session is released; past
    1024 finished commands the oldest go regardless. Its *exit code* outlives
    it either way, so ``poll()`` and ``wait()`` on a ``ContainerProcess`` keep
    answering, which is what Modal does. A read of a dropped command's stream
    reports the rest as lost, through the same ``truncated`` / ``bytes_lost``
    route.

``Probe.with_tcp()`` is unimplemented.
    The backend publishes no ports and has no tunnels, so a TCP check would
    have nothing to observe. ``Probe.with_exec()`` works in full, and covers
    the same ground from inside the Sandbox. This will be revisited when the
    sandbox grows networking APIs.

Readiness probing starts with the main process, not with the container.
    Modal begins probing when the Sandbox is created and reports the resulting
    startup latency. Here the probe starts when the main process does, which is
    after any ``add_local_*(copy=False)`` files have been pushed -- so a probe
    cannot pass on a condition that the base image happened to satisfy before
    setup ran. Each attempt is a real ``runsc exec`` costing tens of
    milliseconds, so the default ``interval_ms=100`` is meaningfully more
    expensive here than on Modal.

``Sandbox.filesystem.watch()`` and ``Sandbox.watch()`` are unimplemented.
    Both raise ``NotSupportedError``, with Modal's full signatures.

Network access is all-or-nothing, and the default is not Modal's isolation.
    ``block_network=True`` cuts the network off entirely; otherwise the Sandbox
    gets a network namespace of its own (``network="public"``, bridged by
    ``slirp4netns``, which the node needs) with private ports and loopback, but
    its egress leaves through the node with no destination filter. So where
    Modal's same default gives an isolated network, code here still reaches
    the private ranges the node can reach -- other Ray nodes, the head node's
    GCS among them -- and the cloud instance-metadata endpoint. The default is
    kept at Modal's so that ported programs keep their egress, and creating a
    Sandbox without ``block_network=True`` logs a warning once per process
    saying so.
    Modal's CIDR and domain allowlists, and its experimental outbound policy,
    are rejected rather than approximated, because silently failing to enforce
    an egress restriction is worse than refusing it. An empty allowlist is
    refused too: on Modal it allows nothing, so it is not the same as none.

``Image.shell()`` is accepted but currently has no effect.
    It is validated and carried into the sandbox, but the backend consults its
    shell only for a command given as a string, and every command this layer
    sends is an argv list. Nothing breaks; nothing changes either.

The default image is a plain Python base, not ``debian_slim()``.
    Modal defaults to ``Image.debian_slim()``, which is a slim Python base plus
    ``gcc gfortran build-essential`` and an upgraded ``pip wheel uv``. Building
    that is not possible here, so the default is ``python:3.13-slim`` alone. A
    program that omits ``image=`` and then compiles a package from source works
    on Modal and fails here; install a toolchain with ``Sandbox.exec()``, or
    start from an image that carries one.

An App is accepted and ignored.
    Modal requires ``app=`` on ``Sandbox.create()``, so every Modal Sandbox
    program passes one; here an App from ``App.lookup()`` is accepted and
    ignored, as ``name=`` is, because the handle rather than an App owns the
    sandbox. An App that was only constructed is refused with Modal's own
    ``ValueError``, since Modal refuses an App that was never initialized.
    Stopping an App therefore stops none of its Sandboxes, and ``App.run()``
    and ``App.deploy()`` are absent.

Some Modal members are absent rather than stubbed.
    ``Sandbox.logs``, ``ContainerProcess.attach()``, and the ``.aio`` surface on
    ``Image`` (which is a plain class here, not put through the sync/async
    wrapper) have no counterpart. ``modal.file_io`` / ``FileIO`` does not exist;
    ``Sandbox.open()`` raises rather than pretending otherwise. Modal's
    experimental sidecar API -- ``Sandbox._experimental_sidecars``,
    ``SidecarContainer``, ``SidecarManager`` -- is absent too, as is the
    ``Object`` base it hydrates its handles through. The reverse holds for
    ``Sandbox.ls()``, ``mkdir()`` and ``rm()``: Modal's current client has
    removed them, and here they still work, delegating to ``filesystem``, so
    code written against an older client keeps running.

``App`` carries Modal's constructor but owns nothing.
    ``App(name, tags=..., image=..., secrets=..., volumes=...)`` accepts every
    parameter Modal's does and refuses the four that need a server-side object.
    ``include_source`` is accepted and ignored, since nothing is built or
    uploaded here. ``App.lookup()`` and its ``.aio`` return an App that
    ``Sandbox.create(app=...)`` accepts -- see the entry on apps above.

``.aio`` calls run on the caller's own event loop.
    Modal runs every call, blocking or ``.aio``, on one background loop of its
    own and relays ``.aio`` results back. Here only blocking calls use a
    background loop; an ``.aio`` call is awaited directly in the caller's
    loop. An object's loop-bound state -- a stream read in flight, the task
    that prints a ``StreamType.STDOUT`` process's output -- therefore belongs
    to the loop that first drove it: do not read one object's streams through
    both surfaces, and do not carry an object used through ``.aio`` into a
    second ``asyncio.run()``. Otherwise the wrapper behaves as Modal's does:
    a blocking call inside a running event loop warns with
    ``modal.exception.AsyncUsageWarning`` (``MODAL_ASYNC_WARNINGS=0``
    silences it), and Ctrl-C during a blocking call cancels it.

Generic public classes are subscriptable but not parameterized.
    Modal types ``ContainerProcess[str]`` and ``StreamReader[bytes]``, keyed on
    ``text=``. Subscripting works here so ported annotations and ``typing.cast``
    calls evaluate, but the element type is discarded rather than checked, and
    ``exec()`` is not overloaded on ``text``.

Exceptions match Modal's hierarchy, with one addition.
    ``TimeoutError`` here also derives from the builtin ``TimeoutError``, which
    Modal's does not, so ``except TimeoutError:`` catches here and not there.
    Kept deliberately: Ray's own ``SandboxTimeoutError`` already derives from
    the builtin, and diverging from Ray inside Ray would be worse.
    ``NotSupportedError`` is an addition with no Modal counterpart; it derives
    from both ``NotImplementedError`` and ``Error``, so either ``except``
    catches it.

    Several names exist only so ported ``except`` clauses resolve, and nothing
    raises them: ``ExecTimeoutError`` (a timed-out ``exec`` reports exit code
    ``-1``, as Modal's ``ContainerProcess`` does), ``AlreadyExistsError``,
    ``RemoteError``, ``ImageBuildError``, ``InteractiveTimeoutError``,
    ``SnapshotCreationError``, ``ExecutionError``, ``InternalError``,
    ``PermissionDeniedError``, ``ServiceError``, ``ConnectionError``,
    ``FilesystemExecutionError``, ``AuthError``, ``DataLossError``,
    ``ResourceExhaustedError``, ``UnimplementedError``, ``RequestSizeError``
    and ``VersionError``. (``ClientClosed`` is raised, as on Modal, by a
    ``detach()``-ed handle.) ``FilesystemExecutionError`` has since been
    removed from Modal's client along with its legacy filesystem API; it stays
    here so older ``except`` clauses still resolve. Modal's
    Function-, Volume-, Mount- and Cls-only errors are not mirrored, nor is the
    ``grpclib`` mixin its control-plane errors carry, so ``err.status`` and
    ``except grpclib.GRPCError`` have no counterpart here.

A finished Sandbox keeps its reservation until ``terminate()``.
    When the main process exits, the Sandbox ends as on Modal: its container
    is torn down, and ``exec()`` and filesystem calls raise ``NotFoundError``.
    Its output and exit codes stay readable, because they live in the actor
    that ran it -- and that actor keeps the cpu and memory Ray reserved for the
    Sandbox until ``terminate()`` is called or the handle is dropped. Modal
    stops charging for a Sandbox the moment it ends.

``terminate()`` discards the Sandbox's own output.
    Measured on Modal, a Sandbox's stdout stays readable after
    ``terminate()``. Here ``terminate()`` gives the reservation back by
    killing the actor that holds the output, so a read afterwards raises
    ``NotFoundError``. Read what you need first, or ``wait()`` for the Sandbox
    to finish, which keeps the output.

``detach()`` leaves ``terminate()`` working.
    As on Modal, a detached handle raises ``ClientClosed`` for further
    operations while ``wait()`` and ``returncode`` keep working. Modal's
    ``terminate()`` refuses too, and a caller reattaches with
    ``Sandbox.from_id()``; with no ``from_id()`` here, ``terminate()`` stays
    available so a detached Sandbox can still be ended early.

A Sandbox with no main process has no streams.
    Modal's default image carries a ``CMD``, so its Sandboxes always have
    readable streams. Created here with no arguments and an image that declares
    neither ``Cmd`` nor ``Entrypoint``, there is no main process, and
    ``stdout`` / ``stderr`` / ``stdin`` raise ``InvalidError`` rather than
    returning an empty stream.

Smaller argument-level differences.
    ``exec(bufsize=...)`` accepts only ``-1`` and ``1``, the two values Modal
    types; Modal does not check at runtime and accepts others. An empty argv is
    rejected here and accepted there. ``add_local_python_source()`` also skips
    dot-prefixed components and ``__pycache__``, which Modal ships -- fewer
    files travel here than there. ``terminate(wait=True)`` returns the exit code
    where Modal re-raises ``SandboxTimeoutError`` for a Sandbox that had already
    timed out. ``cpu`` and ``memory`` follow Modal's model -- the request is
    reserved from Ray, and only a ``(request, limit)`` tuple's limit caps the
    container -- except that Modal's default soft CPU limit, 16 cores above
    the request, is a hard CFS quota here. ``runtime="gvisor"`` is accepted,
    since every Sandbox here runs under gVisor; ``runtime="vm"`` raises
    ``NotSupportedError``.
"""

# Exported as a submodule, not as loose names: upstream spells these
# ``modal.types.FileInfo`` and lists only ``types`` in its own ``__all__``.
from ray.experimental.sandbox.modal import types  # noqa: F401
from ray.experimental.sandbox.modal.app import App
from ray.experimental.sandbox.modal.container_process import ContainerProcess
from ray.experimental.sandbox.modal.exception import (
    AlreadyExistsError,  # noqa: F401
    ClientClosed,  # noqa: F401
    ConflictError,
    ConnectionError,  # noqa: F401
    Error,
    ExecTimeoutError,  # noqa: F401
    ExecutionError,  # noqa: F401
    FilesystemExecutionError,  # noqa: F401
    ImageBuildError,  # noqa: F401
    InteractiveTimeoutError,  # noqa: F401
    InternalError,  # noqa: F401
    InvalidError,
    NotFoundError,
    NotSupportedError,
    PermissionDeniedError,  # noqa: F401
    RemoteError,  # noqa: F401
    SandboxFilesystemDirectoryNotEmptyError,
    SandboxFilesystemError,
    SandboxFilesystemFileTooLargeError,
    SandboxFilesystemIsADirectoryError,
    SandboxFilesystemNotADirectoryError,
    SandboxFilesystemNotFoundError,
    SandboxFilesystemPathAlreadyExistsError,
    SandboxFilesystemPermissionError,
    SandboxTerminatedError,
    SandboxTimeoutError,
    ServiceError,  # noqa: F401
    SnapshotCreationError,  # noqa: F401
    TimeoutError,
)
from ray.experimental.sandbox.modal.image import Image
from ray.experimental.sandbox.modal.io_streams import StreamReader, StreamWriter
from ray.experimental.sandbox.modal.probe import Probe
from ray.experimental.sandbox.modal.sandbox import (
    DEFAULT_IMAGE,
    DEFAULT_TIMEOUT,
    Sandbox,
)
from ray.experimental.sandbox.modal.sandbox_fs import SandboxFilesystem
from ray.experimental.sandbox.modal.stream_type import StreamType

__all__ = [
    "AlreadyExistsError",
    "App",
    "ClientClosed",
    "ConflictError",
    "ConnectionError",
    "ContainerProcess",
    "DEFAULT_IMAGE",
    "DEFAULT_TIMEOUT",
    "Error",
    "ExecTimeoutError",
    "ExecutionError",
    "FilesystemExecutionError",
    "Image",
    "ImageBuildError",
    "InteractiveTimeoutError",
    "InternalError",
    "InvalidError",
    "NotFoundError",
    "NotSupportedError",
    "PermissionDeniedError",
    "RemoteError",
    "ServiceError",
    "SnapshotCreationError",
    "Probe",
    "Sandbox",
    "SandboxFilesystem",
    "SandboxFilesystemDirectoryNotEmptyError",
    "SandboxFilesystemError",
    "SandboxFilesystemFileTooLargeError",
    "SandboxFilesystemIsADirectoryError",
    "SandboxFilesystemNotADirectoryError",
    "SandboxFilesystemNotFoundError",
    "SandboxFilesystemPathAlreadyExistsError",
    "SandboxFilesystemPermissionError",
    "SandboxTerminatedError",
    "SandboxTimeoutError",
    "StreamReader",
    "StreamType",
    "StreamWriter",
    "TimeoutError",
    "types",
]
