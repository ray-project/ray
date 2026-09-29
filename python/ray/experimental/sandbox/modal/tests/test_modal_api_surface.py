"""Unit tests for the Modal-compatible API surface.

Covers argument translation, validation, and the boundary where Modal features
with no Ray equivalent are refused. A ``_Sandbox`` is constructed directly with
no actor behind it, so nothing here needs runsc or a Ray cluster.
"""

import inspect
import sys

import pytest

import ray
from ray.experimental.sandbox import modal
from ray.experimental.sandbox.modal import (
    App,
    Image,
    Sandbox,
    exception as modal_exception,
    sandbox as sandbox_mod,
)
from ray.experimental.sandbox.modal.exception import (
    InvalidError,
    SandboxTerminatedError,
)
from ray.experimental.sandbox.modal.sandbox import _Sandbox


def detached_sandbox(main_exec_id=None, readiness_probe=None):
    """A Sandbox handle with no actor, for surface-level checks."""
    return Sandbox._from_impl(
        _Sandbox(None, "ray-sandbox-test", main_exec_id, readiness_probe)
    )


# -- helpers ---------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [(None, None), (2, 2), (1.5, 1.5), ((2, 4), 2), ([3, 6], 3), ((), None)],
)
def test_resource_tuples_use_the_request_half(value, expected):
    assert sandbox_mod._scalar(value) == expected


@pytest.mark.parametrize(
    "gpu,expected",
    [(None, None), ("A100", 1.0), ("A100:2", 2.0), ("T4:4", 4.0), (2, 2.0)],
)
def test_gpu_specs_reduce_to_a_count(gpu, expected):
    assert sandbox_mod._gpu_count(gpu) == expected


def test_unparseable_gpu_spec_is_rejected():
    with pytest.raises(InvalidError, match="GPU count"):
        sandbox_mod._gpu_count("A100:many")


@pytest.mark.parametrize(
    "env,expected",
    [
        (None, {}),
        ({}, {}),
        ({"A": "1"}, {"A": "1"}),
        # Modal uses None to mean "leave unset".
        ({"A": "1", "B": None}, {"A": "1"}),
    ],
)
def test_none_valued_env_entries_are_dropped(env, expected):
    assert sandbox_mod._clean_env(env) == expected


@pytest.mark.parametrize(
    "args",
    [(), (b"bytes",), ("ok", 5), (None,), ("x" * (sandbox_mod.ARG_MAX_BYTES + 1),)],
)
def test_invalid_exec_arguments_are_rejected(args):
    with pytest.raises(InvalidError):
        sandbox_mod._validate_exec_args(args)


def test_valid_exec_arguments_are_accepted():
    sandbox_mod._validate_exec_args(("bash", "-c", "echo hi"))


# -- Image and App ---------------------------------------------------------


def test_image_from_registry_carries_the_reference():
    assert Image.from_registry("python:3.13-slim").reference == "python:3.13-slim"


def test_debian_slim_is_refused_until_building_lands():
    # It is a base plus a toolchain layer, and there is no builder to add the
    # layer. Handing back the bare base would diverge from Modal invisibly.
    with pytest.raises(NotImplementedError, match="from_registry"):
        Image.debian_slim("3.12")


def test_images_compare_by_reference():
    assert Image("busybox:latest") == Image.from_registry("busybox:latest")
    assert Image("busybox:latest") != Image("alpine:latest")


@pytest.mark.parametrize("reference", ["", "   ", None, 5])
def test_image_requires_a_real_reference(reference):
    with pytest.raises(InvalidError):
        Image(reference)


@pytest.mark.parametrize(
    "image,expected",
    [("busybox", "busybox"), (Image("alpine"), "alpine")],
)
def test_image_arguments_resolve_to_a_reference(image, expected):
    assert sandbox_mod.resolve_image(image).reference == expected


def test_resolving_no_image_yields_nothing():
    assert sandbox_mod.resolve_image(None) is None


def test_a_bad_image_argument_type_is_rejected():
    with pytest.raises(TypeError, match="str or Image"):
        sandbox_mod.resolve_image(42)


def test_private_registry_credentials_are_refused():
    with pytest.raises(NotImplementedError, match="private registry"):
        Image.from_registry("busybox", secret=object())


def test_create_refuses_an_app_that_was_never_looked_up():
    """Modal's App has no app_id until looked up or run, and Sandbox.create
    refuses it with this ValueError -- before anything is created."""
    with pytest.raises(ValueError, match="not been initialized"):
        Sandbox.create(image="busybox:latest", app=App("demo"))


def test_create_refuses_an_app_that_is_not_an_app():
    with pytest.raises(InvalidError, match="modal.App"):
        Sandbox.create(image="busybox:latest", app="my-app")


def test_app_lookup_returns_a_named_app():
    app = App.lookup("my-app", create_if_missing=True)
    assert app.name == "my-app"
    assert app.app_id == "my-app"


def test_a_constructed_app_has_no_id_until_looked_up():
    assert App("demo").app_id is None


# -- refused surface -------------------------------------------------------


def test_sandbox_has_no_public_constructor():
    with pytest.raises(InvalidError, match="no public constructor"):
        Sandbox()


@pytest.mark.parametrize(
    "call",
    [
        lambda sb: sb.get_tags(),
        lambda sb: sb.set_tags({"a": "b"}),
        lambda sb: sb.tunnels(),
        lambda sb: sb.create_connect_token(),
        lambda sb: sb.snapshot_filesystem(),
        lambda sb: sb.snapshot_directory("/tmp"),
        lambda sb: sb.mount_image("/mnt", object()),
        lambda sb: sb.unmount_image("/mnt"),
        lambda sb: sb.reload_volumes(),
        lambda sb: sb.watch("/tmp"),
        lambda sb: sb.filesystem.watch("/tmp"),
    ],
)
def test_cloud_only_methods_are_refused(call):
    with pytest.raises(NotImplementedError):
        call(detached_sandbox())


@pytest.mark.parametrize(
    "call",
    [
        lambda: Sandbox.from_id("sb-123"),
        lambda: Sandbox.from_name("app", "name"),
        lambda: Sandbox.list(),
    ],
)
def test_lookup_methods_are_refused(call):
    with pytest.raises(NotImplementedError):
        call()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"secrets": ["s"]},
        {"volumes": {"/v": object()}},
        {"network_file_systems": {"/n": object()}},
        {"idle_timeout": 30},
        {"cloud": "aws"},
        {"region": "us-east-1"},
        {"encrypted_ports": [8080]},
        {"h2_ports": [8080]},
        {"unencrypted_ports": [8080]},
        {"custom_domain": "example.com"},
        {"proxy": object()},
        {"tags": {"k": "v"}},
        {"pty": True},
        {"include_oidc_identity_token": True},
        {"experimental_options": {"x": 1}},
        {"_experimental_enable_snapshot": True},
        {"client": object()},
        {"environment_name": "prod"},
        {"pty_info": object()},
    ],
)
def test_cloud_only_create_parameters_are_refused(kwargs):
    """Refused before any actor is created, so no cluster is needed."""
    with pytest.raises(NotImplementedError):
        Sandbox.create(image="busybox:latest", **kwargs)


@pytest.mark.parametrize("value", [object(), "ready", ("sh", "-c", "true"), 8080])
def test_create_refuses_a_readiness_probe_that_is_not_a_probe(value):
    """readiness_probe is supported now, so a bad value is invalid rather than
    unsupported -- and must still be caught before an actor is created."""
    with pytest.raises(InvalidError):
        Sandbox.create(image="busybox:latest", readiness_probe=value)


@pytest.mark.parametrize(
    "name",
    [
        "outbound_cidr_allowlist",
        "outbound_domain_allowlist",
        "inbound_cidr_allowlist",
        "cidr_allowlist",
    ],
)
def test_network_allowlists_are_refused_and_say_why(name):
    """The one rejection with a security consequence.

    block_network=False egresses through the node with no destination
    filter, so quietly dropping an allowlist would leave the sandbox reaching
    private ranges and cloud instance metadata while the caller believes
    egress is filtered.
    """
    with pytest.raises(NotImplementedError, match="block_network=True"):
        Sandbox.create(image="busybox:latest", **{name: ["10.0.0.0/8"]})


@pytest.mark.parametrize(
    "kwargs",
    [
        {"readonly": True},
        {"resources": {"custom": 1}},
        {"rootless": False},
        {"ttl_seconds": 30},
        {"dns": ["8.8.8.8"]},
        {"capabilities": []},
        {"_ignore_cgroups": True},
    ],
)
def test_ray_only_create_parameters_are_gone(kwargs):
    """The surface is strictly Modal's: no **kwargs passthrough to SandboxConfig.

    These used to be accepted (or swallowed), which let callers write code here
    that could not run against Modal.
    """
    with pytest.raises(TypeError, match="unexpected keyword"):
        Sandbox.create(image="busybox:latest", **kwargs)


def test_create_accepts_no_parameter_modal_lacks():
    """Guards against a Ray-only parameter creeping back onto create()."""
    modal_parameters = {
        "args",
        "app",
        "name",
        "tags",
        "image",
        "env",
        "secrets",
        "network_file_systems",
        "timeout",
        "idle_timeout",
        "workdir",
        "gpu",
        "cloud",
        "region",
        "cpu",
        "memory",
        "block_network",
        "outbound_cidr_allowlist",
        "outbound_domain_allowlist",
        "inbound_cidr_allowlist",
        "volumes",
        "pty",
        "encrypted_ports",
        "h2_ports",
        "unencrypted_ports",
        "custom_domain",
        "proxy",
        "include_oidc_identity_token",
        "readiness_probe",
        "verbose",
        "experimental_options",
        "_experimental_enable_snapshot",
        "client",
        "environment_name",
        "pty_info",
        "cidr_allowlist",
    }
    ours = set(inspect.signature(sandbox_mod._Sandbox.create).parameters)
    assert ours - modal_parameters == set()


@pytest.mark.parametrize(
    "kwargs",
    [{"pty": True}, {"pty_info": object()}, {"_pty_info": object()}],
)
def test_exec_refuses_every_pty_spelling(kwargs):
    """Modal still accepts the two deprecated spellings, so they must reach the
    same rejection rather than an unexpected-keyword TypeError."""
    with pytest.raises(NotImplementedError, match="pty"):
        detached_sandbox().exec("echo", "hi", **kwargs)


def test_sandbox_open_points_at_the_filesystem_namespace():
    """Defined only so ported code gets a directed error, not AttributeError.

    Modal deprecated Sandbox.open() in favour of Sandbox.filesystem, which is
    implemented here in full.
    """
    with pytest.raises(NotImplementedError, match="filesystem"):
        detached_sandbox().open("/tmp/x")


def test_unsupported_filesystem_members_join_the_modal_hierarchy():
    """NotSupportedError, not the builtin: `except modal.Error` must catch it."""
    with pytest.raises(modal_exception.Error):
        detached_sandbox().filesystem.watch("/tmp")


@pytest.mark.parametrize("workdir", ["relative", "./x", "workspace"])
def test_create_requires_an_absolute_workdir(workdir):
    with pytest.raises(InvalidError, match="absolute"):
        Sandbox.create(image="busybox:latest", workdir=workdir)


def test_a_sandbox_without_a_main_process_has_no_streams():
    sb = detached_sandbox()
    for name in ("stdout", "stderr", "stdin"):
        with pytest.raises(InvalidError, match="no main process"):
            getattr(sb, name)


def test_returncode_is_none_before_any_wait():
    """Unlike ContainerProcess.returncode, this reports None rather than raising."""
    assert detached_sandbox().returncode is None


def test_object_id_is_exposed():
    assert detached_sandbox().object_id == "ray-sandbox-test"


# -- exception hierarchy ---------------------------------------------------
#
# These names exist so an `except` clause ported from Modal resolves instead of
# raising AttributeError. The bases are checked against modal/exception.py.


@pytest.mark.parametrize(
    "name,bases",
    [
        ("RemoteError", ("Error",)),
        ("ImageBuildError", ("RemoteError",)),
        ("ExecTimeoutError", ("TimeoutError",)),
        ("ConflictError", ("InvalidError",)),
        ("AlreadyExistsError", ("Error",)),
        ("SandboxTimeoutError", ("TimeoutError",)),
        ("InteractiveTimeoutError", ("TimeoutError",)),
        ("SnapshotCreationError", ("Error",)),
        ("ExecutionError", ("Error",)),
        ("InternalError", ("Error",)),
        ("PermissionDeniedError", ("Error",)),
        ("ServiceError", ("Error",)),
        ("ConnectionError", ("Error",)),
        ("ClientClosed", ("Error",)),
        ("FilesystemExecutionError", ("Error",)),
    ],
)
def test_exception_bases_match_modal(name, bases):
    cls = getattr(modal, name)
    for base in bases:
        assert issubclass(cls, getattr(modal, base)), f"{name} should subclass {base}"


@pytest.mark.parametrize(
    "name",
    [
        "AlreadyExistsError",
        "ClientClosed",
        "ConflictError",
        "ConnectionError",
        "ExecTimeoutError",
        "ExecutionError",
        "FilesystemExecutionError",
        "ImageBuildError",
        "InteractiveTimeoutError",
        "InternalError",
        "PermissionDeniedError",
        "RemoteError",
        "ServiceError",
        "SnapshotCreationError",
    ],
)
def test_added_exceptions_are_exported(name):
    assert name in modal.__all__
    assert issubclass(getattr(modal, name), modal.Error)


def test_image_build_error_carries_the_image_id():
    """Modal's takes (message, image_id) so a caller can fetch build logs."""
    error = modal.ImageBuildError("build step failed", "im-123")
    assert error.image_id == "im-123"
    assert str(error) == "build step failed"


# -- signature parity ------------------------------------------------------
#
# Modal's signatures, taken from the .pyi stubs it ships. Checked even for
# members that only raise: a `**kwargs` stub leaves inspect.signature, help()
# and editor completion blind, and swallows a misspelled keyword in silence.

MODAL_SIGNATURES = {
    "from_id": "(sandbox_id, client=None)",
    "from_name": "(app_name, name, *, environment_name=None, client=None)",
    "list": "(*, app_id=None, tags=None, client=None)",
    "get_tags": "(self)",
    "set_tags": "(self, tags, *, client=None)",
    "tunnels": "(self, timeout=50)",
    "create_connect_token": "(self, user_metadata=None, port=8080)",
    "snapshot_filesystem": "(self, timeout=55, *, ttl=2592000)",
    "snapshot_directory": (
        "(self, path, *, timeout=55, ttl=2592000," " _experimental_encryption_key=None)"
    ),
    "mount_image": "(self, path, image, *, _experimental_encryption_key=None)",
    "unmount_image": "(self, path)",
    "reload_volumes": "(self, *, timeout=55)",
    "detach": "(self)",
    "open": "(self, path, mode='r')",
    "ls": "(self, path)",
    "mkdir": "(self, path, parents=False)",
    "rm": "(self, path, recursive=False)",
    "watch": "(self, path, filter=None, recursive=None, timeout=None)",
    "wait": "(self, raise_on_termination=True)",
    "poll": "(self)",
    "wait_until_ready": "(self, *, timeout=300)",
    "terminate": "(self, *, wait=False)",
}

FILESYSTEM_SIGNATURES = {
    "read_bytes": "(self, remote_path)",
    "read_text": "(self, remote_path)",
    "stat": "(self, remote_path)",
    "list_files": "(self, remote_path)",
    "write_bytes": "(self, data, remote_path)",
    "write_text": "(self, data, remote_path)",
    "make_directory": "(self, remote_path, *, create_parents=True)",
    "remove": "(self, remote_path, *, recursive=False)",
    "copy_from_local": "(self, local_path, remote_path)",
    "copy_to_local": "(self, remote_path, local_path)",
    # Note the differences from Sandbox.watch above: Modal spells the first
    # parameter differently, makes the rest keyword-only, and defaults
    # `recursive` to False rather than None.
    "watch": "(self, remote_path, *, filter=None, recursive=False, timeout=None)",
}


def _bare_signature(fn):
    """The signature with annotations stripped, so only the shape is compared."""
    params = [
        p.replace(annotation=inspect.Parameter.empty)
        for p in inspect.signature(fn).parameters.values()
    ]
    return str(inspect.Signature(params))


@pytest.mark.parametrize("name,expected", sorted(MODAL_SIGNATURES.items()))
def test_sandbox_signatures_match_modal(name, expected):
    assert _bare_signature(getattr(_Sandbox, name)) == expected


@pytest.mark.parametrize("name,expected", sorted(FILESYSTEM_SIGNATURES.items()))
def test_filesystem_signatures_match_modal(name, expected):
    impl = modal.SandboxFilesystem._impl_class
    assert _bare_signature(getattr(impl, name)) == expected


@pytest.mark.parametrize(
    "name", sorted(set(MODAL_SIGNATURES) | set(FILESYSTEM_SIGNATURES))
)
def test_no_modal_method_erases_its_signature(name):
    """`*args`/`**kwargs` is never the right shape for a Modal-facing method."""
    for cls in (_Sandbox, modal.SandboxFilesystem._impl_class):
        fn = getattr(cls, name, None)
        if fn is None:
            continue
        kinds = {p.kind for p in inspect.signature(fn).parameters.values()}
        assert inspect.Parameter.VAR_KEYWORD not in kinds, name
        assert inspect.Parameter.VAR_POSITIONAL not in kinds, name


def test_watch_reaches_the_refusal_when_given_modal_arguments():
    """Both spellings, with every parameter Modal documents.

    These used to be `**kwargs` stubs, so `path=` reached a TypeError on the
    Sandbox and the filesystem's parameters were silently dropped.
    """
    sb = detached_sandbox()
    with pytest.raises(NotImplementedError):
        sb.watch(path="/tmp", filter=None, recursive=True, timeout=5)
    with pytest.raises(NotImplementedError):
        sb.filesystem.watch("/tmp", filter=None, recursive=True, timeout=5)


@pytest.mark.parametrize("method", ["write_bytes", "write_text"])
def test_writes_validate_the_path_before_the_data_type(method):
    """Modal checks the path first, so a call that gets both wrong agrees.

    Both checks happen before any actor is touched, which is why this needs no
    cluster.
    """
    fs = detached_sandbox().filesystem
    with pytest.raises(InvalidError, match="absolute"):
        getattr(fs, method)(object(), "relative/path")


@pytest.mark.parametrize(
    "method,data", [("write_bytes", "text-not-bytes"), ("write_text", b"bytes-not-str")]
)
def test_writes_still_reject_the_wrong_data_type(method, data):
    fs = detached_sandbox().filesystem
    with pytest.raises(TypeError, match="data argument must be"):
        getattr(fs, method)(data, "/tmp/target")


@pytest.mark.parametrize("timeout", [0, -1])
def test_wait_until_ready_rejects_a_nonpositive_timeout(timeout):
    """Modal checks this before touching the sandbox, so no actor is needed."""
    sb = detached_sandbox(readiness_probe=modal.Probe.with_exec("true"))
    with pytest.raises(InvalidError, match="must be positive"):
        sb.wait_until_ready(timeout=timeout)


def test_wait_until_ready_on_a_gone_sandbox_is_a_conflict():
    """Modal reports a Sandbox that was already gone as ConflictError.

    A Sandbox that dies *while* the wait is parked still raises
    SandboxTerminatedError -- that path is covered by the integration suite's
    test_terminating_mid_probe_raises_rather_than_hanging.
    """
    sb = detached_sandbox(readiness_probe=modal.Probe.with_exec("true"))
    sb._impl._actor_released = True

    with pytest.raises(modal.ConflictError):
        sb.wait_until_ready(timeout=30)


@pytest.mark.parametrize("exit_reason", ["timeout", "terminated", "completed", None])
def test_wait_until_ready_on_a_gone_sandbox_is_a_conflict_whatever_ended_it(
    exit_reason,
):
    """Modal raises ConflictError from _get_task_id for any finished Sandbox;
    it has no branch that reports a timeout or termination here instead."""
    sb = detached_sandbox(readiness_probe=modal.Probe.with_exec("true"))
    sb._impl._actor_released = True
    sb._impl._exit_reason = exit_reason

    with pytest.raises(modal.ConflictError):
        sb.wait_until_ready(timeout=30)


# -- a Sandbox whose actor is gone -----------------------------------------


# -- resources ---------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [(None, None), (2, None), ((2, 4), 4), ([3, 6], 6), ((1,), None)],
)
def test_only_a_tuple_carries_a_hard_limit(value, expected):
    """Modal: a bare value is a request the container may burst above."""
    assert sandbox_mod._limit(value) == expected


@pytest.mark.parametrize(
    "name,value,message",
    [
        ("cpu", (0, 4), "CPU request must be a positive number"),
        ("cpu", (2, 0), "CPU limit must be a positive number"),
        ("cpu", (4, 2), "CPU limit lower than request"),
        ("memory", (1024, 512), "memory limit lower than request"),
    ],
)
def test_resource_tuples_are_validated_as_modal_does(name, value, message):
    with pytest.raises(InvalidError, match=message):
        sandbox_mod._validate_resource(name, value)


@pytest.mark.parametrize("value", [None, 2, (1, 1), (1, 4), (256, 1024)])
def test_valid_resources_pass(value):
    sandbox_mod._validate_resource("cpu", value)
    sandbox_mod._validate_resource("memory", value)


# -- detach ------------------------------------------------------------------


@pytest.mark.parametrize(
    "call",
    [
        lambda sb: sb.poll(),
        lambda sb: sb.exec("true"),
        lambda sb: sb.stdout,
        lambda sb: sb.stdin,
        lambda sb: sb.filesystem,
        lambda sb: sb.wait_until_ready(timeout=5),
        lambda sb: sb.ls("/"),
    ],
)
def test_a_detached_handle_refuses_further_operations(call):
    """Measured on Modal: poll() and exec() raise ClientClosed after detach()."""
    sb = detached_sandbox(
        main_exec_id="main", readiness_probe=modal.Probe.with_exec("true")
    )
    sb.detach()
    with pytest.raises(modal_exception.ClientClosed):
        call(sb)


def test_detach_is_idempotent_and_leaves_returncode_readable():
    sb = detached_sandbox()
    sb.detach()
    sb.detach()
    assert sb.returncode is None


def test_wait_until_ready_without_a_probe_is_a_conflict():
    """Measured on Modal: ConflictError, which is also an InvalidError."""
    with pytest.raises(modal.ConflictError):
        detached_sandbox().wait_until_ready(timeout=5)


class _ExecRecorder:
    """Accepts exec_start and records nothing else; enough to build a process."""

    def __getattr__(self, name):
        return self

    def options(self, **kwargs):
        return self

    def remote(self, *args, **kwargs):
        async def _exec_id():
            return "exec-1"

        return _exec_id()


def test_a_negative_exec_timeout_is_refused():
    """Modal's request builder refuses it; accepted, the handle would report
    -1 while the actor let the command run on."""
    sb = Sandbox._from_impl(_Sandbox(_ExecRecorder(), "ray-sandbox-test", None))
    with pytest.raises(ValueError, match="must not be negative"):
        sb.exec("true", timeout=-1)


@pytest.mark.parametrize("timeout,has_deadline", [(None, False), (0, False), (5, True)])
def test_a_zero_exec_timeout_means_no_deadline(timeout, has_deadline):
    """Modal's client reads 0 as no timeout; a deadline of "now" would have
    killed the command on its first poll()."""
    sb = Sandbox._from_impl(_Sandbox(_ExecRecorder(), "ray-sandbox-test", None))
    process = sb.exec("true", timeout=timeout)
    assert (process._impl._exec_deadline is not None) is has_deadline


class _DeadActor:
    """Every call fails the way an actor that has gone away does."""

    def __getattr__(self, name):
        return self

    def options(self, **kwargs):
        return self

    def remote(self, *args, **kwargs):
        raise ray.exceptions.RayActorError()


@pytest.mark.parametrize(
    "call",
    [
        lambda sb: sb.exec("echo", "hi"),
        lambda sb: sb.poll(),
        lambda sb: sb.wait(),
    ],
)
def test_a_vanished_actor_is_reported_as_the_sandbox_being_gone(call):
    """One condition, one exception, wherever it is met.

    Sandbox.filesystem already translated RayActorError; exec/poll/wait did
    not, so the same dead sandbox surfaced as a Modal error through one and a
    raw Ray error through the other.
    """
    sb = Sandbox._from_impl(_Sandbox(_DeadActor(), "ray-sandbox-test", None))
    with pytest.raises(modal.NotFoundError, match="already shut down"):
        call(sb)


class _Method:
    def __init__(self, fn):
        self._fn = fn

    def options(self, **kwargs):
        return self

    def remote(self, *args, **kwargs):
        return self._fn(*args, **kwargs)


class _LifecycleActor:
    """wait_sandbox and terminate only: reaching for get_state would fail."""

    def __init__(self, outcome, wait_sandbox=None):
        self._outcome = outcome
        self._wait_override = wait_sandbox
        self.calls = []

    def __getattr__(self, name):
        impl = getattr(type(self), f"_{name}", None)
        if impl is None:
            raise AttributeError(name)
        return _Method(lambda *a, **k: impl(self, *a, **k))

    async def _wait_sandbox(self, timeout):
        self.calls.append("wait_sandbox")
        if self._wait_override is not None:
            return await self._wait_override()
        return self._outcome

    async def _terminate(self):
        self.calls.append("terminate")
        return self._outcome


_TERMINATED = {"returncode": 137, "exit_reason": "terminated"}


def test_wait_learns_how_the_sandbox_ended_in_one_call():
    """A second call for the reason could lose the race to terminate()."""
    actor = _LifecycleActor(_TERMINATED)
    sb = Sandbox._from_impl(_Sandbox(actor, "ray-sandbox-test", None))
    with pytest.raises(SandboxTerminatedError):
        sb.wait()
    assert actor.calls == ["wait_sandbox"]
    assert sb.returncode == 137


def test_terminate_learns_the_outcome_in_one_call(monkeypatch):
    # Releasing the actor calls ray.kill, which would start a local cluster
    # just to reject this fake handle.
    monkeypatch.setattr(ray, "kill", lambda actor: None)
    actor = _LifecycleActor(_TERMINATED)
    sb = Sandbox._from_impl(_Sandbox(actor, "ray-sandbox-test", None))
    assert sb.terminate(wait=True) == 137
    assert actor.calls == ["terminate"]
    # Answered from what terminate() recorded, with no call to a dead actor.
    with pytest.raises(SandboxTerminatedError):
        sb.wait()
    assert actor.calls == ["terminate"]


def test_wait_racing_terminate_reports_termination_not_a_vanished_sandbox():
    """terminate() in another thread kills the actor under a parked wait().

    The wait used to surface that as NotFoundError; Modal says terminated.
    """
    impl = None

    async def killed_mid_call():
        # What terminate() leaves behind by the time the actor is gone.
        impl._returncode = 137
        impl._exit_reason = "terminated"
        impl._actor_released = True
        raise ray.exceptions.RayActorError()

    actor = _LifecycleActor(_TERMINATED, wait_sandbox=killed_mid_call)
    impl = _Sandbox(actor, "ray-sandbox-test", None)
    sb = Sandbox._from_impl(impl)
    with pytest.raises(SandboxTerminatedError):
        sb.wait()
    sb.wait(raise_on_termination=False)
    assert sb.returncode == 137


# -- the network default ---------------------------------------------------


def test_creating_a_sandbox_warns_that_the_network_is_not_isolated(caplog, monkeypatch):
    """block_network=False egresses through this node's network here, where
    the same default on Modal is an isolated network. The default is kept for
    parity, so the difference has to be said out loud."""
    monkeypatch.setattr(sandbox_mod, "_host_network_warned", False)

    with caplog.at_level("WARNING"):
        # Refused after the warning, which keeps this off a real cluster.
        with pytest.raises(NotImplementedError):
            Sandbox.create(image="busybox:latest", pty=True)
        sandbox_mod._warn_host_network()

    assert "instance-metadata" in caplog.text
    assert "block_network=True" in caplog.text


def test_the_network_warning_is_said_once_per_process(caplog, monkeypatch):
    monkeypatch.setattr(sandbox_mod, "_host_network_warned", False)
    with caplog.at_level("WARNING"):
        sandbox_mod._warn_host_network()
        sandbox_mod._warn_host_network()
    assert caplog.text.count("block_network=True") == 1


def test_blocking_the_network_says_nothing(caplog, monkeypatch):
    monkeypatch.setattr(sandbox_mod, "_host_network_warned", False)
    with caplog.at_level("WARNING"):
        with pytest.raises(NotImplementedError):
            Sandbox.create(image="busybox:latest", pty=True, block_network=True)
    assert caplog.text == ""


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
