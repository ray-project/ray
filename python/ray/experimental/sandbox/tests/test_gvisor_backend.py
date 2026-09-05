import os
import sys
from unittest.mock import patch

import pytest

import ray
from ray._common import cdi_lib
from ray.actor import ActorHandle
from ray.experimental.sandbox import create
from ray.experimental.sandbox.backend.base import SandboxStatus
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import GVisorSandboxConfig
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxNotFoundError,
)
from ray.experimental.sandbox.runtime import SandboxRuntime


def test_gvisor_backend_local_lifecycle_and_file_ops():
    backend = GVisorSandboxBackend()
    config = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        workdir="/workspace",
        cpu=1.0,
        memory="512Mi",
    )

    sandbox_id = backend.create_sandbox(config)
    assert sandbox_id.startswith("ray-sandbox-")
    assert sandbox_id in backend._sandbox_metadata
    assert backend.get_status(sandbox_id) == SandboxStatus.RUNNING

    # Test file write and read
    backend.write_file(sandbox_id, "/workspace/script.py", "print('Hello gVisor')")
    content = backend.read_file(sandbox_id, "/workspace/script.py")
    assert content == b"print('Hello gVisor')"

    # Test exec command
    res = backend.exec_command(sandbox_id, "echo 'Process isolation'")
    assert res.exit_code == 0
    assert "Process isolation" in res.stdout

    # Test delete
    backend.delete_sandbox(sandbox_id)
    assert backend.get_status(sandbox_id) == SandboxStatus.TERMINATED
    assert sandbox_id not in backend._sandbox_metadata


def test_gvisor_backend_not_found():
    backend = GVisorSandboxBackend()
    with pytest.raises(SandboxNotFoundError):
        backend.exec_command("nonexistent-id", "echo 'hi'")


def test_create_sandbox_helper():
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    sb = create("busybox:latest", workdir="/workspace", shell="/bin/sh")
    assert isinstance(sb, ActorHandle)
    res = ray.get(sb.exec.remote("echo 'Process isolation'"))
    assert res.exit_code == 0
    assert "Process isolation" in res.stdout
    assert res.duration_ms >= 0
    ray.get(sb.terminate.remote())


def test_create_threads_num_gpus_into_actor_options():
    """create()'s num_gpus must reach Sandbox.options() (Ray's own actor
    scheduling), the same way cpu/memory already do -- otherwise a
    caller's GPU request is silently dropped end-to-end: never seen by
    Ray's scheduler, and Sandbox.__init__'s own gpu_ids auto-inherit
    (ray.get_gpu_ids()) then just finds nothing, with no error telling
    the caller why."""
    captured_options = {}

    class _FakeSandboxHandle:
        def remote(self, *args, **kwargs):
            return "sandbox-actor-handle"

    def fake_options(**kwargs):
        captured_options.update(kwargs)
        return _FakeSandboxHandle()

    with patch("ray.experimental.sandbox.Sandbox.options", side_effect=fake_options):
        create("busybox:latest", num_gpus=2)

    assert captured_options.get("num_gpus") == 2


def test_create_num_gpus_reaches_real_ray_scheduling():
    """Unlike the mocked test above, this spawns a real actor against a
    fake-GPU cluster (ray.init(num_gpus=1) is a logical count, no real
    GPU needed) to prove num_gpus reaches Ray's actual scheduler rather
    than being silently dropped. Can't mock CDI here like the tests above
    do -- create() spawns a separate worker process, and mock.patch only
    patches this one -- so whether CDI resolution itself then succeeds
    depends on the test machine (real GPU + nvidia-ctk, or neither).
    Either way is accepted as proof gpu_ids reached Sandbox.__init__: a
    resolved gpu_ids on success, or the same CDI-lookup failure as
    test_gvisor_backend_gpu_ids_without_cdi_spec_raises. Only a silent
    drop -- no error and no gpu_ids -- would indicate the bug this test
    guards against."""
    if ray.is_initialized():
        ray.shutdown()
    ray.init(num_gpus=1)

    try:
        try:
            actor = create("busybox:latest", num_gpus=1)
            config = ray.get(actor.get_config.remote())
            assert config.gpu_ids is not None
            ray.get(actor.delete.remote())
            ray.kill(actor)
        except ray.exceptions.ActorDiedError as e:
            assert "CDI" in str(e)
    finally:
        ray.shutdown()


def test_gvisor_backend_container_image_support():
    backend = GVisorSandboxBackend()
    config = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        workdir="/workspace",
    )
    sandbox_id = backend.create_sandbox(config)
    try:
        assert sandbox_id.startswith("ray-sandbox-")
        assert backend.get_status(sandbox_id) == SandboxStatus.RUNNING

        extracted_dir = "/tmp/ray/sandbox/images/busybox_latest"
        assert os.path.exists(extracted_dir)
        assert os.path.isdir(extracted_dir)
        assert os.path.exists(os.path.join(extracted_dir, ".extracted"))
        assert os.path.exists("/tmp/ray/sandbox/images/busybox_latest.tar")

        res = backend.exec_command(sandbox_id, "/bin/sh -c 'echo hello from busybox'")
        assert res.exit_code == 0
        assert "hello from busybox" in res.stdout
    finally:
        backend.delete_sandbox(sandbox_id)

    assert os.path.exists("/tmp/ray/sandbox/images/busybox_latest")


def test_gvisor_backend_image_required():
    with pytest.raises((TypeError, ValueError)):
        GVisorSandboxConfig(
            image=None,
            workdir="/workspace",
        )
    with pytest.raises((TypeError, ValueError)):
        GVisorSandboxConfig(
            workdir="/workspace",
        )


def test_gvisor_backend_invalid_image():
    backend = GVisorSandboxBackend()
    config = GVisorSandboxConfig(
        image="nonexistent_invalid_image_12345:latest",
        workdir="/workspace",
    )
    with pytest.raises(SandboxCreationError):
        backend.create_sandbox(config)


def test_gvisor_backend_container_image_overlay_isolation():
    backend = GVisorSandboxBackend()
    cfg1 = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        workdir="/workspace",
        readonly=False,
    )
    cfg2 = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        workdir="/workspace",
        readonly=False,
    )

    sb1 = backend.create_sandbox(cfg1)
    sb2 = backend.create_sandbox(cfg2)
    try:
        # SB1 writes to rootfs
        res1 = backend.exec_command(
            sb1, "/bin/sh -c 'echo sb1_root > /overlay_test.txt'"
        )
        assert res1.exit_code == 0

        # SB2 writes to rootfs with different content
        res2 = backend.exec_command(
            sb2, "/bin/sh -c 'echo sb2_root > /overlay_test.txt'"
        )
        assert res2.exit_code == 0

        # Verify SB1 sees sb1_root
        read1 = backend.exec_command(sb1, "cat /overlay_test.txt")
        assert read1.exit_code == 0
        assert "sb1_root" in read1.stdout

        # Verify SB2 sees sb2_root
        read2 = backend.exec_command(sb2, "cat /overlay_test.txt")
        assert read2.exit_code == 0
        assert "sb2_root" in read2.stdout

        # Base image rootfs must not contain /overlay_test.txt
        extracted_dir = "/tmp/ray/sandbox/images/busybox_latest"
        assert not os.path.exists(os.path.join(extracted_dir, "overlay_test.txt"))
    finally:
        backend.delete_sandbox(sb1)
        backend.delete_sandbox(sb2)

    # A newly created SB3 should not see /overlay_test.txt
    cfg3 = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        workdir="/workspace",
        readonly=False,
    )
    sb3 = backend.create_sandbox(cfg3)
    try:
        read3 = backend.exec_command(sb3, "/bin/sh -c 'test -f /overlay_test.txt'")
        assert read3.exit_code != 0
    finally:
        backend.delete_sandbox(sb3)


def test_gvisor_backend_readonly_rootfs():
    backend = GVisorSandboxBackend()
    # Default is readonly=True
    cfg = GVisorSandboxConfig(
        image="busybox:latest",
        shell="/bin/sh",
        workdir="/workspace",
    )
    assert cfg.readonly is True
    sandbox_id = backend.create_sandbox(cfg)
    try:
        # Writing to rootfs should fail because readonly=True by default
        res = backend.exec_command(
            sandbox_id, "/bin/sh -c 'echo test > /test_readonly.txt'"
        )
        assert res.exit_code != 0
        assert "Read-only file system" in res.stderr

        # Writing to /workspace should still succeed because it is mounted rw
        res_ws = backend.exec_command(
            sandbox_id,
            "/bin/sh -c 'echo ws_ok > /workspace/ws.txt && cat /workspace/ws.txt'",
        )
        assert res_ws.exit_code == 0
        assert "ws_ok" in res_ws.stdout
    finally:
        backend.delete_sandbox(sandbox_id)


def test_gvisor_backend_ignore_cgroups_flag():
    backend = GVisorSandboxBackend()
    cfg_default = GVisorSandboxConfig(image="busybox:latest", shell="/bin/sh")
    orig_env = os.environ.pop("RAY_SANDBOX_IGNORE_CGROUPS", None)
    try:
        args_default = backend._runsc_base_args(cfg_default)
        assert "--ignore-cgroups" not in args_default

        cfg_ignored = GVisorSandboxConfig(
            image="busybox:latest", shell="/bin/sh", _ignore_cgroups=True
        )
        args_ignored = backend._runsc_base_args(cfg_ignored)
        assert "--ignore-cgroups" in args_ignored
    finally:
        if orig_env is not None:
            os.environ["RAY_SANDBOX_IGNORE_CGROUPS"] = orig_env


def _gpu_sandbox_config():
    # Represents a sandbox built inside an actor Ray assigned GPU "0" to --
    # mocked so this doesn't depend on the test machine actually having a
    # GPU (ambient absence/presence is fragile; see the module-level note
    # on get_spec mocking below).
    with patch("ray.get_gpu_ids", return_value=["0"]):
        return GVisorSandboxConfig(
            image="busybox:latest", shell="/bin/sh", gpu_ids=["0"]
        )


def test_gvisor_backend_nvproxy_flag():
    backend = GVisorSandboxBackend()
    cfg_no_gpu = GVisorSandboxConfig(image="busybox:latest", shell="/bin/sh")
    assert "--nvproxy" not in backend._runsc_base_args(cfg_no_gpu)

    with patch(
        "ray._common.cdi.get_spec",
        return_value=cdi_lib.CDISpec("nvidia.com/gpu", {"devices": []}),
    ):
        assert "--nvproxy" in backend._runsc_base_args(_gpu_sandbox_config())


def test_gvisor_backend_cdi_flags_are_kind_driven_not_hardcoded():
    """The runsc flag selection isn't a blanket 'gpu_ids set -> --nvproxy':
    it looks up the resolved CDI kind generically (see
    gvisor._CDI_KIND_RUNSC_FLAGS), so a kind with no known runsc flag
    requirement gets none, not --nvproxy by default."""
    backend = GVisorSandboxBackend()
    with patch(
        "ray._common.cdi.get_spec",
        return_value=cdi_lib.CDISpec("acme.com/widget", {"devices": []}),
    ):
        assert "--nvproxy" not in backend._runsc_base_args(_gpu_sandbox_config())


def test_gvisor_backend_rejects_unsupported_gpu_cdi_kind():
    """gpu_ids resolved to a CDI kind gVisor GPU passthrough has no known
    runsc flag for (e.g. a hypothetical AMD device) must fail before any
    sandbox state is created, rather than attempting unverified
    passthrough."""
    backend = GVisorSandboxBackend()
    with patch(
        "ray._common.cdi.get_spec",
        return_value=cdi_lib.CDISpec("amd.com/gpu", {"devices": []}),
    ):
        with pytest.raises(SandboxCreationError, match="amd.com/gpu"):
            backend.create_sandbox(_gpu_sandbox_config())


def test_gvisor_backend_gpu_ids_without_cdi_spec_raises():
    """When no CDI spec for this node's GPUs can be generated, a sandbox
    requesting gpu_ids must fail loudly with a clear error. Mocks
    get_spec directly rather than relying on the test machine happening
    to have no nvidia-ctk on PATH — ambient absence is fragile if a CI
    image ever ships nvidia-ctk for unrelated reasons."""
    backend = GVisorSandboxBackend()
    with patch("ray._common.cdi.get_spec", return_value=None):
        with pytest.raises(SandboxCreationError, match="CDI"):
            backend.create_sandbox(_gpu_sandbox_config())


def test_string_exec_shell_configuration():
    """String commands run under config.shell (default /bin/bash) with a
    per-exec override; there is no auto-detection."""
    # busybox has /bin/sh but no /bin/bash: with the deterministic bash
    # default a string exec fails loudly instead of degrading to sh, so this
    # image configures the shell explicitly.
    runtime = SandboxRuntime()
    instance_id = runtime.create(
        image="busybox:latest", readonly=False, shell="/bin/sh"
    )
    try:
        result = runtime.exec(instance_id, "echo hello-$0")
        assert result.exit_code == 0
        assert "hello-" in result.stdout
        # Per-exec override beats the configured shell.
        result = runtime.exec(instance_id, "echo again", shell="/bin/sh")
        assert result.exit_code == 0
    finally:
        runtime.delete(instance_id)


def test_workdir_writability_matrix():
    """readonly=True + workdir=None -> nothing writable; explicit workdir is
    the only writable path; readonly=False -> everything writable."""
    runtime = SandboxRuntime()

    # Default (readonly=True, workdir=None): the rootfs is not writable.
    # (Standard tmpfs mounts like /tmp are, as in any container runtime.)
    instance_id = runtime.create(image="busybox:latest", shell="/bin/sh")
    try:
        assert runtime.exec(instance_id, "touch /probe").exit_code != 0
        assert runtime.exec(instance_id, "touch /etc/probe").exit_code != 0
    finally:
        runtime.delete(instance_id)

    # readonly=True, explicit workdir: it is the only writable path.
    instance_id = runtime.create(
        image="busybox:latest", workdir="/data", shell="/bin/sh"
    )
    try:
        assert runtime.exec(instance_id, "touch /data/probe").exit_code == 0
        assert runtime.exec(instance_id, "touch /etc/probe").exit_code != 0
        assert runtime.exec(instance_id, "pwd").stdout.strip() == "/data"
    finally:
        runtime.delete(instance_id)

    # readonly=False: everything is writable, with or without a workdir.
    instance_id = runtime.create(
        image="busybox:latest", readonly=False, shell="/bin/sh"
    )
    try:
        assert runtime.exec(instance_id, "touch /etc/probe").exit_code == 0
    finally:
        runtime.delete(instance_id)


def test_image_workdir_sets_cwd_without_becoming_writable():
    """The image's own WORKDIR is inherited as the process cwd only — its
    content stays visible and it is never silently made writable."""
    runtime = SandboxRuntime()
    # golang:alpine sets WORKDIR /go and ships /go/bin and /go/src.
    instance_id = runtime.create(image="golang:1.22-alpine", shell="/bin/sh")
    try:
        assert runtime.exec(instance_id, "pwd").stdout.strip() == "/go"
        listing = runtime.exec(instance_id, "ls /go").stdout
        assert "bin" in listing and "src" in listing
        # Inherited WORKDIR is not a scratch mount: still readonly.
        assert runtime.exec(instance_id, "touch /go/probe").exit_code != 0
    finally:
        runtime.delete(instance_id)

    # With a writable rootfs the same path is writable and unshadowed.
    instance_id = runtime.create(
        image="golang:1.22-alpine", readonly=False, shell="/bin/sh"
    )
    try:
        assert "bin" in runtime.exec(instance_id, "ls /go").stdout
        assert runtime.exec(instance_id, "touch /go/probe").exit_code == 0
    finally:
        runtime.delete(instance_id)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
