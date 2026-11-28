"""SandboxRuntime picks its backend from an argument or RAY_SANDBOX_BACKEND."""

import json
import sys

import pytest

from ray.experimental.sandbox.backend import registry
from ray.experimental.sandbox.backend.base import (
    BaseSandboxBackend,
    ExecResult,
    SandboxStatus,
)
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.exceptions import SandboxCreationError, SandboxError
from ray.experimental.sandbox.runtime import SandboxRuntime


class FakeBackend(BaseSandboxBackend):
    """A backend that records what the runtime asks of it."""

    def __init__(self, image_manager=None):
        super().__init__(image_manager=image_manager or object())
        self.created = []

    def create_sandbox(self, config):
        self.created.append(config)
        return f"fake-{len(self.created)}"

    def delete_sandbox(self, sandbox_id):
        pass

    def exec_command(self, sandbox_id, command, timeout=None, **kwargs):
        return ExecResult(0, f"{sandbox_id}:{command}", "", 0.0)

    def write_file(self, sandbox_id, path, content, append=False):
        pass

    def read_file(self, sandbox_id, path):
        return b""

    def get_status(self, sandbox_id):
        return SandboxStatus.RUNNING

    def checkpoint_sandbox(self, sandbox_id, checkpoint_path=None, **kwargs):
        return {"checkpoint_path": checkpoint_path or f"fake://{sandbox_id}"}

    def restore_sandbox(self, checkpoint_path, **kwargs):
        return "restored"

    def pause_sandbox(self, sandbox_id, timeout_seconds=None):
        pass

    def resume_sandbox(self, sandbox_id, timeout_seconds=None):
        pass


@pytest.fixture
def fake_registered():
    registry.register_backend("fake", FakeBackend)
    yield
    with registry._lock:
        registry._factories.pop("fake", None)


def test_default_is_gvisor(monkeypatch):
    monkeypatch.delenv(registry.RAY_SANDBOX_BACKEND_ENV, raising=False)
    rt = SandboxRuntime()
    assert isinstance(rt.backend, GVisorSandboxBackend)
    assert rt.backend.image_manager is rt.image_manager


def test_backend_instance_argument():
    backend = FakeBackend()
    rt = SandboxRuntime(backend=backend)
    assert rt.backend is backend
    assert rt.image_manager is backend.image_manager
    sid = rt.create(image="busybox")
    assert sid == "fake-1"
    assert rt.exec(sid, "true").stdout == "fake-1:true"
    assert backend.created[0].image == "busybox"


def test_backend_name_argument(fake_registered, monkeypatch):
    monkeypatch.setenv(registry.RAY_SANDBOX_BACKEND_ENV, "gvisor")
    rt = SandboxRuntime(backend="FAKE")
    assert isinstance(rt.backend, FakeBackend)
    assert rt.backend.image_manager is rt.image_manager


def test_env_var_name(fake_registered, monkeypatch):
    monkeypatch.setenv(registry.RAY_SANDBOX_BACKEND_ENV, "fake")
    rt = SandboxRuntime()
    assert isinstance(rt.backend, FakeBackend)
    assert rt.checkpoint(rt.create(image="busybox"), leave_running=False) == (
        "fake://fake-1"
    )
    assert rt.restore("fake://fake-1") == "restored"


def test_env_var_import_path(monkeypatch):
    monkeypatch.setenv(registry.RAY_SANDBOX_BACKEND_ENV, f"{__name__}:FakeBackend")
    assert isinstance(SandboxRuntime().backend, FakeBackend)


def test_entry_point(monkeypatch):
    class _EntryPoint:
        name = "viaentrypoint"

        def load(self):
            return FakeBackend

    def _entry_points(group):
        assert group == registry.ENTRY_POINT_GROUP
        return [_EntryPoint()]

    monkeypatch.setattr(registry, "_entry_point_cache", {})
    monkeypatch.setattr("importlib.metadata.entry_points", _entry_points)
    monkeypatch.setenv(registry.RAY_SANDBOX_BACKEND_ENV, "viaentrypoint")
    assert isinstance(SandboxRuntime().backend, FakeBackend)


class NoImagesBackend(FakeBackend):
    needs_image_manager = False

    def __init__(self, image_manager=None):
        self.got_image_manager = image_manager
        super().__init__(image_manager=image_manager)


def test_backend_without_image_manager(monkeypatch):
    built = []
    monkeypatch.setattr(
        "ray.experimental.sandbox.runtime.ImageManager",
        lambda: built.append(1) or object(),
    )
    monkeypatch.setenv(registry.RAY_SANDBOX_BACKEND_ENV, f"{__name__}:NoImagesBackend")
    rt = SandboxRuntime()
    assert rt.backend.got_image_manager is None
    assert not built
    assert rt.image_manager is rt.backend.image_manager


def test_restore_replaces_the_ttl_timer_of_the_same_id():
    class ResumeInPlace(FakeBackend):
        def restore_sandbox(self, checkpoint_path, **kwargs):
            return "fake-1"  # the checkpointed sandbox itself

    rt = SandboxRuntime(backend=ResumeInPlace())
    sid = rt.create(image="busybox", ttl_seconds=3600)
    old = rt._ttl_timers[sid]
    assert rt.restore("fake://fake-1", ttl_seconds=7200) == sid
    assert not old.is_alive() or old.finished.is_set()
    assert rt._ttl_timers[sid] is not old
    rt.delete(sid)


def test_commit_needs_backend_support():
    rt = SandboxRuntime(backend=FakeBackend())
    with pytest.raises(NotImplementedError, match="FakeBackend"):
        rt.commit("fake-1", "registry.example/repo:tag")


def test_entry_point_lookup_is_cached(monkeypatch):
    calls = []

    class _EntryPoint:
        name = "cachedep"

        def load(self):
            return FakeBackend

    def _entry_points(group):
        calls.append(group)
        return [_EntryPoint()]

    monkeypatch.setattr(registry, "_entry_point_cache", {})
    monkeypatch.setattr("importlib.metadata.entry_points", _entry_points)
    for _ in range(3):
        assert registry.resolve_backend_factory("cachedep") is FakeBackend
    assert len(calls) == 1


def test_check_resources_delegates_to_the_backend():
    class Checks(FakeBackend):
        def check_resources(self, cpu=0.0, memory=0):
            self.checked = (cpu, memory)

    rt = SandboxRuntime(backend=Checks())
    rt.check_resources(cpu=64.0, memory="1Ti")  # no local cgroup check
    assert rt.backend.checked == (64.0, "1Ti")

    class Fails(FakeBackend):
        def check_resources(self, cpu=0.0, memory=0):
            raise ValueError("no cgroup v2 here")

    with pytest.raises(SandboxCreationError, match="no cgroup v2 here"):
        SandboxRuntime(backend=Fails()).check_resources(cpu=1.0)
    assert BaseSandboxBackend.check_resources is not None  # the default exists


def _checkpoint_dir(path, manifest=None):
    (path / "state").mkdir(parents=True)
    (path / "manifest.json").write_text(
        json.dumps(manifest or {"version": "1.0", "sandbox_id": "sb-1"})
    )
    return path


def test_delete_checkpoint(tmp_path):
    ckpt = _checkpoint_dir(tmp_path / "ckpt")
    rt = SandboxRuntime(backend=FakeBackend())
    rt.delete_checkpoint(str(ckpt))
    assert not ckpt.exists()
    with pytest.raises(SandboxError):
        rt.delete_checkpoint(str(ckpt))


def test_delete_checkpoint_refuses_what_isnt_one(tmp_path, monkeypatch):
    rt = SandboxRuntime(backend=FakeBackend())
    stray = tmp_path / "webapp"
    stray.mkdir()
    (stray / "manifest.json").write_text('{"name": "my-extension"}')
    (stray / "index.js").write_text("keep me")
    with pytest.raises(SandboxError):
        rt.delete_checkpoint(str(stray))  # no state/
    (stray / "state").mkdir()
    with pytest.raises(SandboxError, match="not a checkpoint"):
        rt.delete_checkpoint(str(stray))  # no version/sandbox_id
    assert (stray / "index.js").read_text() == "keep me"
    here = _checkpoint_dir(tmp_path / "cwd")
    monkeypatch.chdir(here)
    with pytest.raises(SandboxError, match="Refusing"):
        rt.delete_checkpoint(".")
    with pytest.raises(SandboxError, match="Refusing"):
        rt.delete_checkpoint("/")
    assert here.exists()


def test_restore_refuses_another_backends_checkpoint(tmp_path, fake_registered):
    class GVisorLike(FakeBackend):
        checkpoint_format = "gvisor"

    rt = SandboxRuntime(backend=GVisorLike())
    ckpt = tmp_path / "ckpt"
    ckpt.mkdir()
    (ckpt / "manifest.json").write_text(json.dumps({"backend": "fake"}))
    for bad in (str(ckpt), "fake://env-1"):
        with pytest.raises(SandboxError, match="taken by the fake"):
            rt.restore(bad)
        with pytest.raises(SandboxError, match="taken by the fake"):
            rt.delete_checkpoint(bad)
    (ckpt / "manifest.json").write_text("{}")  # gVisor's: no "backend"
    assert rt.restore(str(ckpt)) == "restored"
    # Only registered backend names count as schemes.
    assert rt.restore("file:///somewhere/ckpt") == "restored"


def test_unknown_backend(monkeypatch):
    monkeypatch.setenv(registry.RAY_SANDBOX_BACKEND_ENV, "no-such-backend")
    with pytest.raises(ValueError, match="no-such-backend"):
        SandboxRuntime()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
