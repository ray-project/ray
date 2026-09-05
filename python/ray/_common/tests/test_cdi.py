import sys

import pytest

from ray._common import cdi, cdi_lib


class _FakeManager:
    def __init__(self, kind, spec=None):
        self._kind = kind
        self._spec = spec
        self.generate_calls = 0

    def get_cdi_kind(self):
        return self._kind

    def generate_cdi_spec(self):
        self.generate_calls += 1
        return self._spec


def test_get_spec_returns_none_without_a_manager(monkeypatch):
    monkeypatch.setattr(
        "ray._private.accelerators.get_accelerator_manager_for_resource",
        lambda resource_name: None,
    )
    assert cdi.get_spec("GPU") is None


def test_get_spec_returns_none_when_manager_lacks_cdi_support(monkeypatch):
    manager = _FakeManager(kind=None)
    monkeypatch.setattr(
        "ray._private.accelerators.get_accelerator_manager_for_resource",
        lambda resource_name: manager,
    )
    assert cdi.get_spec("GPU") is None


def test_get_spec_generates_via_resolved_manager(monkeypatch):
    """Not NVIDIA-specific: any manager implementing get_cdi_kind()/
    generate_cdi_spec() works here, resolved via
    get_accelerator_manager_for_resource — the same mechanism that already
    decides a node's "GPU" resource means NVIDIA vs. AMD vs. Apple."""
    monkeypatch.setattr(cdi_lib, "_generated_spec_cache", {})
    spec = {"kind": "acme.com/widget", "devices": []}
    manager = _FakeManager(kind="acme.com/widget", spec=spec)
    monkeypatch.setattr(
        "ray._private.accelerators.get_accelerator_manager_for_resource",
        lambda resource_name: manager,
    )

    resolved = cdi.get_spec("WIDGET")
    assert isinstance(resolved, cdi_lib.CDISpec)
    assert resolved.kind == "acme.com/widget"
    assert manager.generate_calls == 1

    # Second call hits cdi_lib's in-memory cache; generator isn't re-invoked.
    cdi.get_spec("WIDGET")
    assert manager.generate_calls == 1


def test_get_spec_result_selects_devices_by_resolved_kind(monkeypatch):
    """The returned CDISpec is qualified by the resolved kind — callers
    don't need to pass the resource name again to select devices."""
    monkeypatch.setattr(cdi_lib, "_generated_spec_cache", {})
    spec = {
        "kind": "nvidia.com/gpu",
        "devices": [{"name": "0"}, {"name": "1"}],
    }
    manager = _FakeManager(kind="nvidia.com/gpu", spec=spec)
    monkeypatch.setattr(
        "ray._private.accelerators.get_accelerator_manager_for_resource",
        lambda resource_name: manager,
    )

    resolved = cdi.get_spec("GPU")
    devices = resolved.select_devices(["0", "1"])
    assert [d["name"] for d in devices] == ["0", "1"]


def test_nvidia_gpu_manager_wired_up_for_cdi():
    """Sanity check that the real NvidiaGPUAcceleratorManager (not a mock)
    implements the CDI interface cdi.py depends on."""
    from ray._private.accelerators import NvidiaGPUAcceleratorManager

    assert NvidiaGPUAcceleratorManager.get_cdi_kind() == "nvidia.com/gpu"
    assert callable(NvidiaGPUAcceleratorManager.generate_cdi_spec)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
