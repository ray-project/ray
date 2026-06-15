"""CPU-only unit tests for NIXL backend selection.

These tests cover the backend-selection logic in
``ray.experimental.rdt.nixl_tensor_transport`` without requiring a GPU, NIXL, or
EFA hardware. They exercise the hardware-to-backend mapping (host vs. container
EFA layouts and non-EFA RDMA), the ``RAY_NIXL_BACKEND`` override, and the
LIBFABRIC GPUDirect validation
"""

import sys

import pytest

from ray.experimental.rdt import nixl_tensor_transport as ntt
from ray.experimental.rdt.nixl_tensor_transport import (
    NixlTensorTransport,
    _is_efa_available,
)


@pytest.fixture(autouse=True)
def _clear_caches(monkeypatch):
    # _is_efa_available is lru_cached; clear it so each test sees fresh globs.
    _is_efa_available.cache_clear()
    monkeypatch.delenv("RAY_NIXL_BACKEND", raising=False)
    yield
    _is_efa_available.cache_clear()


def _patch_globs(monkeypatch, present):
    """Make glob.glob return a match only for patterns in ``present``.

    The returned path is derived from the pattern (its trailing ``*`` replaced)
    so that, e.g., ``/sys/class/infiniband/*`` yields a path under
    ``/sys/class/infiniband/`` that ``_patch_ib_driver`` can recognize.
    """

    def fake_glob(pattern):
        return [pattern.replace("*", "dev0")] if pattern in present else []

    monkeypatch.setattr(ntt.glob, "glob", fake_glob)


def _patch_ib_driver(monkeypatch, driver):
    """Make every /sys/class/infiniband device resolve to ``driver``."""
    real_realpath = ntt.os.path.realpath

    def fake_realpath(path):
        if path.startswith("/sys/class/infiniband/"):
            return f"/sys/bus/pci/drivers/{driver}"
        return real_realpath(path)

    monkeypatch.setattr(ntt.os.path, "realpath", fake_realpath)


@pytest.mark.parametrize(
    "globs,ib_driver,expected",
    [
        # Host: EFA exposes an efa* netdev.
        ({"/sys/class/net/efa*"}, None, "LIBFABRIC"),
        # Container: netdev is namespaced away, but the EFA device plugin mounts
        # verbs devices bound to the efa kernel driver.
        ({"/sys/class/infiniband/*"}, "efa", "LIBFABRIC"),
        # Ordinary InfiniBand/RoCE exposes verbs devices too, but under a
        # different driver, so it must not be treated as EFA.
        ({"/sys/class/infiniband/*"}, "mlx5_core", "UCX"),
        # No RDMA hardware at all.
        (set(), None, "UCX"),
    ],
)
def test_select_backend_from_hardware(monkeypatch, globs, ib_driver, expected):
    _patch_globs(monkeypatch, globs)
    if ib_driver is not None:
        _patch_ib_driver(monkeypatch, ib_driver)
    assert NixlTensorTransport().select_backend() == expected


def test_override_wins_over_autodetect(monkeypatch):
    # Override beats EFA auto-detection, and is normalized (case-insensitive).
    _patch_globs(monkeypatch, {"/sys/class/net/efa*"})
    monkeypatch.setenv("RAY_NIXL_BACKEND", "ucx")
    assert NixlTensorTransport().select_backend() == "UCX"


def test_invalid_override_raises(monkeypatch):
    monkeypatch.setenv("RAY_NIXL_BACKEND", "tcp")
    with pytest.raises(ValueError):
        NixlTensorTransport().select_backend()


def _capture_created_backends(monkeypatch):
    """Stub _make_nixl_agent and return the list of backends it's asked to build."""
    created = []

    def fake_make_agent(self, backend):
        created.append(backend)
        return object()

    monkeypatch.setattr(NixlTensorTransport, "_make_nixl_agent", fake_make_agent)
    return created


def test_autodetected_libfabric_commits_when_probe_passes(monkeypatch):
    _patch_globs(monkeypatch, {"/sys/class/net/efa*"})
    created = _capture_created_backends(monkeypatch)
    monkeypatch.setattr(
        NixlTensorTransport,
        "_libfabric_registration_works",
        lambda self, agent: True,
    )

    transport = NixlTensorTransport()
    transport.get_nixl_agent()

    assert created == ["LIBFABRIC"]
    assert transport._backend == "LIBFABRIC"


def test_autodetected_libfabric_raises_when_probe_fails(monkeypatch):
    """A failed validation probe signals a misconfigured instance, so fail loudly."""
    _patch_globs(monkeypatch, {"/sys/class/net/efa*"})
    _capture_created_backends(monkeypatch)
    monkeypatch.setattr(
        NixlTensorTransport,
        "_libfabric_registration_works",
        lambda self, agent: False,
    )

    transport = NixlTensorTransport()
    with pytest.raises(RuntimeError, match="LIBFABRIC"):
        transport.get_nixl_agent()


def test_override_skips_probe(monkeypatch):
    """An explicit LIBFABRIC override must not run the validation probe."""
    _patch_globs(monkeypatch, set())
    monkeypatch.setenv("RAY_NIXL_BACKEND", "LIBFABRIC")
    created = _capture_created_backends(monkeypatch)

    def fail_probe(self, agent):
        raise AssertionError("probe must not run for an explicit override")

    monkeypatch.setattr(
        NixlTensorTransport, "_libfabric_registration_works", fail_probe
    )

    transport = NixlTensorTransport()
    transport.get_nixl_agent()

    assert created == ["LIBFABRIC"]
    assert transport._backend == "LIBFABRIC"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
