"""CPU-only unit tests for NIXL backend selection.

These tests cover the backend-selection logic in
``ray.experimental.rdt.nixl_tensor_transport`` without requiring a GPU, NIXL, or
EFA hardware. They exercise the hardware-to-backend mapping (host vs. container
EFA layouts and non-EFA RDMA).
"""

import sys

import pytest

from ray.experimental.rdt import nixl_tensor_transport as ntt
from ray.experimental.rdt.nixl_tensor_transport import (
    NixlTensorTransport,
    _is_efa_available,
    _nixl_transport_available_in_process,
)


@pytest.fixture(autouse=True)
def _clear_caches(monkeypatch):
    # _is_efa_available is lru_cached; clear it so each test sees fresh globs.
    _is_efa_available.cache_clear()
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


@pytest.mark.parametrize(
    "exc",
    [
        ImportError("nixl is not installed"),
        RuntimeError("LIBFABRIC probe failed"),
    ],
)
def test_nixl_transport_available_in_process_returns_false_on_init_failure(
    monkeypatch, exc
):
    def fail_init(self):
        raise exc

    monkeypatch.setattr(NixlTensorTransport, "get_nixl_agent", fail_init)
    assert _nixl_transport_available_in_process() is False


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
