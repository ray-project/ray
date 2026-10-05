"""CPU-only unit tests for NIXL backend selection.

These tests cover the backend-selection and agent-configuration logic in
``ray.experimental.rdt.nixl_tensor_transport`` without requiring a GPU, NIXL, or
EFA hardware. They exercise the hardware-to-backend mapping (host vs. container
EFA layouts and non-EFA RDMA) and the RAY_NIXL_* env var plumbing.
"""

import sys

import pytest

from ray.experimental.rdt import nixl_tensor_transport as ntt
from ray.experimental.rdt.nixl_tensor_transport import (
    NixlTensorTransport,
    _build_nixl_agent_config_kwargs,
    _is_efa_available,
    _nixl_transport_available_in_process,
    _parse_nixl_backend_init_params,
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


def test_parse_nixl_backend_init_params():
    """Init params are parsed from JSON and stringified for NIXL."""
    assert _parse_nixl_backend_init_params("") == {}
    assert _parse_nixl_backend_init_params('{"UCX": {"num_workers": 8}}') == {
        "UCX": {"num_workers": "8"}
    }
    assert _parse_nixl_backend_init_params('{"UCX": null}') == {"UCX": {}}
    for bad in ("not-json", '{"UCX": "num_workers=8"}', "[1, 2]"):
        with pytest.raises(ValueError, match="RAY_NIXL_BACKEND_INIT_PARAMS"):
            _parse_nixl_backend_init_params(bad)


def test_build_nixl_agent_config_kwargs():
    """Config kwargs honor num_threads and drop options unsupported by the
    installed NIXL version instead of failing agent creation."""

    # A config class supporting num_threads (recent NIXL).
    class NewConfig:
        def __init__(self, backends=None, num_threads=0):
            pass

    assert _build_nixl_agent_config_kwargs(NewConfig, ["UCX"], 8, {}) == {
        "backends": ["UCX"],
        "num_threads": 8,
    }
    # num_threads == 0 means using the NIXL default.
    assert _build_nixl_agent_config_kwargs(NewConfig, ["UCX"], 0, {}) == {
        "backends": ["UCX"]
    }
    # Backends with custom init params are created via create_backend instead.
    assert _build_nixl_agent_config_kwargs(
        NewConfig, ["UCX", "GDS"], 0, {"UCX": {"num_workers": "8"}}
    ) == {"backends": ["GDS"]}

    # A config class without num_threads (older NIXL): the option is dropped.
    class OldConfig:
        def __init__(self, backends=None):
            pass

    assert _build_nixl_agent_config_kwargs(OldConfig, ["UCX"], 8, {}) == {
        "backends": ["UCX"]
    }


def test_backends_env_override(monkeypatch):
    """RAY_NIXL_BACKENDS overrides hardware auto-detection; the first backend
    becomes the primary one used for error guidance."""
    monkeypatch.setattr(ntt, "NIXL_AGENT_BACKENDS", "GDS, UCX")
    captured = {}

    def fake_make(self, backends):
        captured["backends"] = backends
        return object()

    monkeypatch.setattr(NixlTensorTransport, "_make_nixl_agent", fake_make)
    transport = NixlTensorTransport()
    transport.get_nixl_agent()
    assert captured["backends"] == ["GDS", "UCX"]
    assert transport._backend == "GDS"


def test_backends_default_to_auto_detection(monkeypatch):
    """When RAY_NIXL_BACKENDS is unset, select_backend() decides."""
    monkeypatch.setattr(ntt, "NIXL_AGENT_BACKENDS", "")
    monkeypatch.setattr(
        NixlTensorTransport, "select_backend", lambda self: "LIBFABRIC"
    )
    captured = {}

    def fake_make(self, backends):
        captured["backends"] = backends
        return object()

    monkeypatch.setattr(NixlTensorTransport, "_make_nixl_agent", fake_make)
    transport = NixlTensorTransport()
    transport.get_nixl_agent()
    assert captured["backends"] == ["LIBFABRIC"]
    assert transport._backend == "LIBFABRIC"


def test_make_nixl_agent_with_backend_init_params(monkeypatch):
    """Backends with init params are excluded from nixl_agent_config and
    created via create_backend() with stringified params."""

    class FakeConfig:
        def __init__(self, backends=None, num_threads=0):
            self.backends = backends
            self.num_threads = num_threads

    class FakeAgent:
        def __init__(self, name, config):
            self.name = name
            self.config = config
            self.created_backends = {}

        def create_backend(self, backend, init_params):
            self.created_backends[backend] = init_params

    fake_api = type(
        "fake_api", (), {"nixl_agent": FakeAgent, "nixl_agent_config": FakeConfig}
    )
    monkeypatch.setitem(sys.modules, "nixl._api", fake_api)
    monkeypatch.setitem(sys.modules, "nixl", type("nixl", (), {"_api": fake_api}))
    monkeypatch.setattr(
        ntt, "NIXL_AGENT_BACKEND_INIT_PARAMS", '{"UCX": {"num_workers": 8}}'
    )
    monkeypatch.setattr(ntt, "NIXL_AGENT_NUM_THREADS", 4)

    agent = NixlTensorTransport()._make_nixl_agent(["UCX", "GDS"])
    assert agent.config.backends == ["GDS"]
    assert agent.config.num_threads == 4
    assert agent.created_backends == {"UCX": {"num_workers": "8"}}


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
