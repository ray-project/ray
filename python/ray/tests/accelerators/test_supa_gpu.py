import os
import sys
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray._private.accelerators import SupaGPUAcceleratorManager as Accelerator


@patch("glob.glob")
def test_autodetect_num_supa(mock_glob):
    """Detect Biren cards via /dev/biren/card_* device nodes."""
    mock_glob.return_value = [f"/dev/biren/card_{i}" for i in range(4)]
    assert Accelerator.get_current_node_num_accelerators() == 4

    mock_glob.return_value = [f"/dev/biren/card_{i}" for i in range(8)]
    assert Accelerator.get_current_node_num_accelerators() == 8


@patch("glob.glob")
def test_autodetect_num_supa_without_devices(mock_glob):
    """No device nodes or glob exception -> 0 accelerators (no crash)."""
    mock_glob.return_value = []
    assert Accelerator.get_current_node_num_accelerators() == 0

    mock_glob.side_effect = Exception("test exception")
    assert Accelerator.get_current_node_num_accelerators() == 0


def test_supa_accelerator_manager_api():
    """Resource name, env var, and fractional-quantity contract."""
    assert Accelerator.get_resource_name() == "GPU"
    assert (
        Accelerator.get_visible_accelerator_ids_env_var() == "SUPA_VISIBLE_DEVICES"
    )
    assert Accelerator.validate_resource_request_quantity(0.5) == (True, None)
    assert Accelerator.validate_resource_request_quantity(1) == (True, None)


def test_get_current_node_accelerator_type_no_torch(monkeypatch):
    """No torch.supa -> None (no crash)."""
    with patch.dict(sys.modules):
        sys.modules["torch"] = MagicMock(spec=[])  # torch without supa attr
        assert Accelerator.get_current_node_accelerator_type() is None


def test_get_current_process_visible_accelerator_ids(monkeypatch):
    """Parse SUPA_VISIBLE_DEVICES: None / [] / list semantics."""
    monkeypatch.setenv("SUPA_VISIBLE_DEVICES", "0,1,2")
    assert Accelerator.get_current_process_visible_accelerator_ids() == ["0", "1", "2"]

    monkeypatch.delenv("SUPA_VISIBLE_DEVICES")
    assert Accelerator.get_current_process_visible_accelerator_ids() is None

    monkeypatch.setenv("SUPA_VISIBLE_DEVICES", "")
    assert Accelerator.get_current_process_visible_accelerator_ids() == []

    monkeypatch.setenv("SUPA_VISIBLE_DEVICES", "NoDevFiles")
    assert Accelerator.get_current_process_visible_accelerator_ids() == []


def test_set_current_process_visible_accelerator_ids(monkeypatch):
    """Writes SUPA_VISIBLE_DEVICES as comma-joined string."""
    monkeypatch.delenv("SUPA_VISIBLE_DEVICES", raising=False)
    monkeypatch.delenv("RAY_EXPERIMENTAL_NOSET_SUPA_VISIBLE_DEVICES", raising=False)

    Accelerator.set_current_process_visible_accelerator_ids(["0"])
    assert os.environ["SUPA_VISIBLE_DEVICES"] == "0"

    Accelerator.set_current_process_visible_accelerator_ids(["0", "1"])
    assert os.environ["SUPA_VISIBLE_DEVICES"] == "0,1"

    Accelerator.set_current_process_visible_accelerator_ids(["0", "1", "2"])
    assert os.environ["SUPA_VISIBLE_DEVICES"] == "0,1,2"


def test_set_current_process_visible_accelerator_ids_respects_noset(monkeypatch):
    """RAY_EXPERIMENTAL_NOSET_SUPA_VISIBLE_DEVICES=1 disables writing."""
    monkeypatch.delenv("SUPA_VISIBLE_DEVICES", raising=False)
    monkeypatch.setenv("RAY_EXPERIMENTAL_NOSET_SUPA_VISIBLE_DEVICES", "1")
    Accelerator.set_current_process_visible_accelerator_ids(["1", "3"])
    assert "SUPA_VISIBLE_DEVICES" not in os.environ


@pytest.mark.skipif(sys.platform == "win32", reason="Not supported mock on Windows")
def test_visible_supa_type(monkeypatch, shutdown_only):
    """Registry lookup returns SupaGPUAcceleratorManager for 'GPU' resource."""
    with patch.object(
        Accelerator, "get_current_node_num_accelerators", return_value=4
    ), patch.object(
        Accelerator, "get_current_node_accelerator_type", return_value="BR104"
    ):
        from ray._private.accelerators import get_accelerator_manager_for_resource
        if hasattr(
            get_accelerator_manager_for_resource,
            "_resource_name_to_accelerator_manager",
        ):
            del get_accelerator_manager_for_resource._resource_name_to_accelerator_manager
        manager = get_accelerator_manager_for_resource("GPU")
        assert manager is Accelerator
        assert manager.get_current_node_accelerator_type() == "BR104"


@pytest.mark.skipif(sys.platform == "win32", reason="Not supported mock on Windows")
def test_visible_supa_ids(monkeypatch, shutdown_only):
    """SUPA_VISIBLE_DEVICES limits available_resources['GPU']."""
    monkeypatch.setenv("SUPA_VISIBLE_DEVICES", "0,1,2")
    with patch.object(
        Accelerator, "get_current_node_num_accelerators", return_value=4
    ):
        from ray._private.accelerators import get_accelerator_manager_for_resource
        if hasattr(
            get_accelerator_manager_for_resource,
            "_resource_name_to_accelerator_manager",
        ):
            del get_accelerator_manager_for_resource._resource_name_to_accelerator_manager

        ray.init()
        assert ray.available_resources()["GPU"] == 3


@pytest.mark.skipif(sys.platform == "win32", reason="Not supported mock on Windows")
def test_auto_detected_more_than_visible(monkeypatch, shutdown_only):
    """Auto-detected count > env-var-visible count: ray uses the smaller one."""
    monkeypatch.setenv("SUPA_VISIBLE_DEVICES", "0,1,2")
    with patch.object(
        Accelerator, "get_current_node_num_accelerators", return_value=8
    ):
        from ray._private.accelerators import get_accelerator_manager_for_resource
        if hasattr(
            get_accelerator_manager_for_resource,
            "_resource_name_to_accelerator_manager",
        ):
            del get_accelerator_manager_for_resource._resource_name_to_accelerator_manager

        ray.init()
        assert ray.available_resources()["GPU"] == 3


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
