import os
import sys
import pytest
from unittest.mock import patch

import ray
from ray._private.accelerators import NPUAcceleratorManager as Accelerator


@patch("glob.glob")
def test_autodetect_num_npus(mock_glob):
    with patch.dict(sys.modules):
        sys.modules["acl"] = None
        mock_glob.return_value = [f"/dev/davinci{i}" for i in range(4)]
        assert Accelerator.get_current_node_num_accelerators() == 4


@patch("glob.glob")
def test_autodetect_num_npus_without_devices(mock_glob):
    with patch.dict(sys.modules):
        sys.modules["acl"] = None
        mock_glob.side_effect = Exception
        assert Accelerator.get_current_node_num_accelerators() == 0


def test_ascend_npu_accelerator_manager_api():
    assert Accelerator.get_resource_name() == "NPU"
    assert (
        Accelerator.get_visible_accelerator_ids_env_var() == "ASCEND_RT_VISIBLE_DEVICES"
    )
    assert Accelerator.validate_resource_request_quantity(0.5) == (True, None)
    assert Accelerator.validate_resource_request_quantity(1) == (True, None)


def test_visible_ascend_npu_type(monkeypatch, shutdown_only):
    with patch.object(
        Accelerator, "get_current_node_num_accelerators", return_value=4
    ), patch.object(
        Accelerator, "get_current_node_accelerator_type", return_value="Ascend910B"
    ):
        monkeypatch.setenv("ASCEND_RT_VISIBLE_DEVICES", "0,1,2")
        manager = ray._private.accelerators.get_accelerator_manager_for_resource("NPU")
        assert manager.get_current_node_accelerator_type() == "Ascend910B"


@pytest.mark.skipif(sys.platform == "win32", reason="Not supported mock on Windows")
@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Not passing on Python 3.12. Being followed up by external contributors.",
)
def test_visible_ascend_npu_ids(monkeypatch, shutdown_only):
    with patch.dict(sys.modules):
        sys.modules["acl"] = __import__("mock_acl")

        monkeypatch.setenv("ASCEND_RT_VISIBLE_DEVICES", "0,1,2")
        with patch.object(
            Accelerator, "get_current_node_num_accelerators", return_value=4
        ):

            ray.init()
            manager = ray._private.accelerators.get_accelerator_manager_for_resource(
                "NPU"
            )
            assert manager.get_current_node_num_accelerators() == 4
            assert manager.__name__ == "NPUAcceleratorManager"
            assert ray.available_resources()["NPU"] == 3


@pytest.mark.skipif(sys.platform == "win32", reason="Not supported mock on Windows")
@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Not passing on Python 3.12. Being followed up by external contributors.",
)
def test_acl_api_function(shutdown_only):
    with patch.dict(sys.modules):
        sys.modules["acl"] = __import__("mock_acl")

        ray.init()
        manager = ray._private.accelerators.get_accelerator_manager_for_resource("NPU")
        assert manager.get_current_node_num_accelerators() == 4
        assert manager.__name__ == "NPUAcceleratorManager"
        assert manager.get_current_node_accelerator_type() == "Ascend910B"


def test_get_current_process_visible_accelerator_ids(monkeypatch, shutdown_only):
    monkeypatch.setenv("ASCEND_RT_VISIBLE_DEVICES", "0,1,2")
    assert Accelerator.get_current_process_visible_accelerator_ids() == ["0", "1", "2"]

    monkeypatch.delenv("ASCEND_RT_VISIBLE_DEVICES")
    assert Accelerator.get_current_process_visible_accelerator_ids() is None

    monkeypatch.setenv("ASCEND_RT_VISIBLE_DEVICES", "")
    assert Accelerator.get_current_process_visible_accelerator_ids() == []

    monkeypatch.setenv("ASCEND_RT_VISIBLE_DEVICES", "NoDevFiles")
    assert Accelerator.get_current_process_visible_accelerator_ids() == []


def test_set_current_process_visible_accelerator_ids(shutdown_only):
    Accelerator.set_current_process_visible_accelerator_ids(["0"])
    assert os.environ["ASCEND_RT_VISIBLE_DEVICES"] == "0"

    Accelerator.set_current_process_visible_accelerator_ids(["0", "1"])
    assert os.environ["ASCEND_RT_VISIBLE_DEVICES"] == "0,1"

    Accelerator.set_current_process_visible_accelerator_ids(["0", "1", "2"])
    assert os.environ["ASCEND_RT_VISIBLE_DEVICES"] == "0,1,2"


@pytest.mark.skipif(sys.platform == "win32", reason="Not supported mock on Windows")
@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Not passing on Python 3.12. Being followed up by external contributors.",
)
def test_auto_detected_more_than_visible(monkeypatch, shutdown_only):
    with patch.dict(sys.modules):
        sys.modules["acl"] = __import__("mock_acl")

        with patch.object(
            Accelerator, "get_current_node_num_accelerators", return_value=4
        ):
            # If more NPUs are detected than visible.
            monkeypatch.setenv("ASCEND_RT_VISIBLE_DEVICES", "0,1,2")

            ray.init()
            assert ray.available_resources()["NPU"] == 3


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
