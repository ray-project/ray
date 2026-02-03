import os
import sys

import pytest

from ray._private.accelerators.mblt import (
    NOSET_MBLT_RT_VISIBLE_DEVICES_ENV_VAR,
    MBLT_RT_VISIBLE_DEVICES_ENV_VAR,
    MBLTAcceleratorManager,
)


@pytest.fixture(autouse=True)
def mock_qbruntime_module(monkeypatch):
    from ray.tests.accelerators import mock_qbruntime

    monkeypatch.setitem(sys.modules, "qbruntime", mock_qbruntime)


@pytest.fixture
def clear_mblt_environment():
    original_env = os.environ.get(MBLT_RT_VISIBLE_DEVICES_ENV_VAR)
    original_no_set_env = os.environ.get(NOSET_MBLT_RT_VISIBLE_DEVICES_ENV_VAR)

    os.environ.pop(MBLT_RT_VISIBLE_DEVICES_ENV_VAR, None)
    os.environ.pop(NOSET_MBLT_RT_VISIBLE_DEVICES_ENV_VAR, None)

    yield

    if original_env is not None:
        os.environ[MBLT_RT_VISIBLE_DEVICES_ENV_VAR] = original_env
    if original_no_set_env is not None:
        os.environ[NOSET_MBLT_RT_VISIBLE_DEVICES_ENV_VAR] = original_no_set_env


@pytest.mark.usefixtures("clear_mblt_environment")
class TestMBLTAcceleratorManager:
    def test_get_resource_name(self):
        assert MBLTAcceleratorManager.get_resource_name() == "MBLT"

    def test_get_visible_accelerator_ids_env_var(self):
        assert (
            MBLTAcceleratorManager.get_visible_accelerator_ids_env_var()
            == MBLT_RT_VISIBLE_DEVICES_ENV_VAR
        )

    def test_get_current_process_visible_accelerator_ids(self):
        os.environ[MBLT_RT_VISIBLE_DEVICES_ENV_VAR] = "0,1,2,3"
        assert MBLTAcceleratorManager.get_current_process_visible_accelerator_ids() == [
            "0",
            "1",
            "2",
            "3",
        ]

        os.environ[MBLT_RT_VISIBLE_DEVICES_ENV_VAR] = ""
        assert MBLTAcceleratorManager.get_current_process_visible_accelerator_ids() == []

        os.environ.pop(MBLT_RT_VISIBLE_DEVICES_ENV_VAR)
        assert MBLTAcceleratorManager.get_current_process_visible_accelerator_ids() is None

    def test_get_current_node_num_accelerators(self):
        assert MBLTAcceleratorManager.get_current_node_num_accelerators() == 4

    def test_get_current_node_accelerator_type(self):
        assert MBLTAcceleratorManager.get_current_node_accelerator_type() == "ARIES"

    def test_set_current_process_visible_accelerator_ids(self):
        MBLTAcceleratorManager.set_current_process_visible_accelerator_ids(["0", "1"])
        assert os.environ[MBLT_RT_VISIBLE_DEVICES_ENV_VAR] == "0,1"

        os.environ[NOSET_MBLT_RT_VISIBLE_DEVICES_ENV_VAR] = "1"
        MBLTAcceleratorManager.set_current_process_visible_accelerator_ids(["2", "3"])
        assert os.environ[MBLT_RT_VISIBLE_DEVICES_ENV_VAR] == "0,1"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
