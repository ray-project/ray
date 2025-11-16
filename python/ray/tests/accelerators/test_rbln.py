import os
import sys

import pytest

from ray._private.accelerators.rbln import (
    NOSET_RBLN_RT_VISIBLE_DEVICES_ENV_VAR,
    RBLN_RT_VISIBLE_DEVICES_ENV_VAR,
    RBLNAcceleratorManager,
)


@pytest.fixture(autouse=True)
def mock_rebel_module(monkeypatch):
    from ray.tests.accelerators import mock_rebel

    monkeypatch.setitem(sys.modules, "rebel", mock_rebel)


@pytest.fixture
def clear_rbln_environment():
    original_env = os.environ.get(RBLN_RT_VISIBLE_DEVICES_ENV_VAR)
    original_no_set_env = os.environ.get(NOSET_RBLN_RT_VISIBLE_DEVICES_ENV_VAR)

    os.environ.pop(RBLN_RT_VISIBLE_DEVICES_ENV_VAR, None)
    os.environ.pop(NOSET_RBLN_RT_VISIBLE_DEVICES_ENV_VAR, None)

    yield

    if original_env is not None:
        os.environ[RBLN_RT_VISIBLE_DEVICES_ENV_VAR] = original_env
    if original_no_set_env is not None:
        os.environ[NOSET_RBLN_RT_VISIBLE_DEVICES_ENV_VAR] = original_no_set_env


@pytest.mark.usefixtures("clear_rbln_environment")
class TestRBLNAcceleratorManager:
    def test_get_resource_name(self):
        assert RBLNAcceleratorManager.get_resource_name() == "RBLN"

    def test_get_visible_accelerator_ids_env_var(self):
        assert (
            RBLNAcceleratorManager.get_visible_accelerator_ids_env_var()
            == RBLN_RT_VISIBLE_DEVICES_ENV_VAR
        )

    def test_get_current_process_visible_accelerator_ids(self):
        os.environ[RBLN_RT_VISIBLE_DEVICES_ENV_VAR] = "0,1,2,3"
        assert RBLNAcceleratorManager.get_current_process_visible_accelerator_ids() == [
            "0",
            "1",
            "2",
            "3",
        ]

        os.environ[RBLN_RT_VISIBLE_DEVICES_ENV_VAR] = ""
        assert (
            RBLNAcceleratorManager.get_current_process_visible_accelerator_ids() == []
        )

        os.environ.pop(RBLN_RT_VISIBLE_DEVICES_ENV_VAR)
        assert (
            RBLNAcceleratorManager.get_current_process_visible_accelerator_ids() is None
        )

    def test_get_current_node_num_accelerators(self):
        assert RBLNAcceleratorManager.get_current_node_num_accelerators() == 4

    def test_get_current_node_accelerator_type(self):
        assert RBLNAcceleratorManager.get_current_node_accelerator_type() == "RBLN-CA02"

    def test_set_current_process_visible_accelerator_ids(self):
        RBLNAcceleratorManager.set_current_process_visible_accelerator_ids(["0", "1"])
        assert os.environ[RBLN_RT_VISIBLE_DEVICES_ENV_VAR] == "0,1"

        os.environ[NOSET_RBLN_RT_VISIBLE_DEVICES_ENV_VAR] = "1"
        RBLNAcceleratorManager.set_current_process_visible_accelerator_ids(["2", "3"])
        assert os.environ[RBLN_RT_VISIBLE_DEVICES_ENV_VAR] == "0,1"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
