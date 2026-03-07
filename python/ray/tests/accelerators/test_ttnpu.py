import os
import sys

import pytest

from ray._private.accelerators.ttnpu import (
    NOSET_TENSTORRENT_VISIBLE_DEVICES_ENV_VAR,
    TENSTORRENT_VISIBLE_DEVICES_ENV_VAR,
    TTNPUAcceleratorManager,
)




@pytest.fixture
def clear_ttnpu_environment():
    original_env = os.environ.get(TENSTORRENT_VISIBLE_DEVICES_ENV_VAR)
    original_no_set_env = os.environ.get(NOSET_TENSTORRENT_VISIBLE_DEVICES_ENV_VAR)

    os.environ.pop(TENSTORRENT_VISIBLE_DEVICES_ENV_VAR, None)
    os.environ.pop(NOSET_TENSTORRENT_VISIBLE_DEVICES_ENV_VAR, None)

    yield

    if original_env is not None:
        os.environ[TENSTORRENT_VISIBLE_DEVICES_ENV_VAR] = original_env
    if original_no_set_env is not None:
        os.environ[NOSET_TENSTORRENT_VISIBLE_DEVICES_ENV_VAR] = original_no_set_env


@pytest.mark.usefixtures("clear_ttnpu_environment")
class TestTTNPUAcceleratorManager:
    def test_get_resource_name(self):
        assert TTNPUAcceleratorManager.get_resource_name() == "TTNPU"

    def test_get_visible_accelerator_ids_env_var(self):
        assert (
            TTNPUAcceleratorManager.get_visible_accelerator_ids_env_var()
            == TENSTORRENT_VISIBLE_DEVICES_ENV_VAR
        )

    def test_get_current_process_visible_accelerator_ids(self):
        os.environ[TENSTORRENT_VISIBLE_DEVICES_ENV_VAR] = "0,1,2,3"
        assert TTNPUAcceleratorManager.get_current_process_visible_accelerator_ids() == [
            "0",
            "1",
            "2",
            "3",
        ]

        os.environ[TENSTORRENT_VISIBLE_DEVICES_ENV_VAR] = ""
        assert (
            TTNPUAcceleratorManager.get_current_process_visible_accelerator_ids() == []
        )
        os.environ[TENSTORRENT_VISIBLE_DEVICES_ENV_VAR] = "NoDevFiles"
        assert (
            TTNPUAcceleratorManager.get_current_process_visible_accelerator_ids() == []
        )

        os.environ.pop(TENSTORRENT_VISIBLE_DEVICES_ENV_VAR)
        assert (
            TTNPUAcceleratorManager.get_current_process_visible_accelerator_ids() is None
        )

    def test_get_current_node_num_accelerators(self, monkeypatch):
        monkeypatch.setattr(
            "ray._private.accelerators.ttnpu.glob.glob",
            lambda path: [f"/dev/tenstorrent/{i}" for i in range(4)],
        )
        assert TTNPUAcceleratorManager.get_current_node_num_accelerators() == 4
        
        monkeypatch.setattr("ray._private.accelerators.ttnpu.glob.glob", lambda path: [])
        assert TTNPUAcceleratorManager.get_current_node_num_accelerators() == 0
        
        def mock_glob_exception(path):
            raise OSError("test exception")
        
        monkeypatch.setattr(
            "ray._private.accelerators.ttnpu.glob.glob", mock_glob_exception
        )
        assert TTNPUAcceleratorManager.get_current_node_num_accelerators() == 0

    def test_set_current_process_visible_accelerator_ids(self):
        TTNPUAcceleratorManager.set_current_process_visible_accelerator_ids(["0", "1"])
        assert os.environ[TENSTORRENT_VISIBLE_DEVICES_ENV_VAR] == "0,1"

        os.environ[NOSET_TENSTORRENT_VISIBLE_DEVICES_ENV_VAR] = "1"
        TTNPUAcceleratorManager.set_current_process_visible_accelerator_ids(["2", "3"])
        assert os.environ[TENSTORRENT_VISIBLE_DEVICES_ENV_VAR] == "0,1"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
