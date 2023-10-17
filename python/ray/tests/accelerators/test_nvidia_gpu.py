import os
import sys
import pytest
from unittest.mock import patch, mock_open

from ray._private.accelerators import NvidiaGPUAcceleratorManager


@patch("importlib.util.find_spec", return_value=False)
@patch("os.path.isdir", return_value=True)
@patch("os.listdir", return_value=["1"])
@patch("sys.platform", "linux")
def test_gpu_info_parsing(mock_listdir, mock_isdir, mock_find_spec):
    info_string = """Model:           Tesla V100-SXM2-16GB
IRQ:             107
GPU UUID:        GPU-8eaaebb8-bb64-8489-fda2-62256e821983
Video BIOS:      88.00.4f.00.09
Bus Type:        PCIe
DMA Size:        47 bits
DMA Mask:        0x7fffffffffff
Bus Location:    0000:00:1e.0
Device Minor:    0
Blacklisted:     No
    """
    with patch("builtins.open", mock_open(read_data=info_string)):
        assert NvidiaGPUAcceleratorManager.get_current_node_accelerator_type() == "V100"

    info_string = """Model:           Tesla T4
IRQ:             10
GPU UUID:        GPU-415fe7a8-f784-6e3d-a958-92ecffacafe2
Video BIOS:      90.04.84.00.06
Bus Type:        PCIe
DMA Size:        47 bits
DMA Mask:        0x7fffffffffff
Bus Location:    0000:00:1b.0
Device Minor:    0
Blacklisted:     No
    """
    with patch("builtins.open", mock_open(read_data=info_string)):
        assert NvidiaGPUAcceleratorManager.get_current_node_accelerator_type() == "T4"


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
