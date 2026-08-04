import os
import sys
from unittest.mock import MagicMock, patch

import pytest

from ray._private.accelerators.apple_gpu import AppleGPUAcceleratorManager


class TestAppleGPUAcceleratorManager:
    """Test suite for Apple GPU (MPS) accelerator manager."""

    def test_get_resource_name(self):
        """Test that the resource name is 'GPU'."""
        assert AppleGPUAcceleratorManager.get_resource_name() == "GPU"

    def test_get_visible_accelerator_ids_env_var(self):
        """Apple Silicon has no visible-devices env var (single unified GPU)."""
        assert AppleGPUAcceleratorManager.get_visible_accelerator_ids_env_var() is None

    def test_get_current_node_additional_resources(self):
        """Test that no additional resources are required."""
        assert (
            AppleGPUAcceleratorManager.get_current_node_additional_resources() is None
        )

    def test_validate_resource_request_quantity_accepts_any(self):
        """Like other GPU managers, any quantity is accepted (scheduler enforces
        capacity). In particular, quantity > 1 must not be rejected, since the manager
        is chosen by the driver node and may front multi-GPU NVIDIA workers."""
        for quantity in (0.0, 0.5, 1.0, 2.0, 8.0):
            assert AppleGPUAcceleratorManager.validate_resource_request_quantity(
                quantity
            ) == (True, None)

    def test_ec2_instance_methods(self):
        """Test that EC2 methods return None (Apple Silicon not available on EC2)."""
        assert (
            AppleGPUAcceleratorManager.get_ec2_instance_num_accelerators("m5.large", {})
            is None
        )
        assert (
            AppleGPUAcceleratorManager.get_ec2_instance_accelerator_type("m5.large", {})
            is None
        )

    @patch("platform.system")
    @patch("platform.machine")
    def test_is_apple_silicon_detection(self, mock_machine, mock_system):
        """Test Apple Silicon platform detection."""
        mock_system.return_value = "Darwin"
        mock_machine.return_value = "arm64"
        assert AppleGPUAcceleratorManager._is_apple_silicon() is True

        mock_system.return_value = "Linux"
        assert AppleGPUAcceleratorManager._is_apple_silicon() is False

        mock_system.return_value = "Darwin"
        mock_machine.return_value = "x86_64"
        assert AppleGPUAcceleratorManager._is_apple_silicon() is False

    @patch(
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._is_apple_silicon"
    )
    @patch(
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._is_metal_gpu_available"
    )
    def test_get_current_node_num_accelerators(
        self, mock_is_metal_gpu_available, mock_is_apple_silicon
    ):
        """Accelerator count is detected from hardware, not from PyTorch."""
        # Not Apple Silicon
        mock_is_apple_silicon.return_value = False
        assert AppleGPUAcceleratorManager.get_current_node_num_accelerators() == 0

        # Apple Silicon but system_profiler reports no Metal GPU
        mock_is_apple_silicon.return_value = True
        mock_is_metal_gpu_available.return_value = False
        assert AppleGPUAcceleratorManager.get_current_node_num_accelerators() == 0

        # Apple Silicon with a Metal GPU (no PyTorch involved)
        mock_is_apple_silicon.return_value = True
        mock_is_metal_gpu_available.return_value = True
        assert AppleGPUAcceleratorManager.get_current_node_num_accelerators() == 1

    @patch(
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._is_apple_silicon"
    )
    def test_get_current_node_accelerator_type_not_apple_silicon(
        self, mock_is_apple_silicon
    ):
        """Test that None is returned for accelerator type on non-Apple Silicon."""
        mock_is_apple_silicon.return_value = False

        assert AppleGPUAcceleratorManager.get_current_node_accelerator_type() is None

    @patch(
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._is_apple_silicon"
    )
    @patch(
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._get_apple_chip_type"
    )
    def test_get_current_node_accelerator_type_apple_silicon(
        self, mock_get_chip_type, mock_is_apple_silicon
    ):
        """Test that chip type is returned on Apple Silicon."""
        mock_is_apple_silicon.return_value = True
        mock_get_chip_type.return_value = "M2-Pro"

        assert (
            AppleGPUAcceleratorManager.get_current_node_accelerator_type() == "M2-Pro"
        )

    @patch(
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._is_apple_silicon"
    )
    def test_get_current_process_visible_accelerator_ids_apple_silicon(
        self, mock_is_apple_silicon
    ):
        """The only visible id on Apple Silicon is ever "0"."""
        mock_is_apple_silicon.return_value = True

        assert (
            AppleGPUAcceleratorManager.get_current_process_visible_accelerator_ids()
            == ["0"]
        )

    @patch(
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._is_apple_silicon"
    )
    def test_get_current_process_visible_accelerator_ids_not_apple_silicon(
        self, mock_is_apple_silicon
    ):
        """Non-Apple-Silicon hosts report no visible Apple GPU."""
        mock_is_apple_silicon.return_value = False

        assert (
            AppleGPUAcceleratorManager.get_current_process_visible_accelerator_ids()
            is None
        )

    def test_set_current_process_visible_accelerator_ids_is_noop(self):
        """Apple Silicon has no visible-devices env var, so set is a no-op."""
        with patch.dict(os.environ, {}, clear=True):
            AppleGPUAcceleratorManager.set_current_process_visible_accelerator_ids(
                ["0"]
            )
            assert os.environ == {}

    @patch("subprocess.run")
    def test_is_metal_gpu_available(self, mock_run):
        """Metal GPU detection via system_profiler (no framework dependency)."""
        # A Metal-capable GPU is reported.
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = (
            "Graphics/Displays:\n    Apple M3 Pro:\n"
            "      Type: GPU\n      Metal Support: Metal 3\n"
        )
        mock_run.return_value = mock_result
        assert AppleGPUAcceleratorManager._is_metal_gpu_available() is True

        # No Metal GPU in the output.
        mock_result.stdout = "Graphics/Displays:\n"
        assert AppleGPUAcceleratorManager._is_metal_gpu_available() is False

        # system_profiler failing must not raise.
        mock_run.side_effect = Exception("Command failed")
        assert AppleGPUAcceleratorManager._is_metal_gpu_available() is False

    @patch("subprocess.run")
    def test_get_apple_chip_type(self, mock_run):
        """Test Apple chip type detection."""
        # Test M1 Pro detection
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "Chip: Apple M1 Pro\n"
        mock_run.return_value = mock_result

        assert AppleGPUAcceleratorManager._get_apple_chip_type() == "M1-Pro"

        # Test fallback to generic
        mock_run.side_effect = Exception("Command failed")
        assert AppleGPUAcceleratorManager._get_apple_chip_type() == "Apple-Silicon"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
