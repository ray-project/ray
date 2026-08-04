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

    def test_validate_resource_request_quantity_valid(self):
        """Test validation of valid resource quantities."""
        assert AppleGPUAcceleratorManager.validate_resource_request_quantity(0.0) == (
            True,
            None,
        )
        assert AppleGPUAcceleratorManager.validate_resource_request_quantity(0.5) == (
            True,
            None,
        )
        assert AppleGPUAcceleratorManager.validate_resource_request_quantity(1.0) == (
            True,
            None,
        )

    def test_validate_resource_request_quantity_invalid(self):
        """Test validation of invalid resource quantities."""
        valid, error = AppleGPUAcceleratorManager.validate_resource_request_quantity(
            -1.0
        )
        assert not valid
        assert "cannot be negative" in error

        valid, error = AppleGPUAcceleratorManager.validate_resource_request_quantity(
            2.0
        )
        assert not valid
        assert "cannot exceed 1.0" in error

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
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._is_mps_available"
    )
    def test_get_current_node_num_accelerators(
        self, mock_is_mps_available, mock_is_apple_silicon
    ):
        """Test accelerator count detection."""
        # Not Apple Silicon
        mock_is_apple_silicon.return_value = False
        assert AppleGPUAcceleratorManager.get_current_node_num_accelerators() == 0

        # Apple Silicon but no MPS
        mock_is_apple_silicon.return_value = True
        mock_is_mps_available.return_value = False
        assert AppleGPUAcceleratorManager.get_current_node_num_accelerators() == 0

        # Apple Silicon with MPS
        mock_is_apple_silicon.return_value = True
        mock_is_mps_available.return_value = True
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
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._is_mps_available"
    )
    def test_get_current_process_visible_accelerator_ids_mps_available(
        self, mock_is_mps_available
    ):
        """The only visible id on Apple Silicon is ever "0" when MPS is available."""
        mock_is_mps_available.return_value = True

        assert (
            AppleGPUAcceleratorManager.get_current_process_visible_accelerator_ids()
            == ["0"]
        )

    @patch(
        "ray._private.accelerators.apple_gpu.AppleGPUAcceleratorManager._is_mps_available"
    )
    def test_get_current_process_visible_accelerator_ids_mps_not_available(
        self, mock_is_mps_available
    ):
        """Test getting visible accelerator IDs when MPS is not available."""
        mock_is_mps_available.return_value = False

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

    def test_is_mps_available(self):
        """Test MPS availability detection."""
        # Mock torch with MPS available
        mock_torch = MagicMock()
        mock_torch.mps.is_available.return_value = True

        with patch.dict("sys.modules", {"torch": mock_torch}):
            assert AppleGPUAcceleratorManager._is_mps_available() is True

        # Test without torch
        with patch.dict("sys.modules", {"torch": None}):
            assert AppleGPUAcceleratorManager._is_mps_available() is False

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
