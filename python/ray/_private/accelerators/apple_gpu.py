import logging
import os
import platform
import subprocess
from typing import Dict, List, Optional, Tuple

from ray._private.accelerators.accelerator import AcceleratorManager

logger = logging.getLogger(__name__)

# Environment variable for controlling MPS usage
MPS_ENABLE_ENV_VAR = "PYTORCH_ENABLE_MPS_FALLBACK"
NOSET_MPS_ENV_VAR = "RAY_EXPERIMENTAL_NOSET_MPS_DEVICE"


class AppleGPUAcceleratorManager(AcceleratorManager):
    """Apple Silicon GPU (MPS) accelerators manager."""

    @staticmethod
    def get_resource_name() -> str:
        return "GPU"

    @staticmethod
    def get_visible_accelerator_ids_env_var() -> str:
        # For MPS, we use PyTorch's MPS fallback environment variable
        return MPS_ENABLE_ENV_VAR

    @staticmethod
    def get_current_node_num_accelerators() -> int:
        """Detect if MPS is available on Apple Silicon."""
        try:
            # Check if we're on macOS with Apple Silicon
            if not AppleGPUAcceleratorManager._is_apple_silicon():
                return 0

            # Check if PyTorch with MPS support is available
            if not AppleGPUAcceleratorManager._is_mps_available():
                return 0

            # Apple Silicon has one unified GPU
            return 1
        except Exception as e:
            logger.debug(f"Error detecting Apple Silicon GPU: {e}")
            return 0

    @staticmethod
    def get_current_node_accelerator_type() -> Optional[str]:
        """Get the Apple Silicon chip type (M1, M2, M3, etc.)."""
        try:
            if not AppleGPUAcceleratorManager._is_apple_silicon():
                return None

            return AppleGPUAcceleratorManager._get_apple_chip_type()
        except Exception as e:
            logger.debug(f"Error getting Apple Silicon chip type: {e}")
            return None

    @staticmethod
    def get_current_node_additional_resources() -> Optional[Dict[str, float]]:
        """No additional resources required for Apple Silicon GPUs."""
        return None

    @staticmethod
    def validate_resource_request_quantity(
        quantity: float,
    ) -> Tuple[bool, Optional[str]]:
        """Validate GPU resource request quantity."""
        if quantity < 0:
            return False, "GPU resource quantity cannot be negative."

        # For Apple Silicon, we typically have one unified GPU
        # But allow fractional usage for sharing
        if quantity > 1:
            return False, "Apple Silicon GPU resource quantity cannot exceed 1.0."

        return True, None

    @staticmethod
    def get_current_process_visible_accelerator_ids() -> Optional[List[str]]:
        """Get visible MPS device IDs (always ['0'] if MPS is enabled)."""
        try:
            if AppleGPUAcceleratorManager._is_mps_available():
                # Check if MPS is disabled via environment variable
                mps_fallback = os.environ.get(MPS_ENABLE_ENV_VAR, "1")
                if mps_fallback == "0":
                    return []
                return ["0"]
            return None
        except Exception:
            return None

    @staticmethod
    def set_current_process_visible_accelerator_ids(ids: List[str]) -> None:
        """Set visible MPS devices by controlling the MPS fallback environment variable."""
        if os.environ.get(NOSET_MPS_ENV_VAR):
            return

        # For MPS, we control visibility through the PyTorch MPS fallback
        if not ids or ids == []:
            # Disable MPS
            os.environ[MPS_ENABLE_ENV_VAR] = "0"
        else:
            # Enable MPS (Apple Silicon only has one GPU, so any non-empty list enables it)
            os.environ[MPS_ENABLE_ENV_VAR] = "1"

    @staticmethod
    def get_ec2_instance_num_accelerators(
        instance_type: str, instances: dict
    ) -> Optional[int]:
        """Apple Silicon GPUs are not available on EC2."""
        return None

    @staticmethod
    def get_ec2_instance_accelerator_type(
        instance_type: str, instances: dict
    ) -> Optional[str]:
        """Apple Silicon GPUs are not available on EC2."""
        return None

    @staticmethod
    def _is_apple_silicon() -> bool:
        """Check if running on Apple Silicon (ARM64 macOS)."""
        try:
            return platform.system() == "Darwin" and platform.machine() == "arm64"
        except Exception:
            return False

    @staticmethod
    def _is_mps_available() -> bool:
        """Check if PyTorch MPS backend is available."""
        try:
            import torch

            return torch.mps.is_available()
        except (ImportError, AttributeError):
            # torch not available or MPS not supported
            return False

    @staticmethod
    def _get_apple_chip_type() -> Optional[str]:
        """Get the specific Apple Silicon chip type."""
        try:
            # Use system_profiler to get chip information
            result = subprocess.run(
                ["system_profiler", "SPHardwareDataType"],
                capture_output=True,
                text=True,
                timeout=5,
            )

            if result.returncode == 0:
                output = result.stdout
                # Look for chip name in the output
                for line in output.split("\n"):
                    line = line.strip()
                    if "Chip:" in line:
                        # Extract chip name (e.g., "Apple M1 Pro", "Apple M2", etc.)
                        chip_info = line.split("Chip:")[1].strip()
                        if "Apple" in chip_info:
                            # Extract the M-series designation
                            parts = chip_info.split()
                            for i, part in enumerate(parts):
                                if part.startswith("M") and len(part) >= 2:
                                    # Include any suffix (Pro, Max, Ultra)
                                    chip_parts = [part]
                                    if i + 1 < len(parts) and parts[i + 1] in [
                                        "Pro",
                                        "Max",
                                        "Ultra",
                                    ]:
                                        chip_parts.append(parts[i + 1])
                                    return "-".join(chip_parts)

            # Fallback: try using sysctl
            result = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True,
                text=True,
                timeout=5,
            )

            if result.returncode == 0:
                brand_string = result.stdout.strip()
                if "Apple" in brand_string:
                    # Try to extract M-series chip from brand string
                    parts = brand_string.split()
                    for i, part in enumerate(parts):
                        if part.startswith("M") and len(part) >= 2:
                            chip_parts = [part]
                            if i + 1 < len(parts) and parts[i + 1] in [
                                "Pro",
                                "Max",
                                "Ultra",
                            ]:
                                chip_parts.append(parts[i + 1])
                            return "-".join(chip_parts)

            # If we can't determine the specific chip, return generic Apple Silicon
            return "Apple-Silicon"

        except Exception as e:
            logger.debug(f"Error detecting Apple chip type: {e}")
            return "Apple-Silicon"
