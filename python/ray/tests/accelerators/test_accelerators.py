import sys

import pytest

from ray.util import accelerators
from ray.util.annotations import RayDeprecationWarning


def test_basic_accelerators():
    """Test basic accelerator constants and warnings."""
    with pytest.warns(
        UserWarning,
        match="NVIDIA_A100 can be replaced with NVIDIA_A100_40G or NVIDIA_A100_80G for more precise accelerator specification.",
    ):
        assert accelerators.NVIDIA_A100 == "A100"

    with pytest.warns(
        RayDeprecationWarning,
        match="NVIDIA_TESLA_A100 is deprecated, use NVIDIA_A100 instead.",
    ):
        assert accelerators.NVIDIA_TESLA_A100 == "A100"

    with pytest.warns(RayDeprecationWarning):
        assert accelerators.AMD_INSTINCT_MI100 == "AMD-Instinct-MI100"

    with pytest.raises(
        AttributeError,
        match="module 'ray.util.accelerators' has no attribute 'NVIDIA_INVALID'",
    ):
        _ = accelerators.NVIDIA_INVALID


def test_nvidia_accelerators():
    """Test all NVIDIA accelerator constants."""
    assert accelerators.NVIDIA_TESLA_V100 == "V100"
    assert accelerators.NVIDIA_TESLA_P100 == "P100"
    assert accelerators.NVIDIA_TESLA_T4 == "T4"
    assert accelerators.NVIDIA_TESLA_P4 == "P4"
    assert accelerators.NVIDIA_TESLA_K80 == "K80"
    assert accelerators.NVIDIA_TESLA_A10G == "A10G"
    assert accelerators.NVIDIA_L4 == "L4"
    assert accelerators.NVIDIA_L40S == "L40S"
    assert accelerators.NVIDIA_A100 == "A100"
    assert accelerators.NVIDIA_H100 == "H100"
    assert accelerators.NVIDIA_H200 == "H200"
    assert accelerators.NVIDIA_H20 == "H20"
    assert accelerators.NVIDIA_B200 == "B200"
    assert accelerators.NVIDIA_A100_40G == "A100-40G"
    assert accelerators.NVIDIA_A100_80G == "A100-80G"


def test_intel_accelerators():
    """Test all Intel accelerator constants."""
    assert accelerators.INTEL_MAX_1550 == "Intel-GPU-Max-1550"
    assert accelerators.INTEL_MAX_1100 == "Intel-GPU-Max-1100"
    assert accelerators.INTEL_GAUDI == "Intel-GAUDI"


def test_amd_accelerators():
    """Test all AMD accelerator constants."""
    assert accelerators.AMD_INSTINCT_MI100 == "AMD-Instinct-MI100"
    assert accelerators.AMD_INSTINCT_MI250x == "AMD-Instinct-MI250X"
    assert accelerators.AMD_INSTINCT_MI250 == "AMD-Instinct-MI250X-MI250"
    assert accelerators.AMD_INSTINCT_MI210 == "AMD-Instinct-MI210"
    assert accelerators.AMD_INSTINCT_MI300A == "AMD-Instinct-MI300A"
    assert accelerators.AMD_INSTINCT_MI300x == "AMD-Instinct-MI300X-OAM"
    assert accelerators.AMD_INSTINCT_MI300x_HF == "AMD-Instinct-MI300X-HF"
    assert accelerators.AMD_INSTINCT_MI308x == "AMD-Instinct-MI308X"
    assert accelerators.AMD_INSTINCT_MI325x == "AMD-Instinct-MI325X-OAM"
    assert accelerators.AMD_RADEON_R9_200_HD_7900 == "AMD-Radeon-R9-200-HD-7900"
    assert accelerators.AMD_RADEON_HD_7900 == "AMD-Radeon-HD-7900"


def test_google_accelerators():
    """Test all Google accelerator constants."""
    assert accelerators.GOOGLE_TPU_V2 == "TPU-V2"
    assert accelerators.GOOGLE_TPU_V3 == "TPU-V3"
    assert accelerators.GOOGLE_TPU_V4 == "TPU-V4"
    assert accelerators.GOOGLE_TPU_V5P == "TPU-V5P"
    assert accelerators.GOOGLE_TPU_V5LITEPOD == "TPU-V5LITEPOD"
    assert accelerators.GOOGLE_TPU_V6E == "TPU-V6E"


def test_huawei_accelerators():
    """Test all Huawei accelerator constants."""
    assert accelerators.HUAWEI_NPU_910B == "Ascend910B"
    assert accelerators.HUAWEI_NPU_910B4 == "Ascend910B4"


def test_aws_accelerators():
    """Test all AWS accelerator constants."""
    assert accelerators.AWS_NEURON_CORE == "aws-neuron-core"


def test_get_accelerator_type():
    """Test accessing accelerator types in different ways."""
    from ray.util.accelerators.types import all_types, vendor_types

    target_accelerator = "NVIDIA_A100"
    target_vendor = "NVIDIA"

    all_types_result = all_types()
    assert isinstance(all_types_result, str), "all_types() should return a string"
    assert (
        target_accelerator in all_types_result
    ), f"all_types() should contain {target_accelerator}"

    vendor_result = vendor_types(target_vendor)
    assert isinstance(vendor_result, str), "vendor_types() should return a string"
    assert (
        target_accelerator in vendor_result
    ), f"vendor_types('{target_vendor}') should contain {target_accelerator}"

    other_vendor = "AMD"
    other_vendor_result = vendor_types(other_vendor)
    assert (
        target_accelerator not in other_vendor_result
    ), f"vendor_types('{other_vendor}') should not contain {target_accelerator}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
