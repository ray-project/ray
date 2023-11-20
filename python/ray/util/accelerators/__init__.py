import warnings

from ray.util.accelerators import tpu
from ray.util.accelerators.accelerators import (
    NVIDIA_TESLA_V100,
    NVIDIA_TESLA_P100,
    NVIDIA_TESLA_T4,
    NVIDIA_TESLA_P4,
    NVIDIA_TESLA_K80,
    NVIDIA_TESLA_A10G,
    NVIDIA_A100,
    INTEL_MAX_1550,
    INTEL_MAX_1100,
    INTEL_GAUDI,
    AWS_NEURON_CORE,
    GOOGLE_TPU_V2,
    GOOGLE_TPU_V3,
    GOOGLE_TPU_V4,
)

__all__ = [
    "tpu",
    "NVIDIA_TESLA_V100",
    "NVIDIA_TESLA_P100",
    "NVIDIA_TESLA_T4",
    "NVIDIA_TESLA_P4",
    "NVIDIA_TESLA_K80",
    "NVIDIA_TESLA_A10G",
    "NVIDIA_A100",
    "INTEL_MAX_1550",
    "INTEL_MAX_1100",
    "INTEL_GAUDI",
    "AWS_NEURON_CORE",
    "GOOGLE_TPU_V2",
    "GOOGLE_TPU_V3",
    "GOOGLE_TPU_V4",
    # Deprecated
    "NVIDIA_TESLA_A100",
]


def __getattr__(name: str):
    if name == "NVIDIA_TESLA_A100":
        from ray.util.annotations import RayDeprecationWarning

        warnings.warn(
            "NVIDIA_TESLA_A100 is deprecated, use NVIDIA_A100 instead.",
            RayDeprecationWarning,
            stacklevel=2,
        )
        return NVIDIA_A100
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
