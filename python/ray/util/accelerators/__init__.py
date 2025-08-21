import warnings

import ray.util.accelerators.accelerators as _accelerators
from ray.util.accelerators import tpu
from ray.util.accelerators.types import AcceleratorTypes

# Get all GPU constant names
_GPU_CONSTANTS = [
    name
    for name in dir(_accelerators)
    if not (name.startswith("__") and name.endswith("__"))
]

__all__ = ["tpu"] + _GPU_CONSTANTS + ["NVIDIA_TESLA_A100"] + ["TYPES"]


# Dynamically get constants via `__getattr__` and warnings
def __getattr__(name):
    if name == "TYPES":
        return AcceleratorTypes().all_constants

    if name in __all__:
        from ray.util.annotations import RayDeprecationWarning

        if name == "NVIDIA_TESLA_A100":
            warnings.warn(
                "NVIDIA_TESLA_A100 is deprecated, use NVIDIA_A100 instead.",
                RayDeprecationWarning,
                stacklevel=2,
            )
            name = "NVIDIA_A100"

        elif name == "NVIDIA_A100":
            warnings.warn(
                "NVIDIA_A100 can be replaced with NVIDIA_A100_40G or NVIDIA_A100_80G for more precise accelerator specification.",
                UserWarning,
            )

        else:
            warnings.warn(
                f"Accessing GPU constants via this method (ray.util.accelerators.{name}) is deprecated and will be removed in a future version. "
                f"Please use just strings instead ('{getattr(_accelerators, name)}').",
                RayDeprecationWarning,
                stacklevel=2,
            )

        return getattr(_accelerators, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
