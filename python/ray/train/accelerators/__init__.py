from .base import Accelerator  # noqa: F401

try:
    import torch  # noqa: F401
    from .torch import TorchAccelerator  # noqa: F401
except ImportError:
    pass
