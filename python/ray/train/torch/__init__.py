# isort: off
try:
    import torch  # noqa: F401
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "PyTorch isn't installed. To install PyTorch, run 'pip install torch'"
    )
# isort: on

from ray.train.torch.config import TorchConfig
from ray.train.torch.torch_checkpoint import TorchCheckpoint
from ray.train.torch.torch_detection_predictor import TorchDetectionPredictor
from ray.train.torch.torch_predictor import TorchPredictor
from ray.train.torch.torch_trainer import TorchTrainer
from ray.train.torch.train_loop_utils import (
    accelerate,
    backward,
    enable_reproducibility,
    get_device,
    get_devices,
    prepare_data_loader,
    prepare_model,
    prepare_optimizer,
)

# Import XLA utilities
try:
    from ray.train.torch.xla import get_xla_mesh, is_xla_backend
    _XLA_UTILS_AVAILABLE = True
except ImportError:
    _XLA_UTILS_AVAILABLE = False
from ray.train.v2._internal.constants import is_v2_enabled

if is_v2_enabled():
    from ray.train.v2.torch.torch_trainer import TorchTrainer  # noqa: F811
    from ray.train.v2.torch.train_loop_utils import (  # noqa: F811
        accelerate,
        backward,
        enable_reproducibility,
        prepare_data_loader,
        prepare_model,
        prepare_optimizer,
    )


__all__ = [
    "TorchTrainer",
    "TorchCheckpoint",
    "TorchConfig",
    "accelerate",
    "get_device",
    "get_devices",
    "prepare_model",
    "prepare_optimizer",
    "prepare_data_loader",
    "backward",
    "enable_reproducibility",
    "TorchPredictor",
    "TorchDetectionPredictor",
]

# Add XLA utilities to __all__ if available
if _XLA_UTILS_AVAILABLE:
    __all__.extend(["get_xla_mesh", "is_xla_backend"])


# DO NOT ADD ANYTHING AFTER THIS LINE.
