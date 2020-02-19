import logging
logger = logging.getLogger(__name__)

PyTorchTrainer = None
PyTorchTrainable = None

try:
    import torch  # noqa: F401

    from ray.util.sgd.pytorch.pytorch_trainer import (PyTorchTrainer,
                                                      PyTorchTrainable)

    __all__ = ["PyTorchTrainer", "PyTorchTrainable"]
except ImportError:
    logger.warning("PyTorch not found. PyTorchTrainer will not be available")
