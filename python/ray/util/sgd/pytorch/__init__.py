import logging
logger = logging.getLogger(__name__)

PyTorchTrainer = None
PyTorchTrainable = None
TrainingOperator = None

try:
    import torch  # noqa: F401

    from ray.util.sgd.pytorch.pytorch_trainer import (PyTorchTrainer,
                                                      PyTorchTrainable)

    from ray.util.sgd.pytorch.training_operator import TrainingOperator

    __all__ = ["PyTorchTrainer", "PyTorchTrainable", "TrainingOperator"]
except ImportError:
    logger.warning("PyTorch not found. PyTorchTrainer will not be available")
