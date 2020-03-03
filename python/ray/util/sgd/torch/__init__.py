import logging
logger = logging.getLogger(__name__)

TorchTrainer = None
PyTorchTrainable = None
TrainingOperator = None

try:
    import torch  # noqa: F401

    from ray.util.sgd.pytorch.pytorch_trainer import (TorchTrainer,
                                                      PyTorchTrainable)

    from ray.util.sgd.pytorch.training_operator import TrainingOperator

    __all__ = ["TorchTrainer", "PyTorchTrainable", "TrainingOperator"]
except ImportError:
    logger.warning("PyTorch not found. TorchTrainer will not be available")
