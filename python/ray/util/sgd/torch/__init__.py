import logging
logger = logging.getLogger(__name__)

TorchTrainer = None
TrainingOperator = None
BaseTorchTrainable = None
CreatorOperator = None

try:
    import torch  # noqa: F401

    from ray.util.sgd.torch.torch_trainer import (TorchTrainer,
                                                  BaseTorchTrainable)

    from ray.util.sgd.torch.training_operator import (TrainingOperator,
                                                      CreatorOperator)

    __all__ = [
        "TorchTrainer", "BaseTorchTrainable", "TrainingOperator",
        "CreatorOperator"
    ]
except ImportError as e:
    logger.warning(e)
    logger.warning("PyTorch not found. TorchTrainer will not be available")
