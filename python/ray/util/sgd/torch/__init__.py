import logging
logger = logging.getLogger(__name__)

TorchTrainer = None
TrainingOperator = None
BaseTorchTrainable = None

try:
    import torch  # noqa: F401

    from ray.util.sgd.torch.torch_trainer import (TorchTrainer,
                                                  BaseTorchTrainable)

    from ray.util.sgd.torch.training_operator import TrainingOperator
    from ray.util.sgd.torch.func_trainable import (DistributedTrainableCreator,
                                                   distributed_checkpoint)

    __all__ = [
        "TorchTrainer", "BaseTorchTrainable", "TrainingOperator",
        "distributed_checkpoint", "DistributedTrainableCreator"
    ]
except ImportError as e:
    logger.warning(e)
    logger.warning("PyTorch not found. TorchTrainer will not be available")
