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

    __all__ = ["TorchTrainer", "BaseTorchTrainable", "TrainingOperator"]
except ImportError:
    logger.warning("PyTorch not found. TorchTrainer will not be available")

class ContextManager:
	def __enter__(self):
		import tempfile
		if torch.distributed.rank == 0:
			delete = False
		else:
			delete = True
		self._file = tempfile.tempfile(delete=delete)
		return self._file

	def __exit__(self):
		from ray import tune
		if torch.distributed.rank == 0:
			tune.save_checkpoint(self._file)

def create_checkpoint(step):
	return ContextManager()