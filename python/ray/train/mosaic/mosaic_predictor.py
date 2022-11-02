import torch
from typing import Optional

from ray.train.mosaic.mosaic_checkpoint import MosaicCheckpoint
from ray.train.torch import TorchPredictor
from ray.air.checkpoint import Checkpoint


class MosaicPredictor(TorchPredictor):
    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: Checkpoint,
        model: Optional[torch.nn.Module] = None,
        use_gpu: bool = False,
    ) -> "MosaicPredictor":
        """Instantiate the predictor from a Checkpoint.

        The checkpoint is expected to be a result of ``TorchTrainer``.

        Args:
            checkpoint: The checkpoint to load the model and
                preprocessor from. It is expected to be from the result of a
                ``MosaicTrainer`` run.
            model: If the checkpoint contains a model state dict, and not
                the model itself, then the state dict will be loaded to this
                ``model``. If the checkpoint already contains the model itself,
                this model argument will be discarded.
            use_gpu: If set, the model will be moved to GPU on instantiation and
                prediction happens on GPU.
        """
        checkpoint = MosaicCheckpoint.from_checkpoint(checkpoint)
        model = checkpoint.get_model(model)
        preprocessor = checkpoint.get_preprocessor()
        return cls(model=model, preprocessor=preprocessor, use_gpu=use_gpu)
