from typing import TYPE_CHECKING, Optional

import torch

from ray.air._internal.torch_utils import load_torch_model
from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.train.data_parallel_trainer import _load_checkpoint

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


class TorchCheckpoint(Checkpoint):
    """Wrapper class that adds torch-specific accessors to a Checkpoint."""

    def __init__(self, checkpoint: Checkpoint):
        """Construct a TorchCheckpoint from a generic Checkpoint object."""
        self._clone_storage_from(checkpoint)

    @staticmethod
    def from_model(
        model: torch.nn.Module,
        *,
        preprocessor: Optional["Preprocessor"] = None,
        **kwargs
    ) -> "TorchCheckpoint":
        """Create a (Torch)Checkpoint from a torch module.

        Args:
            model: A pretrained model.
            preprocessor: A fitted preprocessor. The preprocessing logic will
                be applied to the inputs for serving/inference.

        Returns:
            A checkpoint that can be loaded by TorchPredictor.
        """
        checkpoint = Checkpoint.from_dict(
            {PREPROCESSOR_KEY: preprocessor, MODEL_KEY: model, **kwargs}
        )
        return TorchCheckpoint(checkpoint)

    def get_model(self, model: Optional[torch.nn.Module] = None) -> torch.nn.Module:
        """Return the model stored in this checkpoint.

        Args:
            model: If the checkpoint contains a model state dict, and not
                the model itself, then the state dict will be loaded to this
                ``model``.

        Returns:
           The model with set weights.
        """
        saved_model, _ = _load_checkpoint(self, "TorchTrainer")
        model = load_torch_model(saved_model=saved_model, model_definition=model)
        return model
