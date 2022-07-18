from typing import TYPE_CHECKING, Optional

import torch

from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


class TorchCheckpoint(Checkpoint):
    """Wrapper class that adds torch-specific accessors to a Checkpoint."""

    def __init__(self, checkpoint: Checkpoint):
        """Construct a TorchCheckpoint from a generic Checkpoint object."""
        self._clone_storage_from(checkpoint)

    @staticmethod
    def from_torch_model(
        model: torch.nn.Module, *, preprocessor: Optional["Preprocessor"] = None
    ) -> "TorchCheckpoint":
        """Create a (Torch)Checkpoint from a torch module.

        Args:
            model: A pretrained model.
            preprocessor: A fitted preprocessor. The preprocessing logic will
                be applied to the inputs for serving/inference.

        Returns:
            A checkpoint that can be loaded by TorchPredictor.
        """
        checkpoint = TorchCheckpoint.from_dict(
            {PREPROCESSOR_KEY: preprocessor, MODEL_KEY: model}
        )
        return checkpoint

    def get_model(self) -> torch.nn.Module:
        """Return the torch model contained in this Checkpoint."""
        return self.as_dict()[MODEL_KEY]
