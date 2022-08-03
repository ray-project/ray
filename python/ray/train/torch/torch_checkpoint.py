from typing import TYPE_CHECKING, Optional

import torch

from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.train.data_parallel_trainer import _load_checkpoint
from ray.air._internal.torch_utils import load_torch_model
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="beta")
class TorchCheckpoint(Checkpoint):
    """A :py:class:`~ray.air.checkpoint.Checkpoint` with Torch-specific
    functionality.

    Create this from a generic :py:class:`~ray.air.checkpoint.Checkpoint` by calling
    ``TorchCheckpoint.from_checkpoint(ckpt)``.
    """

    @classmethod
    def from_model(
        cls,
        model: torch.nn.Module,
        *,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "TorchCheckpoint":
        """Create a :py:class:`~ray.air.checkpoint.Checkpoint` that stores a Torch
        model.

        Args:
            model: The Torch model to store in the checkpoint.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            An :py:class:`TorchCheckpoint` containing the specified model.

        Examples:
            >>> from ray.train.torch import TorchCheckpoint
            >>> import torch
            >>>
            >>> model = torch.nn.Identity()
            >>> checkpoint = TorchCheckpoint.from_model(model)

            You can use a :py:class:`TorchCheckpoint` to create an
            :py:class:`~ray.train.torch.TorchPredictor` and preform inference.

            >>> from ray.train.torch import TorchPredictor
            >>>
            >>> predictor = TorchPredictor.from_checkpoint(checkpoint)
        """
        checkpoint = cls.from_dict({PREPROCESSOR_KEY: preprocessor, MODEL_KEY: model})
        return checkpoint

    def get_model(self, model: Optional[torch.nn.Module] = None) -> torch.nn.Module:
        """Retrieve the model stored in this checkpoint.

        Args:
            model: If the checkpoint contains a model state dict, and not
                the model itself, then the state dict will be loaded to this
                ``model``.
        """
        saved_model, _ = _load_checkpoint(self, "TorchTrainer")
        model = load_torch_model(saved_model=saved_model, model_definition=model)
        return model
