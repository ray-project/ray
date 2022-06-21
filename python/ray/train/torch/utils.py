from typing import TYPE_CHECKING, Optional, Tuple

import torch

from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.train.data_parallel_trainer import _load_checkpoint
from ray.air._internal.torch_utils import load_torch_model

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


def to_air_checkpoint(
    model: torch.nn.Module, preprocessor: Optional["Preprocessor"] = None
) -> Checkpoint:
    """Convert a pretrained model to AIR checkpoint for serve or inference.

    Args:
        model: A pretrained model.
        preprocessor: A fitted preprocessor. The preprocessing logic will
            be applied to serve/inference.
    Returns:
        A Ray Air checkpoint.
    """
    checkpoint = Checkpoint.from_dict(
        {PREPROCESSOR_KEY: preprocessor, MODEL_KEY: model}
    )
    return checkpoint


def load_checkpoint(
    checkpoint: Checkpoint, model: Optional[torch.nn.Module] = None
) -> Tuple[torch.nn.Module, Optional["Preprocessor"]]:
    """Load a Checkpoint from ``TorchTrainer``.

    Args:
        checkpoint: The checkpoint to load the model and
            preprocessor from. It is expected to be from the result of a
            ``TorchTrainer`` run.
        model: If the checkpoint contains a model state dict, and not
            the model itself, then the state dict will be loaded to this
            ``model``.

    Returns:
        The model with set weights and AIR preprocessor contained within.
    """
    saved_model, preprocessor = _load_checkpoint(checkpoint, "TorchTrainer")
    model = load_torch_model(saved_model=saved_model, model_definition=model)
    return model, preprocessor
