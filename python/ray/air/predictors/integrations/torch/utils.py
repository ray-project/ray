from typing import Optional

import torch

from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.air.preprocessor import Preprocessor


def to_air_checkpoint(
    model: torch.nn.Module, preprocessor: Optional[Preprocessor] = None
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
