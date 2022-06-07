from typing import Optional

import os
import lightgbm

from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY
from ray.air.preprocessor import Preprocessor
from ray.air.utils.checkpointing import (
    save_preprocessor_to_dir,
)


def to_air_checkpoint(
    path: str,
    booster: lightgbm.Booster,
    preprocessor: Optional[Preprocessor] = None,
) -> Checkpoint:
    """Convert a pretrained model to AIR checkpoint for serve or inference.

    Args:
        path: The directory path where model and preprocessor steps are stored to.
        booster: A pretrained lightgbm model.
        preprocessor: A fitted preprocessor. The preprocessing logic will
            be applied to serve/inference.
    Returns:
        A Ray Air checkpoint.
    """
    booster.save_model(os.path.join(path, MODEL_KEY))

    if preprocessor:
        save_preprocessor_to_dir(preprocessor, path)

    checkpoint = Checkpoint.from_directory(path)

    return checkpoint
