from typing import Optional

import os
from sklearn.base import BaseEstimator

from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY
from ray.air.preprocessor import Preprocessor
from ray.air.utils.checkpointing import (
    save_preprocessor_to_dir,
)
import ray.cloudpickle as cpickle


def to_air_checkpoint(
    path: str,
    estimator: BaseEstimator,
    preprocessor: Optional[Preprocessor] = None,
) -> Checkpoint:
    """Convert a pretrained model to AIR checkpoint for serve or inference.

    Args:
        path: The directory path where model and preprocessor steps are stored to.
        estimator: A pretrained model.
        preprocessor: A fitted preprocessor. The preprocessing logic will
            be applied to serve/inference.
    Returns:
        A Ray Air checkpoint.
    """
    with open(os.path.join(path, MODEL_KEY), "wb") as f:
        cpickle.dump(estimator, f)

    if preprocessor:
        save_preprocessor_to_dir(preprocessor, path)

    checkpoint = Checkpoint.from_directory(path)

    return checkpoint
