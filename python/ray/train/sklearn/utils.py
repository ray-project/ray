import os
from typing import TYPE_CHECKING, Optional, Tuple

from sklearn.base import BaseEstimator

import ray.cloudpickle as cpickle
from ray.air._internal.checkpointing import (
    load_preprocessor_from_dir,
)
from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="alpha")
def load_checkpoint(
    checkpoint: Checkpoint,
) -> Tuple[BaseEstimator, Optional["Preprocessor"]]:
    """Load a Checkpoint from ``SklearnTrainer``.

    Args:
        checkpoint: The checkpoint to load the estimator and
            preprocessor from. It is expected to be from the result of a
            ``SklearnTrainer`` run.

    Returns:
        The estimator and AIR preprocessor contained within.
    """
    with checkpoint.as_directory() as checkpoint_path:
        estimator_path = os.path.join(checkpoint_path, MODEL_KEY)
        with open(estimator_path, "rb") as f:
            estimator = cpickle.load(f)
        preprocessor = load_preprocessor_from_dir(checkpoint_path)

    return estimator, preprocessor
