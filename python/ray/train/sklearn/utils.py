import os
from typing import TYPE_CHECKING, Optional, Tuple

from sklearn.base import BaseEstimator

import ray.cloudpickle as cpickle
from ray.air._internal.checkpointing import (
    save_preprocessor_to_dir,
    load_preprocessor_from_dir,
)
from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


def to_air_checkpoint(
    path: str,
    estimator: BaseEstimator,
    preprocessor: Optional["Preprocessor"] = None,
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
