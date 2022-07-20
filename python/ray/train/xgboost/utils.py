import os
from typing import TYPE_CHECKING, Optional, Tuple

import xgboost

from ray.air._internal.checkpointing import (
    save_preprocessor_to_dir,
    load_preprocessor_from_dir,
)
from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="alpha")
def to_air_checkpoint(
    booster: xgboost.Booster,
    *,
    path: os.PathLike,
    preprocessor: Optional["Preprocessor"] = None,
) -> Checkpoint:
    """Convert a pretrained model to AIR checkpoint for serve or inference.

    Example:

    .. code-block:: python

        import xgboost
        import tempfile
        from ray.train.xgboost import to_air_checkpoint, XGBoostPredictor

        bst = xgboost.Booster()
        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint = to_air_checkpoint(booster=bst, path=tmpdir)
            predictor = XGBoostPredictor.from_checkpoint(checkpoint)

    Args:
        booster: A pretrained xgboost model.
        path: The directory where the checkpoint will be stored to.
        preprocessor: A fitted preprocessor. The preprocessing logic will
            be applied to the inputs for serving/inference.

    Returns:
        A Ray AIR checkpoint.
    """
    booster.save_model(os.path.join(path, MODEL_KEY))

    if preprocessor:
        save_preprocessor_to_dir(preprocessor, path)

    checkpoint = Checkpoint.from_directory(path)

    return checkpoint


@PublicAPI(stability="alpha")
def load_checkpoint(
    checkpoint: Checkpoint,
) -> Tuple[xgboost.Booster, Optional["Preprocessor"]]:
    """Load a Checkpoint from ``XGBoostTrainer``.

    Args:
        checkpoint: The checkpoint to load the model and
            preprocessor from. It is expected to be from the result of a
            ``XGBoostTrainer`` run.

    Returns:
        The model and AIR preprocessor contained within.
    """
    with checkpoint.as_directory() as checkpoint_path:
        xgb_model = xgboost.Booster()
        xgb_model.load_model(os.path.join(checkpoint_path, MODEL_KEY))
        preprocessor = load_preprocessor_from_dir(checkpoint_path)

    return xgb_model, preprocessor
