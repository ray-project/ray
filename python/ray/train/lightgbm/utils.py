import os
from typing import TYPE_CHECKING, Optional, Tuple

import lightgbm

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
    booster: lightgbm.Booster,
    *,
    path: os.PathLike,
    preprocessor: Optional["Preprocessor"] = None,
) -> Checkpoint:
    """Convert a pretrained model to AIR checkpoint for serve or inference.

    Example:

    .. code-block:: python

        import lightgbm
        import tempfile
        from ray.train.lightgbm import to_air_checkpoint, LightGBMPredictor

        bst = lightgbm.Booster()
        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint = to_air_checkpoint(booster=bst, path=tmpdir)
            predictor = LightGBMPredictor.from_checkpoint(checkpoint)

    Args:
        booster: A pretrained lightgbm model.
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
) -> Tuple[lightgbm.Booster, Optional["Preprocessor"]]:
    """Load a Checkpoint from ``LightGBMTrainer``.

    Args:
        checkpoint: The checkpoint to load the model and
            preprocessor from. It is expected to be from the result of a
            ``LightGBMTrainer`` run.

    Returns:
        The model and AIR preprocessor contained within.
    """
    with checkpoint.as_directory() as checkpoint_path:
        lgbm_model = lightgbm.Booster(
            model_file=os.path.join(checkpoint_path, MODEL_KEY)
        )
        preprocessor = load_preprocessor_from_dir(checkpoint_path)

    return lgbm_model, preprocessor
