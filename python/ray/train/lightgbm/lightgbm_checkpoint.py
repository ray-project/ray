import os
from typing import TYPE_CHECKING, Optional

import lightgbm

from ray.air._internal.checkpointing import save_preprocessor_to_dir
from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="alpha")
class LightGBMCheckpoint(Checkpoint):
    """A :py:class:`~ray.air.checkpoint.Checkpoint` with LightGBM-specific
    functionality.

    Create this from a generic :py:class:`~ray.air.checkpoint.Checkpoint` by calling
    ``LightGBMCheckpoint.from_checkpoint(ckpt)``.
    """

    @classmethod
    def from_model(
        cls,
        booster: lightgbm.Booster,
        *,
        path: os.PathLike,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "LightGBMCheckpoint":
        """Create a :py:class:`~ray.air.checkpoint.Checkpoint` that stores a LightGBM
        model.

        Args:
            booster: The LightGBM model to store in the checkpoint.
            path: The directory where the checkpoint will be stored.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            An :py:class:`LightGBMCheckpoint` containing the specified ``Estimator``.

        Examples:
            >>> from ray.train.lightgbm import LightGBMCheckpoint
            >>> import lightgbm
            >>>
            >>> booster = lightgbm.Booster()  # doctest: +SKIP
            >>> checkpoint = LightGBMCheckpoint.from_model(booster, path=".")  # doctest: +SKIP # noqa: #501

            You can use a :py:class:`LightGBMCheckpoint` to create an
            :py:class:`~ray.train.lightgbm.LightGBMPredictor` and preform inference.

            >>> from ray.train.lightgbm import LightGBMPredictor
            >>>
            >>> predictor = LightGBMPredictor.from_checkpoint(checkpoint)  # doctest: +SKIP # noqa: #501
        """
        booster.save_model(os.path.join(path, MODEL_KEY))

        if preprocessor:
            save_preprocessor_to_dir(preprocessor, path)

        checkpoint = cls.from_directory(path)

        return checkpoint

    def get_model(self) -> lightgbm.Booster:
        """Retrieve the LightGBM model stored in this checkpoint."""
        with self.as_directory() as checkpoint_path:
            return lightgbm.Booster(model_file=os.path.join(checkpoint_path, MODEL_KEY))
