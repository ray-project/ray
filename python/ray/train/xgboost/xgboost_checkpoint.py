import os
from typing import TYPE_CHECKING, Optional

import xgboost

from ray.air._internal.checkpointing import save_preprocessor_to_dir
from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="alpha")
class XGBoostCheckpoint(Checkpoint):
    """A :py:class:`~ray.air.checkpoint.Checkpoint` with XGBoost-specific
    functionality.

    Create this from a generic :py:class:`~ray.air.checkpoint.Checkpoint` by calling
    ``XGBoostCheckpoint.from_checkpoint(ckpt)``.
    """

    @classmethod
    def from_model(
        cls,
        booster: xgboost.Booster,
        *,
        path: os.PathLike,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "XGBoostCheckpoint":
        """Create a :py:class:`~ray.air.checkpoint.Checkpoint` that stores an XGBoost
        model.

        Args:
            booster: The XGBoost model to store in the checkpoint.
            path: The directory where the checkpoint will be stored.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            An :py:class:`XGBoostCheckpoint` containing the specified ``Estimator``.

        Examples:
            >>> from ray.train.xgboost import XGBoostCheckpoint
            >>> import xgboost
            >>>
            >>> booster = xgboost.Booster()
            >>> checkpoint = XGBoostCheckpoint.from_model(booster, path=".")  # doctest: +SKIP # noqa: E501

            You can use a :py:class:`XGBoostCheckpoint` to create an
            :py:class:`~ray.train.xgboost.XGBoostPredictor` and preform inference.

            >>> from ray.train.xgboost import XGBoostPredictor
            >>>
            >>> predictor = XGBoostPredictor.from_checkpoint(checkpoint)  # doctest: +SKIP # noqa: E501
        """
        booster.save_model(os.path.join(path, MODEL_KEY))

        if preprocessor:
            save_preprocessor_to_dir(preprocessor, path)

        checkpoint = cls.from_directory(path)

        return checkpoint

    def get_model(self) -> xgboost.Booster:
        """Retrieve the XGBoost model stored in this checkpoint."""
        with self.as_directory() as checkpoint_path:
            booster = xgboost.Booster()
            booster.load_model(os.path.join(checkpoint_path, MODEL_KEY))
            return booster
