import os
from typing import TYPE_CHECKING, Optional

from sklearn.base import BaseEstimator
from ray.air.checkpoint import Checkpoint
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="alpha")
class SklearnCheckpoint(Checkpoint):
    """A :py:class:`~ray.air.checkpoint.Checkpoint` with sklearn-specific functionality."""

    @classmethod
    def from_estimator(
        cls,
        estimator: BaseEstimator,
        *,
        path: os.PathLike,
        preprocessor: Optional["Preprocessor"] = None,
        **kwargs,
    ) -> "SklearnCheckpoint":
        """Create a :py:class:`~ray.air.checkpoint.Checkpoint` that stores an sklearn ``Estimator``.

        Args:
            estimator: The ``Estimator`` to store in the checkpoint.
            path: The directory where the checkpoint will be stored.
            preprocessor: A fitted preprocessor to be applied before inference.
            **kwargs: Arbitrary data to store in the checkpoint.

        Returns:
            An :py:class:`SklearnCheckpoint` containing the specified ``Estimator``.

        Examples:
            >>> from ray.train.sklearn import SklearnCheckpoint
            >>> from sklearn.ensemble import RandomForestClassifier
            >>>
            >>> estimator = RandomForestClassifier()
            >>> checkpoint = SklearnCheckpoint.from_estimator(estimator, path=".")

            You can use a :py:class:`SklearnCheckpoint` to create an
            :py:class:`~ray.train.sklearn.SklearnPredictor` and preform inference.

            >>> from ray.train.sklearn import SklearnPredictor
            >>>
            >>> predictor = SklearnPredictor.from_checkpoint(checkpoint)
        """
        raise NotImplementedError

    def get_model(self) -> BaseEstimator:
        """Retrieve the ``Estimator`` stored in this checkpoint."""
        raise NotImplementedError
