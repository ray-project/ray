import os
from typing import TYPE_CHECKING, Optional

from sklearn.base import BaseEstimator
from ray.air._internal.checkpointing import save_preprocessor_to_dir
from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY
import ray.cloudpickle as cpickle
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="alpha")
class SklearnCheckpoint(Checkpoint):
    """A :py:class:`~ray.air.checkpoint.Checkpoint` with sklearn-specific
    functionality.

    Create this from a generic :py:class:`~ray.air.checkpoint.Checkpoint` by calling
    ``SklearnCheckpoint.from_checkpoint(ckpt)``
    """

    @classmethod
    def from_estimator(
        cls,
        estimator: BaseEstimator,
        *,
        path: os.PathLike,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "SklearnCheckpoint":
        """Create a :py:class:`~ray.air.checkpoint.Checkpoint` that stores an sklearn
        ``Estimator``.

        Args:
            estimator: The ``Estimator`` to store in the checkpoint.
            path: The directory where the checkpoint will be stored.
            preprocessor: A fitted preprocessor to be applied before inference.

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
        with open(os.path.join(path, MODEL_KEY), "wb") as f:
            cpickle.dump(estimator, f)

        if preprocessor:
            save_preprocessor_to_dir(preprocessor, path)

        checkpoint = cls.from_directory(path)

        return checkpoint

    def get_estimator(self) -> BaseEstimator:
        """Retrieve the ``Estimator`` stored in this checkpoint."""
        with self.as_directory() as checkpoint_path:
            estimator_path = os.path.join(checkpoint_path, MODEL_KEY)
            with open(estimator_path, "rb") as f:
                return cpickle.load(f)
