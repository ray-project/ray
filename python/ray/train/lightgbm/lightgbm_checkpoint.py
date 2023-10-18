import os
import tempfile
from typing import TYPE_CHECKING, Optional

import lightgbm

from ray.train._internal.framework_checkpoint import FrameworkCheckpoint
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="beta")
class LightGBMCheckpoint(FrameworkCheckpoint):
    """A :py:class:`~ray.train.Checkpoint` with LightGBM-specific functionality."""

    MODEL_FILENAME = "model.txt"

    @classmethod
    def from_model(
        cls,
        booster: lightgbm.Booster,
        *,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "LightGBMCheckpoint":
        """Create a :py:class:`~ray.train.Checkpoint` that stores a LightGBM model.

        Args:
            booster: The LightGBM model to store in the checkpoint.
            preprocessor: A fitted preprocessor to be applied before inference.

        Returns:
            An :py:class:`LightGBMCheckpoint` containing the specified ``Estimator``.

        Examples:
            >>> import lightgbm
            >>> import numpy as np
            >>> from ray.train.lightgbm import LightGBMCheckpoint
            >>>
            >>> train_X = np.array([[1, 2], [3, 4]])
            >>> train_y = np.array([0, 1])
            >>>
            >>> model = lightgbm.LGBMClassifier().fit(train_X, train_y)
            >>> checkpoint = LightGBMCheckpoint.from_model(model.booster_)
        """
        tempdir = tempfile.mkdtemp()
        booster.save_model(os.path.join(tempdir, cls.MODEL_FILENAME))

        checkpoint = cls.from_directory(tempdir)
        if preprocessor:
            checkpoint.set_preprocessor(preprocessor)

        return checkpoint

    def get_model(self) -> lightgbm.Booster:
        """Retrieve the LightGBM model stored in this checkpoint."""
        with self.as_directory() as checkpoint_path:
            return lightgbm.Booster(
                model_file=os.path.join(checkpoint_path, self.MODEL_FILENAME)
            )
