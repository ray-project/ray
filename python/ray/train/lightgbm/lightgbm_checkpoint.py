import os
import tempfile
from typing import TYPE_CHECKING, Optional

import lightgbm

from ray.air._internal.checkpointing import save_preprocessor_to_dir
from ray.air.checkpoint import Checkpoint
from ray.air.constants import MODEL_KEY
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="beta")
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
        preprocessor: Optional["Preprocessor"] = None,
    ) -> "LightGBMCheckpoint":
        """Create a :py:class:`~ray.air.checkpoint.Checkpoint` that stores a LightGBM
        model.

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

            You can use a :py:class:`LightGBMCheckpoint` to create an
            :py:class:`~ray.train.lightgbm.LightGBMPredictor` and preform inference.

            >>> from ray.train.lightgbm import LightGBMPredictor
            >>>
            >>> predictor = LightGBMPredictor.from_checkpoint(checkpoint)
        """
        with tempfile.TemporaryDirectory() as tmpdirname:
            booster.save_model(os.path.join(tmpdirname, MODEL_KEY))

            if preprocessor:
                save_preprocessor_to_dir(preprocessor, tmpdirname)

            checkpoint = cls.from_directory(tmpdirname)
            ckpt_dict = checkpoint.to_dict()

        return cls.from_dict(ckpt_dict)

    def get_model(self) -> lightgbm.Booster:
        """Retrieve the LightGBM model stored in this checkpoint."""
        with self.as_directory() as checkpoint_path:
            return lightgbm.Booster(model_file=os.path.join(checkpoint_path, MODEL_KEY))
