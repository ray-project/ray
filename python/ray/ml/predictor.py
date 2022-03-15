import abc
from typing import Union, TYPE_CHECKING

from ray.ml.checkpoint import Checkpoint

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd

DataBatchType = Union["pd.DataFrame", "np.ndarray"]


class Predictor(abc.ABC):
    """Predictors load models from checkpoints to perform inference."""

    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint, **kwargs) -> "Predictor":
        """Create a specific predictor from a checkpoint.

        Args:
            checkpoint: Checkpoint to load predictor data from.
            kwargs: Arguments specific to predictor implementations.

        Returns:
            Predictor: Predictor object.
        """
        raise NotImplementedError

    def predict(self, data: DataBatchType, **kwargs) -> DataBatchType:
        """Perform inference on a batch of data.

        Args:
            data: A batch of input data. Either a pandas Dataframe or numpy
                array.
            kwargs: Arguments specific to predictor implementations.

        Returns:
            DataBatchType: Prediction result.
        """
        raise NotImplementedError
