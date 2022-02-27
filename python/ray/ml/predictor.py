import abc
from typing import Any, Union, TYPE_CHECKING

from ray.ml.checkpoint import Checkpoint

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd

    DataBatchType = Union[pd.DataFrame, np.ndarray]
else:
    DataBatchType = Any


class Predictor(abc.ABC):
    """Predictor interface."""

    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint) -> "Predictor":
        """Load predictor data from checkpoint.

        Args:
            checkpoint (Checkpoint): Checkpoint to load predictor data from.

        Returns:
            Predictor: Predictor object.
        """
        raise NotImplementedError

    def predict(self, data: DataBatchType) -> DataBatchType:
        """Run inference on data batch.

        Args:
            data (DataBatchType): Input data.

        Returns:
            DataBatchType: Prediction result.

        """
        raise NotImplementedError
