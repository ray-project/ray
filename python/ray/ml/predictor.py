import abc
from typing import Any, Union, TYPE_CHECKING

import ray
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

    def predict(
        self, preprocessed_dataset: ray.data.Dataset, **iter_batches_kwargs
    ) -> "pd.DataFrame":

        """Run inference on dataset.

        Args:
            preprocessed_dataset (Dataset): A Ray dataset that has been
                preprocessed already.
            iter_batches_kwargs: kwargs to pass to ``dataset.iter_batches()``

        Returns:
            A Pandas DataFrame containing prediction values.

        """
        predictions = []
        for batch in preprocessed_dataset.iter_batches(**iter_batches_kwargs):
            predictions.append(self.score_fn(batch))
        return pd.DataFrame({"predictions": predictions})

    def score_fn(self, data: DataBatchType) -> DataBatchType:
        """Run inference on data batch.

        Args:
            data (DataBatchType): Input data.

        Returns:
            DataBatchType: Prediction result.

        """
        raise NotImplementedError
