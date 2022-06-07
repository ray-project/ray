import abc
from typing import Dict, Type, Union, TYPE_CHECKING

from ray.air.checkpoint import Checkpoint
from ray.util.annotations import DeveloperAPI, PublicAPI

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    import pyarrow

DataBatchType = Union[np.ndarray, pd.DataFrame, "pyarrow.Table", Dict[str, np.ndarray]]


@PublicAPI(stability="alpha")
class PredictorNotSerializableException(RuntimeError):
    """Error raised when trying to serialize a Predictor instance."""

    pass


@PublicAPI(stability="alpha")
class Predictor(abc.ABC):
    """Predictors load models from checkpoints to perform inference.

    Note: The base ``Predictor`` class cannot be instantiated directly. Only one of
    its subclasses can be used.

    **How does a Predictor work?**

    Predictors expose a ``predict`` method that accepts an input batch of type
    ``DataBatchType`` and outputs predictions of the same type as the input batch.

    When the ``predict`` method is called the following occurs:

        - The input batch is converted into a pandas DataFrame. Tensor input (like a
          ``np.ndarray``) will be converted into a single column Pandas Dataframe.
        - If there is a :ref:`Preprocessor <air-preprocessor-ref>` saved in the provided
          :ref:`Checkpoint <air-checkpoint-ref>`, the preprocessor will be used to
          transform the DataFrame.
        - The transformed DataFrame will be passed to the model for inference (via the
          ``predictor._predict_pandas`` method).
        - The predictions will be outputted by ``predict`` in the same type as the
          original input.

    **How do I create a new Predictor?**

    To implement a new Predictor for your particular framework, you should subclass
    the base ``Predictor`` and implement the following two methods:

        1. ``_predict_pandas``: Given a pandas.DataFrame input, return a
           pandas.DataFrame containing predictions.
        2. ``from_checkpoint``: Logic for creating a Predictor from an
           :ref:`AIR Checkpoint <air-checkpoint-ref>`.
    """

    @classmethod
    @PublicAPI(stability="alpha")
    @abc.abstractmethod
    def from_checkpoint(cls, checkpoint: Checkpoint, **kwargs) -> "Predictor":
        """Create a specific predictor from a checkpoint.

        Args:
            checkpoint: Checkpoint to load predictor data from.
            kwargs: Arguments specific to predictor implementations.

        Returns:
            Predictor: Predictor object.
        """
        raise NotImplementedError

    @PublicAPI(stability="alpha")
    def predict(self, data: DataBatchType, **kwargs) -> DataBatchType:
        """Perform inference on a batch of data.

        Args:
            data: A batch of input data of type ``DataBatchType``.
            kwargs: Arguments specific to predictor implementations. These are passed
            directly to ``_predict_pandas``.

        Returns:
            DataBatchType: Prediction result.
        """

        pass

        # TODO(amogkam): Implement below code.
        # data_df = _convert_batch_type_to_pandas(data)
        #
        # if hasattr(self, "preprocessor") and self.preprocessor:
        #     data_df = self.preprocessor.transform_batch(data_df)
        #
        # predictions_df = self._predict_pandas(data_df)
        # return _convert_pandas_to_batch_type(predictions_df, type=type(data))

    @DeveloperAPI
    def _predict_pandas(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Perform inference on a Pandas DataFrame.

        All predictors are expected to implement this method.

        Args:
            data: A pandas DataFrame to perform predictions on.
            kwargs: Arguments specific to the predictor implementation.

        Returns:
            A pandas DataFrame containing the prediction result.

        """
        raise NotImplementedError

    def __reduce__(self):
        raise PredictorNotSerializableException(
            "Predictor instances are not serializable. Instead, you may want "
            "to serialize a checkpoint and initialize the Predictor with "
            "Predictor.from_checkpoint."
        )


def _convert_batch_type_to_pandas(data: DataBatchType) -> pd.DataFrame:
    """Convert the provided data to a Pandas DataFrame."""
    pass


def _convert_pandas_to_batch_type(
    data: pd.DataFrame, type: Type[DataBatchType]
) -> DataBatchType:
    """Convert the provided Pandas dataframe to the provided ``type``."""

    pass
