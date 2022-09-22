import abc
from typing import Dict, Type, Optional, Callable

import numpy as np
import pandas as pd

from ray.air.checkpoint import Checkpoint
from ray.air.data_batch_type import DataBatchType
from ray.air.util.data_batch_conversion import (
    DataType,
    convert_batch_type_to_pandas,
    convert_pandas_to_batch_type,
)
from ray.data import Preprocessor
from ray.util.annotations import DeveloperAPI, PublicAPI

try:
    import pyarrow

    pa_table = pyarrow.Table
except ImportError:
    pa_table = None

TYPE_TO_ENUM: Dict[Type[DataBatchType], DataType] = {
    np.ndarray: DataType.NUMPY,
    dict: DataType.NUMPY,
    pd.DataFrame: DataType.PANDAS,
    pa_table: DataType.ARROW,
}


@PublicAPI(stability="beta")
class PredictorNotSerializableException(RuntimeError):
    """Error raised when trying to serialize a Predictor instance."""

    pass


@PublicAPI(stability="beta")
class Predictor(abc.ABC):
    """Predictors load models from checkpoints to perform inference.

    .. note::
        The base ``Predictor`` class cannot be instantiated directly. Only one of
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
    3. Optionally ``_predict_arrow`` for better performance when working with
       tensor data to avoid extra copies from Pandas conversions.
    """

    def __init__(self, preprocessor: Optional[Preprocessor] = None):
        """Subclasseses must call Predictor.__init__() to set a preprocessor."""
        self._preprocessor: Optional[Preprocessor] = preprocessor
        # Whether tensor columns should be automatically cast from/to the tensor
        # extension type at UDF boundaries. This can be overridden by subclasses.
        self._cast_tensor_columns = False

    @classmethod
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

    @classmethod
    def from_pandas_udf(
        cls, pandas_udf: Callable[[pd.DataFrame], pd.DataFrame]
    ) -> "Predictor":
        """Create a Predictor from a Pandas UDF.

        Args:
            pandas_udf: A function that takes a pandas.DataFrame and other
                optional kwargs and returns a pandas.DataFrame.
        """

        class PandasUDFPredictor(Predictor):
            @classmethod
            def from_checkpoint(cls, checkpoint: Checkpoint, **kwargs):
                return PandasUDFPredictor()

            def _predict_pandas(self, df, **kwargs) -> "pd.DataFrame":
                return pandas_udf(df, **kwargs)

        return PandasUDFPredictor.from_checkpoint(Checkpoint.from_dict({"dummy": 1}))

    def get_preprocessor(self) -> Optional[Preprocessor]:
        """Get the preprocessor to use prior to executing predictions."""
        return self._preprocessor

    def set_preprocessor(self, preprocessor: Optional[Preprocessor]) -> None:
        """Set the preprocessor to use prior to executing predictions."""
        self._preprocessor = preprocessor

    def _set_cast_tensor_columns(self):
        """Enable automatic tensor column casting.

        If this is called on a predictor, the predictor will cast tensor columns to
        NumPy ndarrays in the input to the preprocessors and cast tensor columns back to
        the tensor extension type in the prediction outputs.
        """
        self._cast_tensor_columns = True

    def predict(self, data: DataBatchType, **kwargs) -> DataBatchType:
        """Perform inference on a batch of data.

        Args:
            data: A batch of input data of type ``DataBatchType``.
            kwargs: Arguments specific to predictor implementations. These are passed
            directly to ``_predict_pandas``.

        Returns:
            DataBatchType:
                Prediction result. The return type will be the same as the input type.
        """
        data_df = convert_batch_type_to_pandas(data, self._cast_tensor_columns)

        if not hasattr(self, "_preprocessor"):
            raise NotImplementedError(
                "Subclasses of Predictor must call Predictor.__init__(preprocessor)."
            )

        if self._preprocessor:
            data_df = self._preprocessor.transform_batch(data_df)

        predictions_df = self._predict_pandas(data_df, **kwargs)
        return convert_pandas_to_batch_type(
            predictions_df,
            type=TYPE_TO_ENUM[type(data)],
            cast_tensor_columns=self._cast_tensor_columns,
        )

    @DeveloperAPI
    def _predict_pandas(self, data: "pd.DataFrame", **kwargs) -> "pd.DataFrame":
        """Perform inference on a Pandas DataFrame.

        All predictors are expected to implement this method.

        Args:
            data: A pandas DataFrame to perform predictions on.
            kwargs: Arguments specific to the predictor implementation.

        Returns:
            A pandas DataFrame containing the prediction result.

        """
        raise NotImplementedError

    @DeveloperAPI
    def _predict_arrow(self, data: "pyarrow.Table", **kwargs) -> "pyarrow.Table":
        """Perform inference on an Arrow Table.

        Predictors can implement this method instead of ``_predict_pandas``
        for better performance when the input batch type is a Numpy array, dict of
        numpy arrays, or an Arrow Table as conversion from these types are zero copy.

        Args:
            data: An Arrow Table to perform predictions on.
            kwargs: Arguments specific to the predictor implementation.

        Returns:
            An Arrow Table containing the prediction result.
        """

        raise NotImplementedError

    def __reduce__(self):
        raise PredictorNotSerializableException(
            "Predictor instances are not serializable. Instead, you may want "
            "to serialize a checkpoint and initialize the Predictor with "
            "Predictor.from_checkpoint."
        )
