import abc
from typing import Callable, Dict, Optional, Type, Union

import numpy as np
import pandas as pd

from ray.air.data_batch_type import DataBatchType
from ray.air.util.data_batch_conversion import (
    BatchFormat,
    _convert_batch_type_to_numpy,
    _convert_batch_type_to_pandas,
)
from ray.data import Preprocessor
from ray.train import Checkpoint
from ray.util.annotations import DeveloperAPI, PublicAPI

try:
    import pyarrow

    pa_table = pyarrow.Table
except ImportError:
    pa_table = None

# Reverse mapping from data batch type to batch format.
TYPE_TO_ENUM: Dict[Type[DataBatchType], BatchFormat] = {
    np.ndarray: BatchFormat.NUMPY,
    dict: BatchFormat.NUMPY,
    pd.DataFrame: BatchFormat.PANDAS,
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
    - If there is a :ref:`Preprocessor <preprocessor-ref>` saved in the provided
      :class:`Checkpoint <ray.train.Checkpoint>`, the preprocessor will be used to
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
    2. ``from_checkpoint``: Logic for creating a Predictor from a
       :class:`Checkpoint <ray.train.Checkpoint>`.
    3. Optionally ``_predict_numpy`` for better performance when working with
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
            def from_checkpoint(cls, checkpoint: Checkpoint, **kwargs) -> "Predictor":
                return PandasUDFPredictor()

            def _predict_pandas(self, df, **kwargs) -> "pd.DataFrame":
                return pandas_udf(df, **kwargs)

        return PandasUDFPredictor()

    def get_preprocessor(self) -> Optional[Preprocessor]:
        """Get the preprocessor to use prior to executing predictions."""
        return self._preprocessor

    def set_preprocessor(self, preprocessor: Optional[Preprocessor]) -> None:
        """Set the preprocessor to use prior to executing predictions."""
        self._preprocessor = preprocessor

    @classmethod
    @DeveloperAPI
    def preferred_batch_format(cls) -> BatchFormat:
        """Batch format hint for upstream producers to try yielding best block format.

        The preferred batch format to use if both `_predict_pandas` and
        `_predict_numpy` are implemented. Defaults to Pandas.

        Can be overriden by predictor classes depending on the framework type,
        e.g. TorchPredictor prefers Numpy and XGBoostPredictor prefers Pandas as
        native batch format.

        """
        return BatchFormat.PANDAS

    @classmethod
    def _batch_format_to_use(cls) -> BatchFormat:
        """Determine the batch format to use for the predictor."""
        has_pandas_implemented = cls._predict_pandas != Predictor._predict_pandas
        has_numpy_implemented = cls._predict_numpy != Predictor._predict_numpy
        if has_pandas_implemented and has_numpy_implemented:
            return cls.preferred_batch_format()
        elif has_pandas_implemented:
            return BatchFormat.PANDAS
        elif has_numpy_implemented:
            return BatchFormat.NUMPY
        else:
            raise NotImplementedError(
                f"Predictor {cls.__name__} must implement at least one of "
                "`_predict_pandas` and `_predict_numpy`."
            )

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
            directly to ``_predict_numpy`` or ``_predict_pandas``.

        Returns:
            DataBatchType:
                Prediction result. The return type will be the same as the input type.
        """
        if not hasattr(self, "_preprocessor"):
            raise NotImplementedError(
                "Subclasses of Predictor must call Predictor.__init__(preprocessor)."
            )
        try:
            batch_format = TYPE_TO_ENUM[type(data)]
        except KeyError:
            raise RuntimeError(
                f"Invalid input data type of {type(data)}, supported "
                f"types: {list(TYPE_TO_ENUM.keys())}"
            )

        if self._preprocessor:
            data = self._preprocessor.transform_batch(data)

        batch_format_to_use = self._batch_format_to_use()

        # We can finish prediction as long as one predict method is implemented.
        # For prediction, we have to return back in the same format as the input.
        if batch_format == BatchFormat.PANDAS:
            if batch_format_to_use == BatchFormat.PANDAS:
                return self._predict_pandas(
                    _convert_batch_type_to_pandas(data), **kwargs
                )
            elif batch_format_to_use == BatchFormat.NUMPY:
                return _convert_batch_type_to_pandas(
                    self._predict_numpy(_convert_batch_type_to_numpy(data), **kwargs)
                )
        elif batch_format == BatchFormat.NUMPY:
            if batch_format_to_use == BatchFormat.PANDAS:
                return _convert_batch_type_to_numpy(
                    self._predict_pandas(_convert_batch_type_to_pandas(data), **kwargs)
                )
            elif batch_format_to_use == BatchFormat.NUMPY:
                return self._predict_numpy(_convert_batch_type_to_numpy(data), **kwargs)

    @DeveloperAPI
    def _predict_pandas(self, data: "pd.DataFrame", **kwargs) -> "pd.DataFrame":
        """Perform inference on a Pandas DataFrame.

        Args:
            data: A pandas DataFrame to perform predictions on.
            kwargs: Arguments specific to the predictor implementation.

        Returns:
            A pandas DataFrame containing the prediction result.

        """
        raise NotImplementedError

    @DeveloperAPI
    def _predict_numpy(
        self, data: Union[np.ndarray, Dict[str, np.ndarray]], **kwargs
    ) -> Union[np.ndarray, Dict[str, np.ndarray]]:
        """Perform inference on a Numpy data.

        All Predictors working with tensor data (like deep learning predictors)
        should implement this method.

        Args:
            data: A Numpy ndarray or dictionary of ndarrays to perform predictions on.
            kwargs: Arguments specific to the predictor implementation.

        Returns:
            A Numpy ndarray or dictionary of ndarray containing the prediction result.

        """
        raise NotImplementedError

    def __reduce__(self):
        raise PredictorNotSerializableException(
            "Predictor instances are not serializable. Instead, you may want "
            "to serialize a checkpoint and initialize the Predictor with "
            "Predictor.from_checkpoint."
        )
