import abc
from typing import Union, TYPE_CHECKING, Any

from ray.ml.checkpoint import Checkpoint
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd

DataBatchType = Union["pd.DataFrame", "np.ndarray"]

from ray.ml.predictor import DataBatchType

class TypeConverter:
    def __init__
    def convert_to_arrow(self, data: ) -> pyarrow.Table:
        pass

    def convert_from_arrow(self, arrow_table):


class PredictorNotSerializableException(RuntimeError):
    """Error raised when trying to serialize a Predictor instance."""

    pass

@DeveloperAPI
class Predictor(abc.ABC):
    """Predictors load models from checkpoints to perform inference."""

    MODEL_INPUT_TYPE = object
    MODEL_OUTPUT_TYPE = object

    @PublicAPI(stability="alpha")
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

    # @DeveloperAPI
    # @abc.abstractmethod
    # def _predict(self, data: MODEL_INPUT_TYPE, **kwargs) -> MODEL_OUTPUT_TYPE:
    #     raise NotImplementedError


    @PublicAPI(stability="alpha")
    def predict(self, data: DataBatchType, **kwargs) -> DataBatchType:
        """Perform inference on a batch of data.

        When implementing a custom Predictor, you should set the `MODEL_INPUT_TYPE`
        and `MODEL_OUTPUT_TYPE` class attributes for your predictor class and override
        `_predict` method. This `predict` method should not be overridden.

        Args:
            data: A batch of input data. Either a pandas Dataframe or numpy
                array.
            kwargs: Arguments specific to predictor implementations. These are passed
            directly

        Returns:
            DataBatchType: Prediction result.
        """
        # if hasattr(self, "preprocessor") and self.preprocessor:
        #     data = self.preprocessor.transform_batch(data)
        pass



    def __reduce__(self):
        raise PredictorNotSerializableException(
            "Predictor instances are not serializable. Instead, you may want "
            "to serialize a checkpoint and initialize the Predictor with "
            "Predictor.from_checkpoint."
        )
