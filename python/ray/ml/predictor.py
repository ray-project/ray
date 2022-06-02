import abc

from ray.ml.checkpoint import Checkpoint
from ray.ml.utils.conversion_utils import (
    convert_data_batch_to_arrow,
    convert_arrow_to_data_batch,
    ArrowDataType,
    DataBatchType,
)

from ray.util.annotations import DeveloperAPI, PublicAPI


class PredictorNotSerializableException(RuntimeError):
    """Error raised when trying to serialize a Predictor instance."""

    pass


@DeveloperAPI
class Predictor(abc.ABC):
    """Predictors load models from checkpoints to perform inference."""

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

    @DeveloperAPI
    @abc.abstractmethod
    def _predict(self, data: ArrowDataType, **kwargs) -> ArrowDataType:
        raise NotImplementedError

    @PublicAPI(stability="alpha")
    def predict(self, data: DataBatchType, **kwargs) -> DataBatchType:
        """Perform inference on a batch of data.

        Args:
            data: A batch of input data. Either a pandas Dataframe or numpy
                array.
            kwargs: Arguments specific to predictor implementations. These are passed
                directly to _predict.

        Returns:
            DataBatchType: Prediction result.
        """
        if hasattr(self, "preprocessor") and self.preprocessor:
            data = self.preprocessor.transform_batch(data)

        arrow_data = convert_data_batch_to_arrow(data)
        predict_output = self._predict(arrow_data, **kwargs)

        return convert_arrow_to_data_batch(predict_output, type(data))

    def __reduce__(self):
        raise PredictorNotSerializableException(
            "Predictor instances are not serializable. Instead, you may want "
            "to serialize a checkpoint and initialize the Predictor with "
            "Predictor.from_checkpoint."
        )
