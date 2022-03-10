from typing import Optional, Union, List

import torch

from ray.ml.predictor import Predictor, DataBatchType
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint


class TorchPredictor(Predictor):
    """A predictor for PyTorch models.

    Args:
        model: The torch module to use for predictions.
        preprocessor: The preprocessor used to transform data batches prior
            to prediction.

    """

    def __init__(self, model: torch.nn.Module, preprocessor: Preprocessor):
        raise NotImplementedError

    @classmethod
    def from_checkpoint(
        cls, checkpoint: Checkpoint, model_definition: Optional[torch.nn.Module] = None
    ) -> "TorchPredictor":
        """Instantiate the predictor from a Checkpoint.

        The checkpoint is expected to be a result of ``TorchTrainer``.

        Args:
            checkpoint: The checkpoint to load the model and
                preprocessor from. It is expected to be from the result of a
                ``TorchTrainer`` run.
            model_definition: If checkpoints
                contains the model state dict, and not the model itself,
                then the state dict will be loaded to the model_definition.

        """
        raise NotImplementedError

    def predict(
        self,
        data: DataBatchType,
        column_names: Optional[List[str]] = None,
        feature_columns: Optional[Union[List[str], List[List[str]]]] = None,
        dtype: Optional[Union[torch.dtype, List[torch.dtype]]] = None,
    ) -> DataBatchType:
        """Run inference on data batch.

        Args:
            data: A batch of input data.
            columns_names: If provided, the column names to set for the data
                batch.
            feature_columns: The names of the columns in the dataframe to
                use as features to predict on. If this arg is a List[List[
                str]], then the data batch will be converted into a list of
                tensors. This is useful for multi-input models. If None,
                then use all columns in the ``data_batch``.
            dtype: The torch dtype to use for the tensor. If set to None,
                then automatically infer the dtype.

        Returns:
            DataBatchType: Prediction result.

        """
        raise NotImplementedError
