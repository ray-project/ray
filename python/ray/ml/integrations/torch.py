from typing import Optional, Union, Dict, List

import numpy as np
import pandas as pd
import torch

from ray.ml.predictor import Predictor, DataBatchType
from ray.ml.constants import PREPROCESSOR_KEY
from ray.ml.constants import MODEL_KEY
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint


def convert_pandas_to_torch_tensor(
    data_batch: pd.DataFrame,
    columns: Optional[Union[List[str], List[List[str]]]] = None,
    column_dtypes: Optional[Union[torch.dtype, List[torch.dtype]]] = None,
) -> Union[torch.Tensor, List[torch.Tensor]]:

    """Converts a Pandas dataframe to a torch Tensor or list of torch Tensors.

    The format of the return type will match the format of ``columns``. If a
    list of columns is provided, the return type will be a single tensor. If
    ``columns`` is a list of list, then the return type will be a list of
    tensors.

    Args:
        data_batch (pandas.DataFrame): The pandas dataframe to conver to a
            torch tensor.
        columns (Optional[Union[List[str], List[List[str]]]):
            The names of the columns in the dataframe to include in the
            torch tensor. If this arg is a List[List[str]], then the return
            type will be a List of tensors. This is useful for multi-input
            models. If None, then use all columns in the ``data_batch``.
        column_dtype (Optional[Union[torch.dtype, List[torch.dtype]): The
            torch dtype to use for the tensor. If set to None,
            then automatically
            infer the dtype.

    Returns:
        Either a torch tensor of size (N, len(columns)) where N is the
        number of rows in the ``data_batch`` Dataframe, or a list of
        tensors, where the size of item i is (N, len(columns[i])).

    """

    multi_input = columns and (isinstance(columns[0], (list, tuple)))

    if multi_input and column_dtypes:
        if not type(column_dtypes) not in [torch.dtype, list, tuple]:
            raise TypeError(
                "If `columns` is a list of lists, "
                "`column_dtypes` must be None, `torch.dtype`,"
                f" or a sequence, got {type(column_dtypes)}."
            )

    if (
        not multi_input
        and column_dtypes
        and type(column_dtypes) not in [torch.dtype, list, tuple]
    ):
        raise TypeError(
            "If `columns` is a list of strings, "
            "`column_dtypes` must be None or a single `torch.dtype`."
            f"Got {type(column_dtypes)} instead."
        )

    def get_tensor_for_columns(columns, dtype):
        feature_tensors = []

        if columns:
            batch = data_batch[columns]
        else:
            batch = data_batch

        for col in batch.columns:
            col_vals = batch[col].values
            t = torch.as_tensor(col_vals, dtype=dtype)
            t = t.view(-1, 1)
            feature_tensors.append(t)

        return torch.cat(feature_tensors, dim=1)

    if multi_input:
        if isinstance(column_dtypes, torch.dtype):
            column_dtypes = [column_dtypes] * len(columns)
        return [
            get_tensor_for_columns(columns=subcolumns, dtype=dtype)
            for subcolumns, dtype in zip(columns, column_dtypes)
        ]
    else:
        return get_tensor_for_columns(columns=columns, dtype=column_dtypes)


class TorchPredictor(Predictor):
    """A predictor for PyTorch models."""

    def __init__(self, model: torch.nn.Module, preprocessor: Preprocessor):
        self.model = model
        self.model.eval()
        self.preprocessor = preprocessor

    @classmethod
    def from_checkpoint(
        cls, checkpoint: Checkpoint, model_definition: Optional[torch.nn.Module] = None
    ) -> "TorchPredictor":
        """Instantiate the predictor from a Checkpoint.

        The checkpoint is expected to be a result of ``TorchTrainer``.

        Args:
            checkpoint (Checkpoint): The checkpoint to load the model and
                preprocessor from. It is expected to be from the result of a
                ``TorchTrainer`` run.
            model_definition (Optional[torch.nn.Module]): If checkpoints
                contains the model state dict, and not the model itself,
                then the state dict will be loaded to the model_definition.

        """
        checkpoint_dict = checkpoint.to_dict()
        preprocessor = checkpoint_dict[PREPROCESSOR_KEY]
        model = _load_torch_model(
            saved_model=checkpoint_dict[MODEL_KEY], model_definition=model_definition
        )
        return TorchPredictor(model=model, preprocessor=preprocessor)

    def predict(
        self,
        data: DataBatchType,
        column_names: Optional[List[str]] = None,
        feature_columns: Optional[Union[List[str], List[List[str]]]] = None,
        dtype: Optional[Union[torch.dtype, List[torch.dtype]]] = None,
    ) -> DataBatchType:
        """Run inference on data batch.

        Args:
            data (DataBatchType): Input data.
            columns_names (Optional[List[str]]): If provided, the column
                names to set for the data batch.
            feature_columns (Optional[Union[List[str], List[List[str]]]]):
                The names of the columns in the dataframe to use as
                features to predict on. If this arg is a List[List[str]],
                then the data batch will be converted into a list of tensors.
                This is useful for multi-input
                models. If None, then use all columns in the ``data_batch``.
            dtype (Optional[Union[torch.dtype, List[torch.dtype]]]): The
                torch dtype to use for the tensor. If set to None,
                then automatically infer the dtype.

        Returns:
            DataBatchType: Prediction result.

        """
        if isinstance(data, np.ndarray):
            # If numpy array, then convert to pandas dataframe.
            data = pd.DataFrame(data)

        if column_names:
            data.columns = column_names
        tensor = convert_pandas_to_torch_tensor(
            data, columns=feature_columns, column_dtypes=dtype
        )
        prediction = self.model(tensor).cpu().detach().numpy()
        return pd.DataFrame(prediction, columns=["predictions"])


def _load_torch_model(
    saved_model: Union[torch.nn.Module, Dict],
    model_definition: Optional[torch.nn.Module] = None,
) -> torch.nn.Module:
    if isinstance(saved_model, torch.nn.Module):
        return saved_model
    elif isinstance(saved_model, dict):
        if not model_definition:
            raise RuntimeError(
                "Attempting to load torch model from a "
                "state_dict, but no `model_definition` was "
                "provided."
            )
        model_definition.load_state_dict(saved_model)
        return model_definition
    else:
        raise RuntimeError(
            f"Saved model is of type {type(saved_model)}. "
            f"The model saved in the checkpoint is expected "
            f"to be of type `torch.nn.Module`, or a model "
            f"state dict of type dict."
        )
