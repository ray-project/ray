import warnings
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
import torch

from ray.air.util.data_batch_conversion import _unwrap_ndarray_object_type_if_needed


def convert_pandas_to_torch_tensor(
    data_batch: pd.DataFrame,
    columns: Optional[Union[List[str], List[List[str]]]] = None,
    column_dtypes: Optional[Union[torch.dtype, List[torch.dtype]]] = None,
    unsqueeze: bool = True,
) -> Union[torch.Tensor, List[torch.Tensor]]:
    """Converts a Pandas dataframe to a torch Tensor or list of torch Tensors.

    The format of the return type will match the format of ``columns``. If a
    list of columns is provided, the return type will be a single tensor. If
    ``columns`` is a list of lists, then the return type will be a list of
    tensors.

    Args:
        data_batch: The pandas dataframe to convert to a
            torch tensor.
        columns:
            The names of the columns in the dataframe to include in the
            torch tensor. If this arg is a List[List[str]], then the return
            type will be a List of tensors. This is useful for multi-input
            models. If None, then use all columns in the ``data_batch``.
        column_dtype: The
            torch dtype to use for the tensor. If set to None,
            then automatically infer the dtype.
        unsqueeze: If set to True, the tensors
            will be unsqueezed (reshaped to (N, 1)) before being concatenated into
            the final tensor. Otherwise, they will be left as is, that is
            (N, ). Defaults to True.

    Returns:
        Either a torch tensor of size (N, len(columns)) where N is the
        number of rows in the ``data_batch`` Dataframe, or a list of
        tensors, where the size of item i is (N, len(columns[i])).

    """

    multi_input = columns and (isinstance(columns[0], (list, tuple)))

    if not multi_input and column_dtypes and type(column_dtypes) != torch.dtype:
        raise TypeError(
            "If `columns` is a list of strings, "
            "`column_dtypes` must be None or a single `torch.dtype`."
            f"Got {type(column_dtypes)} instead."
        )

    columns = columns if columns else []

    def tensorize(vals, dtype):
        """This recursive function allows to convert pyarrow List dtypes
        to multi-dimensional tensors."""
        if isinstance(vals, pd.api.extensions.ExtensionArray):
            # torch.as_tensor() does not yet support the __array__ protocol, so we need
            # to convert extension arrays to ndarrays manually before converting to a
            # Torch tensor.
            # See https://github.com/pytorch/pytorch/issues/51156.
            vals = vals.to_numpy()
        try:
            return torch.as_tensor(vals, dtype=dtype)
        except TypeError:
            # This exception will be raised if vals is of object dtype
            # or otherwise cannot be made into a tensor directly.
            # We assume it's a sequence in that case.
            # This is more robust than checking for dtype.
            tensors = [tensorize(x, dtype) for x in vals]
            try:
                return torch.stack(tensors)
            except RuntimeError:
                # NOTE: RuntimeError is raised when trying to stack ragged tensors.
                # Try to coerce the tensor to a nested tensor, if possible.
                # If this fails, the exception will be propagated up to the caller.
                return torch.nested_tensor(tensors)

    def get_tensor_for_columns(columns, dtype):
        feature_tensors = []

        if columns:
            batch = data_batch[columns]
        else:
            batch = data_batch

        for col in batch.columns:
            col_vals = batch[col].values
            try:
                t = tensorize(col_vals, dtype=dtype)
            except Exception:
                raise ValueError(
                    f"Failed to convert column {col} to a Torch Tensor of dtype "
                    f"{dtype}. See above exception chain for the exact failure."
                )
            if unsqueeze:
                t = t.unsqueeze(1)
            feature_tensors.append(t)

        if len(feature_tensors) > 1:
            feature_tensor = torch.cat(feature_tensors, dim=1)
        else:
            feature_tensor = feature_tensors[0]
        return feature_tensor

    if multi_input:
        if type(column_dtypes) not in [list, tuple]:
            column_dtypes = [column_dtypes] * len(columns)
        return [
            get_tensor_for_columns(columns=subcolumns, dtype=dtype)
            for subcolumns, dtype in zip(columns, column_dtypes)
        ]
    else:
        return get_tensor_for_columns(columns=columns, dtype=column_dtypes)


def convert_ndarray_to_torch_tensor(
    ndarray: np.ndarray,
    dtype: Optional[torch.dtype] = None,
    device: Optional[str] = None,
) -> torch.Tensor:
    """Convert a NumPy ndarray to a Torch Tensor.

    Args:
        ndarray: A NumPy ndarray that we wish to convert to a Torch Tensor.
        dtype: A Torch dtype for the created tensor; if None, the dtype will be
            inferred from the NumPy ndarray data.

    Returns: A Torch Tensor.
    """
    ndarray = _unwrap_ndarray_object_type_if_needed(ndarray)

    # The numpy array is not always writeable as it can come from the Ray object store.
    # Numpy will throw a verbose warning here, which we suppress, as we don't write
    # to the tensors. We also don't want to copy the array to avoid memory overhead.
    # Original warning: https://github.com/pytorch/pytorch/blob/v1.13.0/
    # torch/csrc/utils/tensor_numpy.cpp#L198-L206
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return torch.as_tensor(ndarray, dtype=dtype, device=device)


def convert_ndarray_batch_to_torch_tensor_batch(
    ndarrays: Union[np.ndarray, Dict[str, np.ndarray]],
    dtypes: Optional[Union[torch.dtype, Dict[str, torch.dtype]]] = None,
    device: Optional[str] = None,
) -> Union[torch.Tensor, Dict[str, torch.Tensor]]:
    """Convert a NumPy ndarray batch to a Torch Tensor batch.

    Args:
        ndarray: A (dict of) NumPy ndarray(s) that we wish to convert to a Torch Tensor.
        dtype: A (dict of) Torch dtype(s) for the created tensor; if None, the dtype
            will be inferred from the NumPy ndarray data.
        device: The device on which the tensor(s) should be placed; if None, the Torch
            tensor(s) will be constructed on the CPU.

    Returns: A (dict of) Torch Tensor(s).
    """
    if isinstance(ndarrays, np.ndarray):
        # Single-tensor case.
        if isinstance(dtypes, dict):
            if len(dtypes) != 1:
                raise ValueError(
                    "When constructing a single-tensor batch, only a single dtype "
                    f"should be given, instead got: {dtypes}"
                )
            dtypes = next(iter(dtypes.values()))
        batch = convert_ndarray_to_torch_tensor(ndarrays, dtype=dtypes, device=device)
    else:
        # Multi-tensor case.
        batch = {
            col_name: convert_ndarray_to_torch_tensor(
                col_ndarray,
                dtype=dtypes[col_name] if isinstance(dtypes, dict) else dtypes,
                device=device,
            )
            for col_name, col_ndarray in ndarrays.items()
        }

    return batch


def load_torch_model(
    saved_model: Union[torch.nn.Module, Dict],
    model_definition: Optional[torch.nn.Module] = None,
) -> torch.nn.Module:
    """Loads a PyTorch model from the provided ``saved_model``.

    ``model_definition`` is only used when ``saved_model`` is
    a torch state dict, which will be loaded into ``model_definition``.
    Otherwise, ``model_definition`` is discarded.
    """
    if isinstance(saved_model, torch.nn.Module):
        return saved_model
    elif isinstance(saved_model, dict):
        if not model_definition:
            raise ValueError(
                "Attempting to load torch model from a "
                "state_dict, but no `model_definition` was "
                "provided."
            )
        model_definition.load_state_dict(saved_model)
        return model_definition
    else:
        raise ValueError(
            f"Saved model is of type {type(saved_model)}. "
            f"The model saved in the checkpoint is expected "
            f"to be of type `torch.nn.Module`, or a model "
            f"state dict of type dict."
        )


def contains_tensor(obj):
    if isinstance(obj, torch.Tensor):
        return True
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if contains_tensor(k):
                return True
            if contains_tensor(v):
                return True
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            if contains_tensor(v):
                return True
    return False
