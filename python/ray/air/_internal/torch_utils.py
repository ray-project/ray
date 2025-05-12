import warnings
from typing import Any, Dict, List, Optional, Union, Sequence
from collections.abc import Mapping, Sequence as ABCSequence

import numpy as np
import pandas as pd
import torch
import pyarrow

from ray.air._internal.device_manager import get_torch_device_manager_by_context
from ray.air.util.data_batch_conversion import _unwrap_ndarray_object_type_if_needed
from ray.data.collate_fn import TensorBatchType, TensorBatchReturnType


def get_devices() -> List[torch.device]:
    """Gets the correct torch device list configured for this process.

    Returns a list of torch accelerator (GPU, HPU, NPU...) devices allocated for
    the current worker.
    If no accelerators are assigned, then it returns a list with a single CPU device.
    """
    return get_torch_device_manager_by_context().get_devices()


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

    if not multi_input and column_dtypes and not isinstance(column_dtypes, torch.dtype):
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

        if vals.dtype.type is np.object_:
            # Column has an object dtype which Torch can't handle, so we try to
            # tensorize each column element and then stack the resulting tensors.
            tensors = [tensorize(x, dtype) for x in vals]
            try:
                return torch.stack(tensors)
            except RuntimeError:
                # NOTE: RuntimeError is raised when trying to stack ragged tensors.
                # Try to coerce the tensor to a nested tensor, if possible.
                # If this fails, the exception will be propagated up to the caller.
                return torch.nested_tensor(tensors)
        else:
            return torch.as_tensor(vals, dtype=dtype)

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
            except Exception as e:
                raise ValueError(
                    f"Failed to convert column {col} to a Torch Tensor of dtype "
                    f"{dtype}. See above exception chain for the exact failure."
                ) from e
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
    device: Optional[Union[str, "torch.device"]] = None,
) -> torch.Tensor:
    """Convert a NumPy ndarray to a Torch Tensor.

    Args:
        ndarray: A NumPy ndarray that we wish to convert to a Torch Tensor.
        dtype: A Torch dtype for the created tensor; if None, the dtype will be
            inferred from the NumPy ndarray data.
        device: The device on which the tensor(s) should be placed; if None, the Torch
            tensor(s) will be constructed on the CPU.

    Returns: A Torch Tensor.
    """
    ndarray = _unwrap_ndarray_object_type_if_needed(ndarray)

    # Object dtype cannot be converted into PyTorch Tensor.
    if ndarray.dtype.type is np.object_:
        raise RuntimeError(
            "Numpy array of object dtype cannot be converted to a Torch Tensor. This "
            "may because the numpy array is a ragged tensor--it contains items of "
            "different sizes. If using `iter_torch_batches()` API, you can pass in a "
            "`collate_fn` argument to specify custom logic to convert the Numpy array "
            "batch to a Torch tensor batch."
        )

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
    device: Optional[Union[str, "torch.device"]] = None,
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


# Not present in torch<=1.7.0
# Adapted from https://github.com/pytorch/pytorch/blob/\
# c18da597e0bb1c1aecc97c77a73fed1849057fa4/torch/nn/modules/utils.py
def consume_prefix_in_state_dict_if_present_not_in_place(
    state_dict: Dict[str, Any], prefix: str
) -> Dict[str, Any]:
    """Strip the prefix in state_dict, if any and return a new dict.

    Adapted from https://github.com/pytorch/pytorch/blob/\
c18da597e0bb1c1aecc97c77a73fed1849057fa4/torch/nn/modules/utils.py
    The original method modified the dict in-place.

    Args:
        state_dict: a state-dict to be loaded to the model.
        prefix: prefix.

    """
    copied = False

    for key in state_dict:
        if key.startswith(prefix):
            newkey = key[len(prefix) :]
            if not copied:
                # We are doing shallow copies here, so the performance
                # impact should be negligible anyway, but this is
                # a simple optimization.
                state_dict = state_dict.copy()
                copied = True
            state_dict[newkey] = state_dict.pop(key)

    if "_metadata" in state_dict:
        state_dict["_metadata"] = state_dict["_metadata"].copy()
        metadata = state_dict["_metadata"]
        for key in metadata:
            if len(key) == 0:
                continue
            newkey = key[len(prefix) :]
            metadata[newkey] = metadata.pop(key)

    return state_dict


def convert_ndarray_list_to_torch_tensor_list(
    ndarrays: Dict[str, List[np.ndarray]],
    dtypes: Optional[Union[torch.dtype, Dict[str, torch.dtype]]] = None,
    device: Optional[Union[str, "torch.device"]] = None,
) -> Dict[str, List[torch.Tensor]]:
    """Convert a dict mapping column names to lists of ndarrays to Torch Tensors.

    Args:
        ndarrays: A dict mapping column names to lists of ndarrays that we wish to convert
            to Torch Tensors.
        dtypes: A (dict of) Torch dtype(s) for the created tensors; if None, the dtype
            will be inferred from the NumPy ndarray data.
        device: The device on which the tensor(s) should be placed; if None, the Torch
            tensor(s) will be constructed on the CPU.

    Returns: A dict mapping column names to lists of Tensors.
    """
    return {
        col_name: [
            convert_ndarray_batch_to_torch_tensor_batch(
                ndarray,
                dtypes=dtypes[col_name] if isinstance(dtypes, dict) else dtypes,
                device=device,
            )
            for ndarray in col_ndarrays
        ]
        for col_name, col_ndarrays in ndarrays.items()
    }


def arrow_batch_to_tensors(
    batch: pyarrow.Table,
    dtypes: Optional[Union[torch.dtype, Dict[str, torch.dtype]]] = None,
    combine_chunks: bool = False,
) -> Dict[str, List[torch.Tensor]]:
    """Convert PyArrow batch to PyTorch tensors.

    Args:
        batch: PyArrow batch to convert
        dtypes: A (dict of) Torch dtype(s) for the created tensors; if None, the dtype
            will be inferred from the NumPy ndarray data.
        combine_chunks: If True, combine chunks in Arrow batch before converting to
            tensors.

    Returns:
        A dictionary of column name to list of tensors. For non-chunked columns,
        the list will contain a single tensor.
    """
    from ray.data._internal.arrow_ops import transform_pyarrow
    from ray.data._internal.arrow_block import ArrowBlockAccessor

    if combine_chunks:
        numpy_batch = ArrowBlockAccessor(batch).to_batch_format("numpy")
        return {
            col_name: convert_ndarray_batch_to_torch_tensor_batch(
                col_array,
                dtypes=dtypes[col_name] if isinstance(dtypes, dict) else dtypes,
            )
            for col_name, col_array in numpy_batch.items()
        }
    else:
        numpy_list = transform_pyarrow.table_to_numpy_dict_chunked(
            batch,
        )
        return convert_ndarray_list_to_torch_tensor_list(
            numpy_list,
            dtypes=dtypes,
        )


@torch.no_grad()
def concat_tensors_to_device(
    tensor_sequence: Sequence[torch.Tensor],
    device: Optional[Union[str, "torch.device"]] = None,
    non_blocking: bool = False,
) -> torch.Tensor:
    """Stack sequence of tensors into a contiguous GPU tensor.

    Args:
        tensor_sequence: Sequence of tensors to stack
        device: The device to move tensors to
        non_blocking: If True, perform device transfer without forcing a
            synchronization.

    Returns:
        A contiguous tensor on the target device
    """
    # Assumes tensors have the same shape/dtype
    assert tensor_sequence, "Cannot stack empty sequence of tensors"
    assert all(
        isinstance(t, torch.Tensor) for t in tensor_sequence
    ), "All items must be torch.Tensor"
    assert all(
        t.dtype == tensor_sequence[0].dtype for t in tensor_sequence
    ), "All tensors must have the same dtype"
    assert all(
        t.shape[1:] == tensor_sequence[0].shape[1:] for t in tensor_sequence
    ), "All tensors must have the same shape[1:]"

    first = tensor_sequence[0]
    dtype = first.dtype
    shape_tail = first.shape[1:]
    total_rows = sum(t.shape[0] for t in tensor_sequence)

    # Allocate an empty Tensor on device
    result = torch.empty((total_rows, *shape_tail), dtype=dtype, device=device)

    row_start = 0
    for t in tensor_sequence:
        row_end = row_start + t.shape[0]
        result[row_start:row_end].copy_(t, non_blocking=non_blocking)
        row_start = row_end

    assert isinstance(result, torch.Tensor), "Result must be a torch.Tensor"
    return result


@torch.no_grad()
def move_tensors_to_device(
    batch: TensorBatchType,
    device: Optional[Union[str, "torch.device"]] = None,
    non_blocking: bool = False,
) -> TensorBatchReturnType:
    """Move tensors to the specified device.

    Args:
        batch: A tensor or collection of tensors to move to device. Can be:
            - A single tensor
            - A sequence of tensors
            - A sequence of sequences of tensors. The inner sequence of tensors is
              combined during GPU transfer.
            - A mapping (e.g., dict) of keys to tensors or sequences of tensors. The
              sequence of tensors is combined during GPU transfer.
        device: The device to move tensors to. If None, tensors are not moved.
        non_blocking: If True, perform device transfer without forcing a
            synchronization.

    Returns:
        The input tensors moved to the specified device
    """
    if device is None:
        return batch

    if isinstance(batch, torch.Tensor):
        return batch.to(device=device, non_blocking=non_blocking)

    elif isinstance(batch, Mapping):
        new_batch = {}
        for k, v in batch.items():
            if isinstance(v, torch.Tensor):
                new_batch[k] = v.to(device=device, non_blocking=non_blocking)

            elif isinstance(v, ABCSequence) and not isinstance(v, (str, bytes)):
                if all(isinstance(t, torch.Tensor) for t in v):
                    new_batch[k] = concat_tensors_to_device(
                        v, device=device, non_blocking=non_blocking
                    )
                else:
                    raise TypeError(
                        f"Expected a sequence of torch.Tensor for key '{k}', "
                        f"but got sequence of types: {[type(t) for t in v]}"
                    )

            else:
                raise TypeError(
                    f"Unsupported type for key '{k}': expected torch.Tensor or "
                    f"sequence of torch.Tensor, but got {type(v)}"
                )
        return new_batch

    elif isinstance(batch, ABCSequence) and not isinstance(batch, (str, bytes)):
        if all(isinstance(t, torch.Tensor) for t in batch):
            return concat_tensors_to_device(
                batch, device=device, non_blocking=non_blocking
            )

        elif all(
            isinstance(seq, ABCSequence)
            and not isinstance(seq, (str, bytes))
            and all(isinstance(t, torch.Tensor) for t in seq)
            for seq in batch
        ):
            return tuple(
                concat_tensors_to_device(seq, device=device, non_blocking=non_blocking)
                for seq in batch
            )

        else:
            sample_type = type(batch[0]) if batch else "empty"
            raise TypeError(
                f"Unsupported sequence structure. Got: {type(batch)} with inner type "
                f"{sample_type}"
            )

    else:
        raise TypeError(
            f"Batch must be one of {TensorBatchType} types, got {type(batch)}"
        )
