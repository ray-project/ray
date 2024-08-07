import warnings
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Union

import numpy as np

from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.data_batch_type import DataBatchType
from ray.util.annotations import Deprecated, DeveloperAPI

if TYPE_CHECKING:
    import pandas as pd

# TODO: Consolidate data conversion edges for arrow bug workaround.
try:
    import pyarrow
except ImportError:
    pyarrow = None

# Lazy import to avoid ray init failures without pandas installed and allow
# dataset to import modules in this file.
_pandas = None


def _lazy_import_pandas():
    global _pandas
    if _pandas is None:
        import pandas

        _pandas = pandas
    return _pandas


@DeveloperAPI
class BatchFormat(str, Enum):
    PANDAS = "pandas"
    # TODO: Remove once Arrow is deprecated as user facing batch format
    ARROW = "arrow"
    NUMPY = "numpy"  # Either a single numpy array or a Dict of numpy arrays.


@DeveloperAPI
class BlockFormat(str, Enum):
    """Internal Dataset block format enum."""

    PANDAS = "pandas"
    ARROW = "arrow"
    SIMPLE = "simple"


def _convert_batch_type_to_pandas(
    data: DataBatchType,
    cast_tensor_columns: bool = False,
) -> "pd.DataFrame":
    """Convert the provided data to a Pandas DataFrame.

    Args:
        data: Data of type DataBatchType
        cast_tensor_columns: Whether tensor columns should be cast to NumPy ndarrays.

    Returns:
        A pandas Dataframe representation of the input data.

    """
    pd = _lazy_import_pandas()

    if isinstance(data, np.ndarray):
        data = pd.DataFrame({TENSOR_COLUMN_NAME: _ndarray_to_column(data)})
    elif isinstance(data, dict):
        tensor_dict = {}
        for col_name, col in data.items():
            if not isinstance(col, np.ndarray):
                raise ValueError(
                    "All values in the provided dict must be of type "
                    f"np.ndarray. Found type {type(col)} for key {col_name} "
                    f"instead."
                )
            tensor_dict[col_name] = _ndarray_to_column(col)
        data = pd.DataFrame(tensor_dict)
    elif pyarrow is not None and isinstance(data, pyarrow.Table):
        data = data.to_pandas()
    elif not isinstance(data, pd.DataFrame):
        raise ValueError(
            f"Received data of type: {type(data)}, but expected it to be one "
            f"of {DataBatchType}"
        )
    if cast_tensor_columns:
        data = _cast_tensor_columns_to_ndarrays(data)
    return data


def _convert_pandas_to_batch_type(
    data: "pd.DataFrame",
    type: BatchFormat,
    cast_tensor_columns: bool = False,
) -> DataBatchType:
    """Convert the provided Pandas dataframe to the provided ``type``.

    Args:
        data: A Pandas DataFrame
        type: The specific ``BatchFormat`` to convert to.
        cast_tensor_columns: Whether tensor columns should be cast to our tensor
            extension type.

    Returns:
        The input data represented with the provided type.
    """
    if cast_tensor_columns:
        data = _cast_ndarray_columns_to_tensor_extension(data)
    if type == BatchFormat.PANDAS:
        return data

    elif type == BatchFormat.NUMPY:
        if len(data.columns) == 1:
            # If just a single column, return as a single numpy array.
            return data.iloc[:, 0].to_numpy()
        else:
            # Else return as a dict of numpy arrays.
            output_dict = {}
            for column in data:
                output_dict[column] = data[column].to_numpy()
            return output_dict

    elif type == BatchFormat.ARROW:
        if not pyarrow:
            raise ValueError(
                "Attempted to convert data to Pyarrow Table but Pyarrow "
                "is not installed. Please do `pip install pyarrow` to "
                "install Pyarrow."
            )
        return pyarrow.Table.from_pandas(data)

    else:
        raise ValueError(
            f"Received type {type}, but expected it to be one of {DataBatchType}"
        )


@Deprecated
def convert_batch_type_to_pandas(
    data: DataBatchType,
    cast_tensor_columns: bool = False,
):
    """Convert the provided data to a Pandas DataFrame.

    This API is deprecated from Ray 2.4.

    Args:
        data: Data of type DataBatchType
        cast_tensor_columns: Whether tensor columns should be cast to NumPy ndarrays.

    Returns:
        A pandas Dataframe representation of the input data.

    """
    warnings.warn(
        "`convert_batch_type_to_pandas` is deprecated as a developer API "
        "starting from Ray 2.4. All batch format conversions should be "
        "done manually instead of relying on this API.",
        PendingDeprecationWarning,
    )
    return _convert_batch_type_to_pandas(
        data=data, cast_tensor_columns=cast_tensor_columns
    )


@Deprecated
def convert_pandas_to_batch_type(
    data: "pd.DataFrame",
    type: BatchFormat,
    cast_tensor_columns: bool = False,
):
    """Convert the provided Pandas dataframe to the provided ``type``.

    Args:
        data: A Pandas DataFrame
        type: The specific ``BatchFormat`` to convert to.
        cast_tensor_columns: Whether tensor columns should be cast to our tensor
            extension type.

    Returns:
        The input data represented with the provided type.
    """
    warnings.warn(
        "`convert_pandas_to_batch_type` is deprecated as a developer API "
        "starting from Ray 2.4. All batch format conversions should be "
        "done manually instead of relying on this API.",
        PendingDeprecationWarning,
    )
    return _convert_pandas_to_batch_type(
        data=data, type=type, cast_tensor_columns=cast_tensor_columns
    )


def _convert_batch_type_to_numpy(
    data: DataBatchType,
) -> Union[np.ndarray, Dict[str, np.ndarray]]:
    """Convert the provided data to a NumPy ndarray or dict of ndarrays.

    Args:
        data: Data of type DataBatchType

    Returns:
        A numpy representation of the input data.
    """
    pd = _lazy_import_pandas()

    if isinstance(data, np.ndarray):
        return data
    elif isinstance(data, dict):
        for col_name, col in data.items():
            if not isinstance(col, np.ndarray):
                raise ValueError(
                    "All values in the provided dict must be of type "
                    f"np.ndarray. Found type {type(col)} for key {col_name} "
                    f"instead."
                )
        return data
    elif pyarrow is not None and isinstance(data, pyarrow.Table):
        from ray.air.util.tensor_extensions.arrow import ArrowTensorType
        from ray.air.util.transform_pyarrow import (
            _concatenate_extension_column,
            _is_column_extension_type,
        )

        if data.column_names == [TENSOR_COLUMN_NAME] and (
            isinstance(data.schema.types[0], ArrowTensorType)
        ):
            # If representing a tensor dataset, return as a single numpy array.
            # Example: ray.data.from_numpy(np.arange(12).reshape((3, 2, 2)))
            # Arrow’s incorrect concatenation of extension arrays:
            # https://issues.apache.org/jira/browse/ARROW-16503
            return _concatenate_extension_column(data[TENSOR_COLUMN_NAME]).to_numpy(
                zero_copy_only=False
            )
        else:
            output_dict = {}
            for col_name in data.column_names:
                col = data[col_name]
                if col.num_chunks == 0:
                    col = pyarrow.array([], type=col.type)
                elif _is_column_extension_type(col):
                    # Arrow’s incorrect concatenation of extension arrays:
                    # https://issues.apache.org/jira/browse/ARROW-16503
                    col = _concatenate_extension_column(col)
                else:
                    col = col.combine_chunks()
                output_dict[col_name] = col.to_numpy(zero_copy_only=False)
            return output_dict
    elif isinstance(data, pd.DataFrame):
        return _convert_pandas_to_batch_type(data, BatchFormat.NUMPY)
    else:
        raise ValueError(
            f"Received data of type: {type(data)}, but expected it to be one "
            f"of {DataBatchType}"
        )


def _ndarray_to_column(arr: np.ndarray) -> Union["pd.Series", List[np.ndarray]]:
    """Convert a NumPy ndarray into an appropriate column format for insertion into a
    pandas DataFrame.

    If conversion to a pandas Series fails (e.g. if the ndarray is multi-dimensional),
    fall back to a list of NumPy ndarrays.
    """
    pd = _lazy_import_pandas()
    try:
        # Try to convert to Series, falling back to a list conversion if this fails
        # (e.g. if the ndarray is multi-dimensional).
        return pd.Series(arr)
    except ValueError:
        return list(arr)


def _unwrap_ndarray_object_type_if_needed(arr: np.ndarray) -> np.ndarray:
    """Unwrap an object-dtyped NumPy ndarray containing ndarray pointers into a single
    contiguous ndarray, if needed/possible.
    """
    if arr.dtype.type is np.object_:
        try:
            # Try to convert the NumPy ndarray to a non-object dtype.
            arr = np.array([np.asarray(v) for v in arr])
        except Exception:
            # This may fail if the subndarrays are of heterogeneous shape
            pass
    return arr


def _cast_ndarray_columns_to_tensor_extension(df: "pd.DataFrame") -> "pd.DataFrame":
    """
    Cast all NumPy ndarray columns in df to our tensor extension type, TensorArray.
    """
    pd = _lazy_import_pandas()
    try:
        SettingWithCopyWarning = pd.core.common.SettingWithCopyWarning
    except AttributeError:
        # SettingWithCopyWarning was moved to pd.errors in Pandas 1.5.0.
        SettingWithCopyWarning = pd.errors.SettingWithCopyWarning

    from ray.air.util.tensor_extensions.pandas import (
        TensorArray,
        column_needs_tensor_extension,
    )

    # Try to convert any ndarray columns to TensorArray columns.
    # TODO(Clark): Once Pandas supports registering extension types for type
    # inference on construction, implement as much for NumPy ndarrays and remove
    # this. See https://github.com/pandas-dev/pandas/issues/41848
    # TODO(Clark): Optimize this with propagated DataFrame metadata containing a list of
    # column names containing tensor columns, to make this an O(# of tensor columns)
    # check rather than the current O(# of columns) check.
    for col_name, col in df.items():
        if column_needs_tensor_extension(col):
            try:
                # Suppress Pandas warnings:
                # https://github.com/ray-project/ray/issues/29270
                # We actually want in-place operations so we surpress this warning.
                # https://stackoverflow.com/a/74193599
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=FutureWarning)
                    warnings.simplefilter("ignore", category=SettingWithCopyWarning)
                    df[col_name] = TensorArray(col)
            except Exception as e:
                raise ValueError(
                    f"Tried to cast column {col_name} to the TensorArray tensor "
                    "extension type but the conversion failed. To disable "
                    "automatic casting to this tensor extension, set "
                    "ctx = DataContext.get_current(); "
                    "ctx.enable_tensor_extension_casting = False."
                ) from e
    return df


def _cast_tensor_columns_to_ndarrays(df: "pd.DataFrame") -> "pd.DataFrame":
    """Cast all tensor extension columns in df to NumPy ndarrays."""
    pd = _lazy_import_pandas()
    try:
        SettingWithCopyWarning = pd.core.common.SettingWithCopyWarning
    except AttributeError:
        # SettingWithCopyWarning was moved to pd.errors in Pandas 1.5.0.
        SettingWithCopyWarning = pd.errors.SettingWithCopyWarning
    from ray.air.util.tensor_extensions.pandas import TensorDtype

    # Try to convert any tensor extension columns to ndarray columns.
    # TODO(Clark): Optimize this with propagated DataFrame metadata containing a list of
    # column names containing tensor columns, to make this an O(# of tensor columns)
    # check rather than the current O(# of columns) check.
    for col_name, col in df.items():
        if isinstance(col.dtype, TensorDtype):
            # Suppress Pandas warnings:
            # https://github.com/ray-project/ray/issues/29270
            # We actually want in-place operations so we surpress this warning.
            # https://stackoverflow.com/a/74193599
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=FutureWarning)
                warnings.simplefilter("ignore", category=SettingWithCopyWarning)
                df[col_name] = list(col.to_numpy())
    return df
