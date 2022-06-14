from typing import Optional, Union, List, Dict, Any, TYPE_CHECKING
if TYPE_CHECKING:
    import jax

import pandas as pd


def convert_pandas_to_jax_tensor(
    data_batch: pd.DataFrame,
    columns: Optional[Union[List[str], List[List[str]]]] = None,
    column_dtypes: Optional[Union["jax.dtype", List["jax.dtype"]]] = None,
    unsqueeze: bool = True,
) -> Union["jax.dtype", List["jax.dtype"]]:
    """Converts a Pandas dataframe to a jax Tensor or list of jax Tensors.

    The format of the return type will match the format of ``columns``. If a
    list of columns is provided, the return type will be a single tensor. If
    ``columns`` is a list of lists, then the return type will be a list of
    tensors.

    Args:
        data_batch (pandas.DataFrame): The pandas dataframe to convert to a
            jax tensor.
        columns (Optional[Union[List[str], List[List[str]]]):
            The names of the columns in the dataframe to include in the
            jax tensor. If this arg is a List[List[str]], then the return
            type will be a List of tensors. This is useful for multi-input
            models. If None, then use all columns in the ``data_batch``.
        column_dtype (Optional[Union[jax.dtype, List[jax.dtype]): The
            jax dtype to use for the tensor. If set to None,
            then automatically infer the dtype.
        unsqueeze: If set to True, the tensors
            will be unsqueezed (reshaped to (N, 1)) before being concatenated into
            the final tensor. Otherwise, they will be left as is, that is
            (N, ). Defaults to True.

    Returns:
        Either a jax tensor of size (N, len(columns)) where N is the
        number of rows in the ``data_batch`` Dataframe, or a list of
        tensors, where the size of item i is (N, len(columns[i])).

    """

    import jax 
    import jax.numpy as jnp

    multi_input = columns and (isinstance(columns[0], (list, tuple)))

    columns = columns if columns else []

    def tensorize(vals, dtype):
        """This recursive function allows to convert pyarrow List dtypes
        to multi-dimensional tensors."""
        if isinstance(vals, pd.api.extensions.ExtensionArray):
            # jax.as_tensor() does not yet support the __array__ protocol, so we need
            # to convert extension arrays to ndarrays manually before converting to a
            # jax tensor.
            # See https://github.com/jax/jax/issues/51156.
            vals = vals.to_numpy()
        try:
            return jax.numpy.asarray(vals, dtype=dtype)
        except TypeError:
            # This exception will be raised if vals is of object dtype
            # or otherwise cannot be made into a tensor directly.
            # We assume it's a sequence in that case.
            # This is more robust than checking for dtype.
            return jax.numpy.stack([tensorize(x, dtype) for x in vals])

    def get_tensor_for_columns(columns, dtype):
        feature_tensors = []

        if columns:
            batch = data_batch[columns]
        else:
            batch = data_batch

        for col in batch.columns:
            col_vals = batch[col].values
            t = tensorize(col_vals, dtype=dtype)
            if unsqueeze:
                t = t.unsqueeze(1)
            feature_tensors.append(t)

        if len(feature_tensors) > 1:
            feature_tensor = jnp.concatenate(feature_tensors, axis=1)
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
