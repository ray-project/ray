from typing import Optional, Union, List, Dict, Any

import pandas as pd


def convert_pandas_to_jax_tensor(
    data_batch: pd.DataFrame,
    columns: Optional[Union[List[str], List[List[str]]]] = None,
    column_dtypes: Optional[Union[Any, List[Any]]] = None,
    unsqueeze: bool = True,
) -> Union[Any, List[Any]]:
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

    # ignore the type for now!
    # if not multi_input and column_dtypes and type(column_dtypes) != jax.dtype:
    #     raise TypeError(
    #         "If `columns` is a list of strings, "
    #         "`column_dtypes` must be None or a single `jax.dtype`."
    #         f"Got {type(column_dtypes)} instead."
    #     )

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
            return jax.stack([tensorize(x, dtype) for x in vals])

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


# def load_jax_model(
#     saved_model: Union[jax.nn.Module, Dict],
#     model_definition: Optional[jax.nn.Module] = None,
# ) -> jax.nn.Module:
#     """Loads a jax model from the provided ``saved_model``.

#     If ``saved_model`` is a jax Module, then return it directly. If ``saved_model`` is
#     a jax state dict, then load it in the ``model_definition`` and return the loaded
#     model.
#     """
#     if isinstance(saved_model, jax.nn.Module):
#         return saved_model
#     elif isinstance(saved_model, dict):
#         if not model_definition:
#             raise ValueError(
#                 "Attempting to load jax model from a "
#                 "state_dict, but no `model_definition` was "
#                 "provided."
#             )
#         model_definition.load_state_dict(saved_model)
#         return model_definition
#     else:
#         raise ValueError(
#             f"Saved model is of type {type(saved_model)}. "
#             f"The model saved in the checkpoint is expected "
#             f"to be of type `jax.nn.Module`, or a model "
#             f"state dict of type dict."
#         )
