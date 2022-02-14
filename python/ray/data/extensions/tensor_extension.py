# Adapted from
# https://github.com/CODAIT/text-extensions-for-pandas/blob/dc03278689fe1c5f131573658ae19815ba25f33e/text_extensions_for_pandas/array/tensor.py
# and
# https://github.com/CODAIT/text-extensions-for-pandas/blob/dc03278689fe1c5f131573658ae19815ba25f33e/text_extensions_for_pandas/array/arrow_conversion.py

#
#  Copyright (c) 2020 IBM Corp.
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# Modifications:
# - Added ArrowTensorType.to_pandas_type()
# - Added ArrowTensorArray.__getitem__()
# - Added ArrowTensorArray.__iter__()
# - Added support for column casts to extension types.
# - Fleshed out docstrings and examples.
# - Fixed TensorArray.isna() so it returns an appropriate ExtensionArray.
# - Added different (more vectorized) TensorArray.take() operation.
# - Added support for more reducers (agg funcs) to TensorArray.
# - Added support for logical operators to TensorArray(Element).
# - Miscellaneous small bug fixes and optimizations.

from collections import Iterable
import numbers
from typing import Sequence, Any, Union, Tuple, Optional, Callable

import numpy as np
import pandas as pd
from pandas._typing import Dtype
from pandas.compat import set_function_name
from pandas.core.dtypes.generic import ABCDataFrame, ABCSeries

try:
    from pandas.core.dtypes.generic import ABCIndex
except ImportError:
    # ABCIndexClass changed to ABCIndex in Pandas 1.3
    from pandas.core.dtypes.generic import ABCIndexClass as ABCIndex
from pandas.core.indexers import check_array_indexer, validate_indices
import pyarrow as pa

from ray.util.annotations import PublicAPI

# -----------------------------------------------------------------------------
#                       Pandas extension type and array
# -----------------------------------------------------------------------------


@PublicAPI(stability="beta")
@pd.api.extensions.register_extension_dtype
class TensorDtype(pd.api.extensions.ExtensionDtype):
    """
    Pandas extension type for a column of fixed-shape, homogeneous-typed
    tensors.

    See:
    https://github.com/pandas-dev/pandas/blob/master/pandas/core/dtypes/base.py
    for up-to-date interface documentation and the subclassing contract. The
    docstrings of the below properties and methods were copied from the base
    ExtensionDtype.

    Examples:
        >>> # Create a DataFrame with a list of ndarrays as a column.
        >>> df = pd.DataFrame({
                "one": [1, 2, 3],
                "two": list(np.arange(24).reshape((3, 2, 2, 2)))})
        >>> # Note the opaque np.object dtype for this column.
        >>> df.dtypes
        one     int64
        two    object
        dtype: object

        >>> # Cast column to our TensorDtype extension type.
        >>> df["two"] = df["two"].astype(TensorDtype())

        >>> # Note that the column dtype is now TensorDtype instead of
        >>> # np.object.
        >>> df.dtypes
        one          int64
        two    TensorDtype
        dtype: object

        >>> # Pandas is now aware of this tensor column, and we can do the
        >>> # typical DataFrame operations on this column.
        >>> col = 2 * (df["two"] + 10)
        >>> # The ndarrays underlying the tensor column will be manipulated,
        >>> # but the column itself will continue to be a Pandas type.
        >>> type(col)
        pandas.core.series.Series
        >>> col
        0   [[[ 2  4]
              [ 6  8]]
             [[10 12]
               [14 16]]]
        1   [[[18 20]
              [22 24]]
             [[26 28]
              [30 32]]]
        2   [[[34 36]
              [38 40]]
             [[42 44]
              [46 48]]]
        Name: two, dtype: TensorDtype
        >>> # Once you do an aggregation on that column that returns a single
        >>> # row's value, you get back our TensorArrayElement type.
        >>> tensor = col.mean()
        >>> type(tensor)
        ray.data.extensions.tensor_extension.TensorArrayElement
        >>> tensor
        array([[[18., 20.],
                [22., 24.]],
               [[26., 28.],
                [30., 32.]]])
        >>> # This is a light wrapper around a NumPy ndarray, and can easily
        >>> # be converted to an ndarray.
        >>> type(tensor.to_numpy())
        numpy.ndarray

        >>> # In addition to doing Pandas operations on the tensor column,
        >>> # you can now put the DataFrame into a Dataset.
        >>> ds = ray.data.from_pandas(df)
        >>> # Internally, this column is represented the corresponding
        >>> # Arrow tensor extension type.
        >>> ds.schema()
        one: int64
        two: extension<arrow.py_extension_type<ArrowTensorType>>

        >>> # You can write the dataset to Parquet.
        >>> ds.write_parquet("/some/path")
        >>> # And you can read it back.
        >>> read_ds = ray.data.read_parquet("/some/path")
        >>> read_ds.schema()
        one: int64
        two: extension<arrow.py_extension_type<ArrowTensorType>>

        >>> read_df = ray.get(read_ds.to_pandas_refs())[0]
        >>> read_df.dtypes
        one          int64
        two    TensorDtype
        dtype: object
        >>> # The tensor extension type is preserved along the
        >>> # Pandas --> Arrow --> Parquet --> Arrow --> Pandas
        >>> # conversion chain.
        >>> read_df.equals(df)
        True
    """

    # NOTE(Clark): This is apparently required to prevent integer indexing
    # errors, but is an undocumented ExtensionDtype attribute. See issue:
    # https://github.com/CODAIT/text-extensions-for-pandas/issues/166
    base = None

    @property
    def type(self):
        """
        The scalar type for the array, e.g. ``int``
        It's expected ``ExtensionArray[item]`` returns an instance
        of ``ExtensionDtype.type`` for scalar ``item``, assuming
        that value is valid (not NA). NA values do not need to be
        instances of `type`.
        """
        return TensorArrayElement

    @property
    def name(self) -> str:
        """
        A string identifying the data type.
        Will be used for display in, e.g. ``Series.dtype``
        """
        return "TensorDtype"

    @classmethod
    def construct_from_string(cls, string: str):
        """
        Construct this type from a string.

        This is useful mainly for data types that accept parameters.
        For example, a period dtype accepts a frequency parameter that
        can be set as ``period[H]`` (where H means hourly frequency).

        By default, in the abstract class, just the name of the type is
        expected. But subclasses can overwrite this method to accept
        parameters.

        Parameters
        ----------
        string : str
            The name of the type, for example ``category``.

        Returns
        -------
        ExtensionDtype
            Instance of the dtype.

        Raises
        ------
        TypeError
            If a class cannot be constructed from this 'string'.

        Examples
        --------
        For extension dtypes with arguments the following may be an
        adequate implementation.

        >>> @classmethod
        ... def construct_from_string(cls, string):
        ...     pattern = re.compile(r"^my_type\[(?P<arg_name>.+)\]$")
        ...     match = pattern.match(string)
        ...     if match:
        ...         return cls(**match.groupdict())
        ...     else:
        ...         raise TypeError(
        ...             f"Cannot construct a '{cls.__name__}' from '{string}'"
        ...         )
        """
        if not isinstance(string, str):
            raise TypeError(
                f"'construct_from_string' expects a string, got {type(string)}"
            )
        # Upstream code uses exceptions as part of its normal control flow and
        # will pass this method bogus class names.
        if string == cls.__name__:
            return cls()
        else:
            raise TypeError(f"Cannot construct a '{cls.__name__}' from '{string}'")

    @classmethod
    def construct_array_type(cls):
        """
        Return the array type associated with this dtype.

        Returns
        -------
        type
        """
        return TensorArray

    def __from_arrow__(self, array: Union[pa.Array, pa.ChunkedArray]):
        """
        Convert a pyarrow (chunked) array to a TensorArray.

        This and TensorArray.__arrow_array__ make up the
        Pandas extension type + array <--> Arrow extension type + array
        interoperability protocol. See
        https://pandas.pydata.org/pandas-docs/stable/development/extending.html#compatibility-with-apache-arrow
        for more information.
        """
        if isinstance(array, pa.ChunkedArray):
            if array.num_chunks > 1:
                # TODO(Clark): Remove concat and construct from list with
                # shape.
                values = np.concatenate(
                    [chunk.to_numpy() for chunk in array.iterchunks()]
                )
            else:
                values = array.chunk(0).to_numpy()
        else:
            values = array.to_numpy()

        return TensorArray(values)


class TensorOpsMixin(pd.api.extensions.ExtensionScalarOpsMixin):
    """
    Mixin for TensorArray operator support, applying operations on the
    underlying ndarrays.
    """

    @classmethod
    def _create_method(cls, op, coerce_to_dtype=True, result_dtype=None):
        """
        Add support for binary operators by unwrapping, applying, and
        rewrapping.
        """

        # NOTE(Clark): This overrides, but coerce_to_dtype, result_dtype might
        # not be needed

        def _binop(self, other):
            lvalues = self._tensor

            if isinstance(other, (ABCDataFrame, ABCSeries, ABCIndex)):
                # Rely on Pandas to unbox and dispatch to us.
                return NotImplemented

            # divmod returns a tuple
            if op_name in ["__divmod__", "__rdivmod__"]:
                # TODO(Clark): Add support for divmod and rdivmod.
                # div, mod = result
                raise NotImplementedError

            if isinstance(other, (TensorArray, TensorArrayElement)):
                rvalues = other._tensor
            else:
                rvalues = other

            result = op(lvalues, rvalues)

            # Force a TensorArray if rvalue is not a scalar.
            if isinstance(self, TensorArrayElement) and (
                not isinstance(other, TensorArrayElement) or not np.isscalar(other)
            ):
                result_wrapped = TensorArray(result)
            else:
                result_wrapped = cls(result)

            return result_wrapped

        op_name = f"__{op.__name__}__"
        return set_function_name(_binop, op_name, cls)

    @classmethod
    def _create_logical_method(cls, op):
        return cls._create_method(op)


class TensorArrayElement(TensorOpsMixin):
    """
    Single element of a TensorArray, wrapping an underlying ndarray.
    """

    def __init__(self, values: np.ndarray):
        """
        Construct a TensorArrayElement from a NumPy ndarray.

        Args:
            values: ndarray that underlies this TensorArray element.
        """
        self._tensor = values

    def __repr__(self):
        return self._tensor.__repr__()

    def __str__(self):
        return self._tensor.__str__()

    def to_numpy(self):
        """
        Return the values of this element as a NumPy ndarray.
        """
        return np.asarray(self._tensor)

    def __array__(self):
        return np.asarray(self._tensor)


@PublicAPI(stability="beta")
class TensorArray(pd.api.extensions.ExtensionArray, TensorOpsMixin):
    """
    Pandas `ExtensionArray` representing a tensor column, i.e. a column
    consisting of ndarrays as elements. All tensors in a column must have the
    same shape.

    Examples:
        >>> # Create a DataFrame with a list of ndarrays as a column.
        >>> df = pd.DataFrame({
                "one": [1, 2, 3],
                "two": TensorArray(np.arange(24).reshape((3, 2, 2, 2)))})

        >>> # Note that the column dtype is TensorDtype.
        >>> df.dtypes
        one          int64
        two    TensorDtype
        dtype: object

        >>> # Pandas is aware of this tensor column, and we can do the
        >>> # typical DataFrame operations on this column.
        >>> col = 2 * (df["two"] + 10)
        >>> # The ndarrays underlying the tensor column will be manipulated,
        >>> # but the column itself will continue to be a Pandas type.
        >>> type(col)
        pandas.core.series.Series
        >>> col
        0   [[[ 2  4]
              [ 6  8]]
             [[10 12]
               [14 16]]]
        1   [[[18 20]
              [22 24]]
             [[26 28]
              [30 32]]]
        2   [[[34 36]
              [38 40]]
             [[42 44]
              [46 48]]]
        Name: two, dtype: TensorDtype
        >>> # Once you do an aggregation on that column that returns a single
        >>> # row's value, you get back our TensorArrayElement type.
        >>> tensor = col.mean()
        >>> type(tensor)
        ray.data.extensions.tensor_extension.TensorArrayElement
        >>> tensor
        array([[[18., 20.],
                [22., 24.]],
               [[26., 28.],
                [30., 32.]]])
        >>> # This is a light wrapper around a NumPy ndarray, and can easily
        >>> # be converted to an ndarray.
        >>> type(tensor.to_numpy())
        numpy.ndarray

        >>> # In addition to doing Pandas operations on the tensor column,
        >>> # you can now put the DataFrame into a Dataset.
        >>> ds = ray.data.from_pandas(df)
        >>> # Internally, this column is represented the corresponding
        >>> # Arrow tensor extension type.
        >>> ds.schema()
        one: int64
        two: extension<arrow.py_extension_type<ArrowTensorType>>

        >>> # You can write the dataset to Parquet.
        >>> ds.write_parquet("/some/path")
        >>> # And you can read it back.
        >>> read_ds = ray.data.read_parquet("/some/path")
        >>> read_ds.schema()
        one: int64
        two: extension<arrow.py_extension_type<ArrowTensorType>>

        >>> read_df = ray.get(read_ds.to_pandas_refs())[0]
        >>> read_df.dtypes
        one          int64
        two    TensorDtype
        dtype: object
        >>> # The tensor extension type is preserved along the
        >>> # Pandas --> Arrow --> Parquet --> Arrow --> Pandas
        >>> # conversion chain.
        >>> read_df.equals(df)
        True
    """

    SUPPORTED_REDUCERS = {
        "sum": np.sum,
        "all": np.all,
        "any": np.any,
        "min": np.min,
        "max": np.max,
        "mean": np.mean,
        "median": np.median,
        "prod": np.prod,
        "std": np.std,
        "var": np.var,
    }

    # See https://github.com/pandas-dev/pandas/blob/master/pandas/core/arrays/base.py  # noqa
    # for interface documentation and the subclassing contract.
    def __init__(
        self,
        values: Union[
            np.ndarray,
            ABCSeries,
            Sequence[Union[np.ndarray, TensorArrayElement]],
            TensorArrayElement,
            Any,
        ],
    ):
        """
        Args:
            values: A NumPy ndarray or sequence of NumPy ndarrays of equal
                shape.
        """
        if isinstance(values, ABCSeries):
            # Convert series to ndarray and passthrough to ndarray handling
            # logic.
            values = values.to_numpy()

        if isinstance(values, np.ndarray):
            if (
                values.dtype.type is np.object_
                and len(values) > 0
                and isinstance(values[0], (np.ndarray, TensorArrayElement))
            ):
                # Convert ndarrays of ndarrays/TensorArrayElements
                # with an opaque object type to a properly typed ndarray of
                # ndarrays.
                self._tensor = np.array([np.asarray(v) for v in values])
            else:
                self._tensor = values
        elif isinstance(values, Sequence):
            if len(values) == 0:
                self._tensor = np.array([])
            else:
                self._tensor = np.stack([np.asarray(v) for v in values], axis=0)
        elif isinstance(values, TensorArrayElement):
            self._tensor = np.array([np.asarray(values)])
        elif np.isscalar(values):
            # `values` is a single element:
            self._tensor = np.array([values])
        elif isinstance(values, TensorArray):
            raise TypeError("Use the copy() method to create a copy of a TensorArray")
        else:
            raise TypeError(
                "Expected a numpy.ndarray or sequence of numpy.ndarray, "
                f"but received {values} "
                f"of type '{type(values)}' instead."
            )

    @classmethod
    def _from_sequence(
        cls, scalars, *, dtype: Optional[Dtype] = None, copy: bool = False
    ):
        """
        Construct a new ExtensionArray from a sequence of scalars.

        Parameters
        ----------
        scalars : Sequence
            Each element will be an instance of the scalar type for this
            array, ``cls.dtype.type`` or be converted into this type in this
            method.
        dtype : dtype, optional
            Construct for this particular dtype. This should be a Dtype
            compatible with the ExtensionArray.
        copy : bool, default False
            If True, copy the underlying data.

        Returns
        -------
        ExtensionArray
        """
        if copy and isinstance(scalars, np.ndarray):
            scalars = scalars.copy()
        elif isinstance(scalars, TensorArray):
            scalars = scalars._tensor.copy() if copy else scalars._tensor
        return TensorArray(scalars)

    @classmethod
    def _from_factorized(
        cls, values: np.ndarray, original: pd.api.extensions.ExtensionArray
    ):
        """
        Reconstruct an ExtensionArray after factorization.

        Parameters
        ----------
        values : ndarray
            An integer ndarray with the factorized values.
        original : ExtensionArray
            The original ExtensionArray that factorize was called on.

        See Also
        --------
        factorize : Top-level factorize method that dispatches here.
        ExtensionArray.factorize : Encode the extension array as an enumerated
            type.
        """
        raise NotImplementedError

    def __getitem__(
        self, item: Union[int, slice, np.ndarray]
    ) -> Union["TensorArray", "TensorArrayElement"]:
        """
        Select a subset of self.

        Parameters
        ----------
        item : int, slice, or ndarray
            * int: The position in 'self' to get.
            * slice: A slice object, where 'start', 'stop', and 'step' are
              integers or None
            * ndarray: A 1-d boolean NumPy ndarray the same length as 'self'

        Returns
        -------
        item : scalar or ExtensionArray

        Notes
        -----
        For scalar ``item``, return a scalar value suitable for the array's
        type. This should be an instance of ``self.dtype.type``.
        For slice ``key``, return an instance of ``ExtensionArray``, even
        if the slice is length 0 or 1.
        For a boolean mask, return an instance of ``ExtensionArray``, filtered
        to the values where ``item`` is True.
        """
        # Return scalar if single value is selected, a TensorArrayElement for
        # single array element, or TensorArray for slice.
        if isinstance(item, int):
            value = self._tensor[item]
            if np.isscalar(value):
                return value
            else:
                return TensorArrayElement(value)
        else:
            # BEGIN workaround for Pandas issue #42430
            if isinstance(item, tuple) and len(item) > 1 and item[0] == Ellipsis:
                if len(item) > 2:
                    # Hopefully this case is not possible, but can't be sure
                    raise ValueError(
                        "Workaround Pandas issue #42430 not "
                        "implemented for tuple length > 2"
                    )
                item = item[1]
            # END workaround for issue #42430
            if isinstance(item, TensorArray):
                item = np.asarray(item)
            item = check_array_indexer(self, item)
            return TensorArray(self._tensor[item])

    def __len__(self) -> int:
        """
        Length of this array.

        Returns
        -------
        length : int
        """
        return len(self._tensor)

    @property
    def dtype(self) -> pd.api.extensions.ExtensionDtype:
        """
        An instance of 'ExtensionDtype'.
        """
        return TensorDtype()

    @property
    def nbytes(self) -> int:
        """
        The number of bytes needed to store this object in memory.
        """
        return self._tensor.nbytes

    def isna(self) -> "TensorArray":
        """
        A 1-D array indicating if each value is missing.

        Returns
        -------
        na_values : Union[np.ndarray, ExtensionArray]
            In most cases, this should return a NumPy ndarray. For
            exceptional cases like ``SparseArray``, where returning
            an ndarray would be expensive, an ExtensionArray may be
            returned.

        Notes
        -----
        If returning an ExtensionArray, then

        * ``na_values._is_boolean`` should be True
        * `na_values` should implement :func:`ExtensionArray._reduce`
        * ``na_values.any`` and ``na_values.all`` should be implemented
        """
        if self._tensor.dtype.type is np.object_:
            # Avoid comparing with __eq__ because the elements of the tensor
            # may do something funny with that operation.
            result_list = [self._tensor[i] is None for i in range(len(self))]
            result = np.broadcast_to(
                np.array(result_list, dtype=np.bool), self.numpy_shape
            )
        elif self._tensor.dtype.type is np.str_:
            result = self._tensor == ""
        else:
            result = np.isnan(self._tensor)
        return TensorArray(result)

    def take(
        self, indices: Sequence[int], allow_fill: bool = False, fill_value: Any = None
    ) -> "TensorArray":
        """
        Take elements from an array.

        Parameters
        ----------
        indices : sequence of int
            Indices to be taken.
        allow_fill : bool, default False
            How to handle negative values in `indices`.

            * False: negative values in `indices` indicate positional indices
              from the right (the default). This is similar to
              :func:`numpy.take`.

            * True: negative values in `indices` indicate
              missing values. These values are set to `fill_value`. Any other
              other negative values raise a ``ValueError``.

        fill_value : any, optional
            Fill value to use for NA-indices when `allow_fill` is True.
            This may be ``None``, in which case the default NA value for
            the type, ``self.dtype.na_value``, is used.

            For many ExtensionArrays, there will be two representations of
            `fill_value`: a user-facing "boxed" scalar, and a low-level
            physical NA value. `fill_value` should be the user-facing version,
            and the implementation should handle translating that to the
            physical version for processing the take if necessary.

        Returns
        -------
        ExtensionArray

        Raises
        ------
        IndexError
            When the indices are out of bounds for the array.
        ValueError
            When `indices` contains negative values other than ``-1``
            and `allow_fill` is True.

        See Also
        --------
        numpy.take : Take elements from an array along an axis.
        api.extensions.take : Take elements from an array.

        Notes
        -----
        ExtensionArray.take is called by ``Series.__getitem__``, ``.loc``,
        ``iloc``, when `indices` is a sequence of values. Additionally,
        it's called by :meth:`Series.reindex`, or any other method
        that causes realignment, with a `fill_value`.

        Examples
        --------
        Here's an example implementation, which relies on casting the
        extension array to object dtype. This uses the helper method
        :func:`pandas.api.extensions.take`.

        .. code-block:: python

           def take(self, indices, allow_fill=False, fill_value=None):
               from pandas.core.algorithms import take

               # If the ExtensionArray is backed by an ndarray, then
               # just pass that here instead of coercing to object.
               data = self.astype(object)

               if allow_fill and fill_value is None:
                   fill_value = self.dtype.na_value

               # fill value should always be translated from the scalar
               # type for the array, to the physical storage type for
               # the data, before passing to take.

               result = take(data, indices, fill_value=fill_value,
                             allow_fill=allow_fill)
               return self._from_sequence(result, dtype=self.dtype)
        """
        if allow_fill:
            # With allow_fill being True, negative values in `indices` indicate
            # missing values and should be set to `fill_value`.
            indices = np.asarray(indices, dtype=np.intp)
            validate_indices(indices, len(self._tensor))

            # Check if there are missing indices to fill, otherwise we can
            # delegate to NumPy ndarray .take().
            has_missing = np.any(indices < 0)
            if has_missing:
                if fill_value is None:
                    fill_value = np.nan

                # Create an array populated with fill value.
                values = np.full((len(indices),) + self._tensor.shape[1:], fill_value)

                # Put tensors at the given positive indices into array.
                is_nonneg = indices >= 0
                np.put(values, np.where(is_nonneg)[0], self._tensor[indices[is_nonneg]])

                return TensorArray(values)

        # Delegate take to NumPy array.
        values = self._tensor.take(indices, axis=0)

        return TensorArray(values)

    def copy(self) -> "TensorArray":
        """
        Return a copy of the array.

        Returns
        -------
        ExtensionArray
        """
        # TODO(Clark): Copy cached properties.
        return TensorArray(self._tensor.copy())

    @classmethod
    def _concat_same_type(cls, to_concat: Sequence["TensorArray"]) -> "TensorArray":
        """
        Concatenate multiple array of this dtype.

        Parameters
        ----------
        to_concat : sequence of this type

        Returns
        -------
        ExtensionArray
        """
        return TensorArray(np.concatenate([a._tensor for a in to_concat]))

    def __setitem__(self, key: Union[int, np.ndarray], value: Any) -> None:
        """
        Set one or more values inplace.

        This method is not required to satisfy the pandas extension array
        interface.

        Parameters
        ----------
        key : int, ndarray, or slice
            When called from, e.g. ``Series.__setitem__``, ``key`` will be
            one of

            * scalar int
            * ndarray of integers.
            * boolean ndarray
            * slice object

        value : ExtensionDtype.type, Sequence[ExtensionDtype.type], or object
            value or values to be set of ``key``.

        Returns
        -------
        None
        """
        key = check_array_indexer(self, key)
        if isinstance(value, TensorArrayElement) or np.isscalar(value):
            value = np.asarray(value)
        if isinstance(value, list):
            value = [
                np.asarray(v) if isinstance(v, TensorArrayElement) else v for v in value
            ]
        if isinstance(value, ABCSeries) and isinstance(value.dtype, TensorDtype):
            value = value.values
        if value is None or isinstance(value, Sequence) and len(value) == 0:
            self._tensor[key] = np.full_like(self._tensor[key], np.nan)
        elif isinstance(key, (int, slice, np.ndarray)):
            self._tensor[key] = value
        else:
            raise NotImplementedError(
                f"__setitem__ with key type '{type(key)}' not implemented"
            )

    def __contains__(self, item) -> bool:
        """
        Return for `item in self`.
        """
        if isinstance(item, TensorArrayElement):
            np_item = np.asarray(item)
            if np_item.size == 1 and np.isnan(np_item).all():
                return self.isna().any()
        return super().__contains__(item)

    def __repr__(self):
        return self._tensor.__repr__()

    def __str__(self):
        return self._tensor.__str__()

    def _values_for_factorize(self) -> Tuple[np.ndarray, Any]:
        # TODO(Clark): return self._tensor, np.nan
        raise NotImplementedError

    def _reduce(self, name: str, skipna: bool = True, **kwargs):
        """
        Return a scalar result of performing the reduction operation.

        Parameters
        ----------
        name : str
            Name of the function, supported values are:
            { any, all, min, max, sum, mean, median, prod,
            std, var, sem, kurt, skew }.
        skipna : bool, default True
            If True, skip NaN values.
        **kwargs
            Additional keyword arguments passed to the reduction function.
            Currently, `ddof` is the only supported kwarg.

        Returns
        -------
        scalar

        Raises
        ------
        TypeError : subclass does not define reductions
        """
        supported_kwargs = ["ddof"]
        reducer_kwargs = {}
        for kw in supported_kwargs:
            try:
                reducer_kwargs[kw] = kwargs[kw]
            except KeyError:
                pass
        try:
            return TensorArrayElement(
                self.SUPPORTED_REDUCERS[name](self._tensor, axis=0, **reducer_kwargs)
            )
        except KeyError:
            raise NotImplementedError(f"'{name}' aggregate not implemented.") from None

    def __array__(self, dtype: np.dtype = None):
        return np.asarray(self._tensor, dtype=dtype)

    def __array_ufunc__(self, ufunc: Callable, method: str, *inputs, **kwargs):
        """
        Supports NumPy ufuncs without requiring sloppy coercion to an
        ndarray.
        """
        out = kwargs.get("out", ())
        for x in inputs + out:
            if not isinstance(x, (TensorArray, np.ndarray, numbers.Number)):
                return NotImplemented

        # Defer to the implementation of the ufunc on unwrapped values.
        inputs = tuple(x._tensor if isinstance(x, TensorArray) else x for x in inputs)
        if out:
            kwargs["out"] = tuple(
                x._tensor if isinstance(x, TensorArray) else x for x in out
            )
        result = getattr(ufunc, method)(*inputs, **kwargs)

        if type(result) is tuple:
            # Multiple return values.
            return tuple(type(self)(x) for x in result)
        elif method == "at":
            # No return value.
            return None
        else:
            # One return value.
            return type(self)(result)

    def to_numpy(
        self,
        dtype: np.dtype = None,
        copy: bool = False,
        na_value: Any = pd.api.extensions.no_default,
    ):
        """
        Convert to a NumPy ndarray.

        .. versionadded:: 1.0.0

        This is similar to :meth:`numpy.asarray`, but may provide additional
        control over how the conversion is done.

        Parameters
        ----------
        dtype : str or numpy.dtype, optional
            The dtype to pass to :meth:`numpy.asarray`.
        copy : bool, default False
            Whether to ensure that the returned value is a not a view on
            another array. Note that ``copy=False`` does not *ensure* that
            ``to_numpy()`` is no-copy. Rather, ``copy=True`` ensure that
            a copy is made, even if not strictly necessary.
        na_value : Any, optional
            The value to use for missing values. The default value depends
            on `dtype` and the type of the array.

        Returns
        -------
        numpy.ndarray
        """
        if dtype is not None:
            dtype = pd.api.types.pandas_dtype(dtype)
            if copy:
                values = np.array(self._tensor, dtype=dtype, copy=True)
            else:
                values = self._tensor.astype(dtype)
        elif copy:
            values = self._tensor.copy()
        else:
            values = self._tensor
        return values

    @property
    def numpy_dtype(self):
        """
        Get the dtype of the tensor.
        :return: The numpy dtype of the backing ndarray
        """
        return self._tensor.dtype

    @property
    def numpy_ndim(self):
        """
        Get the number of tensor dimensions.
        :return: integer for the number of dimensions
        """
        return self._tensor.ndim

    @property
    def numpy_shape(self):
        """
        Get the shape of the tensor.
        :return: A tuple of integers for the numpy shape of the backing ndarray
        """
        return self._tensor.shape

    @property
    def _is_boolean(self):
        """
        Whether this extension array should be considered boolean.

        By default, ExtensionArrays are assumed to be non-numeric.
        Setting this to True will affect the behavior of several places,
        e.g.

        * is_bool
        * boolean indexing

        Returns
        -------
        bool
        """
        # This is needed to support returning a TensorArray from .isnan().
        # TODO(Clark): Propagate tensor dtype to extension TensorDtype and
        # move this property there.
        return np.issubdtype(self._tensor.dtype, np.bool)

    def astype(self, dtype, copy=True):
        """
        Cast to a NumPy array with 'dtype'.

        Parameters
        ----------
        dtype : str or dtype
            Typecode or data-type to which the array is cast.
        copy : bool, default True
            Whether to copy the data, even if not necessary. If False,
            a copy is made only if the old dtype does not match the
            new dtype.

        Returns
        -------
        array : ndarray
            NumPy ndarray with 'dtype' for its dtype.
        """
        dtype = pd.api.types.pandas_dtype(dtype)

        if isinstance(dtype, TensorDtype):
            values = TensorArray(self._tensor.copy()) if copy else self
        elif not (
            pd.api.types.is_object_dtype(dtype) and pd.api.types.is_string_dtype(dtype)
        ):
            values = np.array([str(t) for t in self._tensor])
            if isinstance(dtype, pd.StringDtype):
                return dtype.construct_array_type()._from_sequence(values, copy=False)
            else:
                return values
        elif pd.api.types.is_object_dtype(dtype):
            # Interpret astype(object) as "cast to an array of numpy arrays"
            values = np.empty(len(self), dtype=object)
            for i in range(len(self)):
                values[i] = self._tensor[i]
        else:
            values = self._tensor.astype(dtype, copy=copy)
        return values

    def any(self, axis=None, out=None, keepdims=False):
        """
        Test whether any array element along a given axis evaluates to True.

        See numpy.any() documentation for more information
        https://numpy.org/doc/stable/reference/generated/numpy.any.html#numpy.any

        :param axis: Axis or axes along which a logical OR reduction is
            performed.
        :param out: Alternate output array in which to place the result.
        :param keepdims: If this is set to True, the axes which are reduced are
            left in the result as dimensions with size one.
        :return: single boolean unless axis is not None else TensorArray
        """
        result = self._tensor.any(axis=axis, out=out, keepdims=keepdims)
        return result if axis is None else TensorArray(result)

    def all(self, axis=None, out=None, keepdims=False):
        """
        Test whether all array elements along a given axis evaluate to True.

        :param axis: Axis or axes along which a logical AND reduction is
            performed.
        :param out: Alternate output array in which to place the result.
        :param keepdims: If this is set to True, the axes which are reduced are
            left in the result as dimensions with size one.
        :return: single boolean unless axis is not None else TensorArray
        """
        result = self._tensor.all(axis=axis, out=out, keepdims=keepdims)
        return result if axis is None else TensorArray(result)

    def __arrow_array__(self, type=None):
        """
        Convert this TensorArray to an ArrowTensorArray extension array.

        This and TensorDtype.__from_arrow__ make up the
        Pandas extension type + array <--> Arrow extension type + array
        interoperability protocol. See
        https://pandas.pydata.org/pandas-docs/stable/development/extending.html#compatibility-with-apache-arrow
        for more information.
        """
        return ArrowTensorArray.from_numpy(self._tensor)


# Add operators from the mixin to the TensorArrayElement and TensorArray
# classes.
TensorArrayElement._add_arithmetic_ops()
TensorArrayElement._add_comparison_ops()
TensorArrayElement._add_logical_ops()
TensorArray._add_arithmetic_ops()
TensorArray._add_comparison_ops()
TensorArray._add_logical_ops()

# -----------------------------------------------------------------------------
#                       Arrow extension type and array
# -----------------------------------------------------------------------------


@PublicAPI(stability="beta")
class ArrowTensorType(pa.PyExtensionType):
    """
    Arrow ExtensionType for an array of fixed-shaped, homogeneous-typed
    tensors.

    This is the Arrow side of TensorDtype.

    See Arrow extension type docs:
    https://arrow.apache.org/docs/python/extending_types.html#defining-extension-types-user-defined-types
    """

    def __init__(self, shape: Tuple[int, ...], dtype: pa.DataType):
        """
        Construct the Arrow extension type for array of fixed-shaped tensors.

        Args:
            shape: Shape of contained tensors.
            dtype: pyarrow dtype of tensor elements.
        """
        self._shape = shape
        super().__init__(pa.list_(dtype))

    @property
    def shape(self):
        """
        Shape of contained tensors.
        """
        return self._shape

    def to_pandas_dtype(self):
        """
        Convert Arrow extension type to corresponding Pandas dtype.

        Returns:
            An instance of pd.api.extensions.ExtensionDtype.
        """
        return TensorDtype()

    def __reduce__(self):
        return ArrowTensorType, (self._shape, self.storage_type.value_type)

    def __arrow_ext_class__(self):
        """
        ExtensionArray subclass with custom logic for this array of tensors
        type.

        Returns:
            A subclass of pd.api.extensions.ExtensionArray.
        """
        return ArrowTensorArray

    def __str__(self):
        return "<ArrowTensorType: shape={}, dtype={}>".format(
            self.shape, self.storage_type.value_type
        )


@PublicAPI(stability="beta")
class ArrowTensorArray(pa.ExtensionArray):
    """
    An array of fixed-shape, homogeneous-typed tensors.

    This is the Arrow side of TensorArray.

    See Arrow docs for customizing extension arrays:
    https://arrow.apache.org/docs/python/extending_types.html#custom-extension-array-class
    """

    OFFSET_DTYPE = np.int32

    def __getitem__(self, key):
        # This __getitem__ hook allows us to support proper
        # indexing when accessing a single tensor (a "scalar" item of the
        # array). Without this hook for integer keys, the indexing will fail on
        # all currently released pyarrow versions due to a lack of proper
        # ExtensionScalar support. Support was added in
        # https://github.com/apache/arrow/pull/10904, but hasn't been released
        # at the time of this comment, and even with this support, the returned
        # ndarray is a flat representation of the n-dimensional tensor.

        # NOTE(Clark): We'd like to override the pa.Array.getitem() helper
        # instead, which would obviate the need for overriding __iter__()
        # below, but unfortunately overriding Cython cdef methods with normal
        # Python methods isn't allowed.
        if isinstance(key, slice):
            return super().__getitem__(key)
        return self._to_numpy(key)

    def __iter__(self):
        # Override pa.Array.__iter__() in order to return an iterator of
        # properly shaped tensors instead of an iterator of flattened tensors.
        # See comment in above __getitem__ method.
        for i in range(len(self)):
            # Use overridden __getitem__ method.
            yield self.__getitem__(i)

    def to_pylist(self):
        # Override pa.Array.to_pylist() due to a lack of ExtensionScalar
        # support (see comment in __getitem__).
        return list(self)

    @classmethod
    def from_numpy(cls, arr):
        """
        Convert an ndarray or an iterable of fixed-shape ndarrays to an array
        of fixed-shape, homogeneous-typed tensors.

        Args:
            arr: An ndarray or an iterable of fixed-shape ndarrays.

        Returns:
            An ArrowTensorArray containing len(arr) tensors of fixed shape.
        """
        if isinstance(arr, (list, tuple)):
            if np.isscalar(arr[0]):
                return pa.array(arr)
            elif isinstance(arr[0], np.ndarray):
                # Stack ndarrays and pass through to ndarray handling logic
                # below.
                arr = np.stack(arr, axis=0)
        if isinstance(arr, np.ndarray):
            if not arr.flags.c_contiguous:
                # We only natively support C-contiguous ndarrays.
                arr = np.ascontiguousarray(arr)
            pa_dtype = pa.from_numpy_dtype(arr.dtype)
            outer_len = arr.shape[0]
            element_shape = arr.shape[1:]
            total_num_items = arr.size
            num_items_per_element = np.prod(element_shape) if element_shape else 1

            # Data buffer.
            data_buffer = pa.py_buffer(arr)
            data_array = pa.Array.from_buffers(
                pa_dtype, total_num_items, [None, data_buffer]
            )

            # Offset buffer.
            offset_buffer = pa.py_buffer(
                cls.OFFSET_DTYPE(
                    [i * num_items_per_element for i in range(outer_len + 1)]
                )
            )

            storage = pa.Array.from_buffers(
                pa.list_(pa_dtype),
                outer_len,
                [None, offset_buffer],
                children=[data_array],
            )
            type_ = ArrowTensorType(element_shape, pa_dtype)
            return pa.ExtensionArray.from_storage(type_, storage)
        elif isinstance(arr, Iterable):
            return cls.from_numpy(list(arr))
        else:
            raise ValueError("Must give ndarray or iterable of ndarrays.")

    def _to_numpy(self, index: Optional[int] = None, zero_copy_only: bool = False):
        """
        Helper for getting either an element of the array of tensors as an
        ndarray, or the entire array of tensors as a single ndarray.

        Args:
            index: The index of the tensor element that we wish to return as
                an ndarray. If not given, the entire array of tensors is
                returned as an ndarray.
            zero_copy_only: If True, an exception will be raised if the
                conversion to a NumPy array would require copying the
                underlying data (e.g. in presence of nulls, or for
                non-primitive types). This argument is currently ignored, so
                zero-copy isn't enforced even if this argument is true.

        Returns:
            The corresponding tensor element as an ndarray if an index was
            given, or the entire array of tensors as an ndarray otherwise.
        """
        # Buffers schema:
        # [None, offset_buffer, None, data_buffer]
        buffers = self.buffers()
        data_buffer = buffers[3]
        storage_list_type = self.storage.type
        ext_dtype = storage_list_type.value_type.to_pandas_dtype()
        shape = self.type.shape
        value_type = storage_list_type.value_type
        if pa.types.is_boolean(value_type):
            # Boolean array buffers are byte-packed, with 8 entries per byte,
            # and are accessed via bit offsets.
            buffer_item_width = value_type.bit_width
        else:
            # We assume all other array types are accessed via byte array
            # offsets.
            buffer_item_width = value_type.bit_width // 8
        # Number of items per inner ndarray.
        num_items_per_element = np.prod(shape) if shape else 1
        # Base offset into data buffer, e.g. due to zero-copy slice.
        buffer_offset = self.offset * num_items_per_element
        # Offset of array data in buffer.
        offset = buffer_item_width * buffer_offset
        if index is not None:
            # Getting a single tensor element of the array.
            offset_buffer = buffers[1]
            offset_array = np.ndarray(
                (len(self),), buffer=offset_buffer, dtype=self.OFFSET_DTYPE
            )
            # Offset into array to reach logical index.
            index_offset = offset_array[index]
            # Add the index offset to the base offset.
            offset += buffer_item_width * index_offset
        else:
            # Getting the entire array of tensors.
            shape = (len(self),) + shape
        # TODO(Clark): Enforce zero_copy_only.
        # TODO(Clark): Support strides?
        return np.ndarray(shape, dtype=ext_dtype, buffer=data_buffer, offset=offset)

    def to_numpy(self, zero_copy_only: bool = True):
        """
        Convert the entire array of tensors into a single ndarray.

        Args:
            zero_copy_only: If True, an exception will be raised if the
                conversion to a NumPy array would require copying the
                underlying data (e.g. in presence of nulls, or for
                non-primitive types). This argument is currently ignored, so
                zero-copy isn't enforced even if this argument is true.

        Returns:
            A single ndarray representing the entire array of tensors.
        """
        return self._to_numpy(zero_copy_only=zero_copy_only)
