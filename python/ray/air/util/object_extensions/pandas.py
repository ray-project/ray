import collections.abc
import typing

import numpy as np
import pandas as pd
import pyarrow as pa
from pandas._libs import lib
from pandas._typing import ArrayLike, Dtype, PositionalIndexer, TakeIndexer, npt

import ray.air.util.object_extensions.arrow
from ray.util.annotations import PublicAPI


# See https://pandas.pydata.org/docs/development/extending.html for more information.
@PublicAPI(stability="alpha")
class PythonObjectArray(pd.api.extensions.ExtensionArray):
    """Implements the Pandas extension array interface for the Arrow object array"""

    def __init__(self, values: collections.abc.Iterable[typing.Any]):
        vals = list(values)
        self.values = np.empty(len(vals), dtype=object)
        self.values[:] = vals

    @classmethod
    def _from_sequence(
        cls,
        scalars: collections.abc.Sequence[typing.Any],
        *,
        dtype: typing.Union[Dtype, None] = None,
        copy: bool = False,
    ) -> "PythonObjectArray":
        return PythonObjectArray(scalars)

    @classmethod
    def _from_factorized(
        cls, values: collections.abc.Sequence[typing.Any], original: "PythonObjectArray"
    ) -> "PythonObjectArray":
        return PythonObjectArray(values)

    def __getitem__(self, item: PositionalIndexer) -> typing.Any:
        return self.values[item]

    def __setitem__(self, key, value) -> None:
        self.values[key] = value

    def __len__(self) -> int:
        return len(self.values)

    def __eq__(self, other: object) -> ArrayLike:
        if isinstance(other, PythonObjectArray):
            return self.values == other.values
        elif isinstance(other, np.ndarray):
            return self.values == other
        else:
            return NotImplemented

    def to_numpy(
        self,
        dtype: typing.Union["npt.DTypeLike", None] = None,
        copy: bool = False,
        na_value: object = lib.no_default,
    ) -> np.ndarray:
        result = self.values
        if copy or na_value is not lib.no_default:
            result = result.copy()
        if na_value is not lib.no_default:
            result[self.isna()] = na_value
        return result

    @property
    def dtype(self) -> pd.api.extensions.ExtensionDtype:
        return PythonObjectDtype()

    @property
    def nbytes(self) -> int:
        return self.values.nbytes

    def __arrow_array__(self, type=None):
        return ray.air.util.object_extensions.arrow.ArrowPythonObjectArray.from_objects(
            self.values
        )

    def isna(self) -> np.ndarray:
        return pd.isnull(self.values)

    def take(
        self,
        indices: TakeIndexer,
        *,
        allow_fill: bool = False,
        fill_value: typing.Any = None,
    ) -> "PythonObjectArray":
        if allow_fill and fill_value is None:
            fill_value = self.dtype.na_value

        result = pd.core.algorithms.take(
            self.values, indices, allow_fill=allow_fill, fill_value=fill_value
        )
        return self._from_sequence(result, dtype=self.dtype)

    def copy(self) -> "PythonObjectArray":
        return PythonObjectArray(self.values)

    @classmethod
    def _concat_same_type(
        cls, to_concat: collections.abc.Sequence["PythonObjectArray"]
    ) -> "PythonObjectArray":
        values_to_concat = [element.values for element in to_concat]
        return cls(np.concatenate(values_to_concat))


@PublicAPI(stability="alpha")
@pd.api.extensions.register_extension_dtype
class PythonObjectDtype(pd.api.extensions.ExtensionDtype):
    @classmethod
    def construct_from_string(cls, string: str):
        if string != "python_object()":
            raise TypeError(f"Cannot construct a '{cls.__name__}' from '{string}'")
        return cls()

    @property
    def type(self):
        """
        The scalar type for the array, e.g. ``int``
        It's expected ``ExtensionArray[item]`` returns an instance
        of ``ExtensionDtype.type`` for scalar ``item``, assuming
        that value is valid (not NA). NA values do not need to be
        instances of `type`.
        """
        return object

    @property
    def name(self) -> str:
        return "python_object()"

    @classmethod
    def construct_array_type(cls: type) -> type:
        """
        Return the array type associated with this dtype.
        """
        return PythonObjectArray

    def __from_arrow__(
        self, array: typing.Union[pa.Array, pa.ChunkedArray]
    ) -> PythonObjectArray:
        return PythonObjectArray(array.to_pylist())
