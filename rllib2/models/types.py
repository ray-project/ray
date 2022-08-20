# Copyright 2021 DeepMind Technologies Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Custom Types."""

import itertools
from typing import (
    AbstractSet,
    Any,
    Dict,
    Generic,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

import numpy as np

from .specs import TensorSpecs
from ray.rllib.utils.typing import TensorType

StreamType = Union[str, Sequence[str]]
_IndexType = Union[StreamType, Sequence[StreamType]]
T = TypeVar("T")
# TODO(b/207379690): use recursive types when supported
_NestedDictType = Dict[str, Any]
_NestedMappingType = Mapping[_IndexType, Any]


def _flatten_index(index: _IndexType) -> Sequence[str]:
    if isinstance(index, str):
        return (index,)
    else:
        return tuple(itertools.chain.from_iterable([_flatten_index(y) for y in index]))


class StrKey(str):
    """A str StreamType which can be compared to Sequence[str] StreamType.

    This is needed for the tree functions to work.
    """

    def __lt__(self, other: StreamType):
        if isinstance(other, str):
            return str(self) < other
        else:
            return (self,) < other

    def __gt__(self, other: StreamType):
        if isinstance(other, str):
            return str(self) > other
        else:
            return (self,) > other


class NestedDict(Generic[T], MutableMapping[str, Union[T, "NestedDict"]]):
    """A nested dict with convenience functions.

    * The nested dict function gives access to nested elements as a sequence of
    strings.
    * These nested dicts can also be used to filter a superset into a subset of
    nested elements with the filter function.
    * This can be instantiated with any mapping of strings, or an iterable of
    key value tuples where the values can themselves be recursively the values
    that a nested dict can take.

    Note that the type of values T should not be an instance of Mapping.

    Example Usage:
      Basic
        foo_dict = NestedDict()
        # Setting elements, possibly nested:
        foo_dict['a'] = 100
        foo_dict['b', 'c'] = 200
        foo_dict['b', 'd'] = 300
        # Getting elements:
        print(foo_dict['b', 'c'])  --> 200
        print(foo_dict['b'])  --> IndexError("Use get for partial indexing.")
        print(foo_dict.get('b'))  --> {'c': 200, 'd': 300}
        print(foo_dict)  --> {'a': 100, 'b': {'c': 200, 'd': 300}}
        # Converting to a dict:
        foo_dict.asdict()  --> {'a': 100, 'b': {'c': 200, 'd': 300}}
        # len function:
        print(len(foo_dict))  --> 3
        # Iterating:
        foo_dict.keys()  --> dict_keys(['a', ('b', 'c'), ('b', 'd')])
        foo_dict.items()  --> dict_items([('a', 100), (('b', 'c'), 200), (('b',
        'd'), 300)])
        foo_dict.shallow_keys()  --> dict_keys(['a', 'b'])

      Filter
        dict1 = NestedDict([
          (('foo', 'a'), 10), (('foo', 'b'), 11),
          (('bar', 'c'), 11), (('bar', 'a'), 110)])
        dict2 = NestedDict([('foo', NestedDict(dict(a=11)))])
        dict3 = NestedDict([('foo', NestedDict(dict(a=100))),
                            ('bar', NestedDict(dict(d=11)))])
        dict4 = NestedDict([('foo', NestedDict(dict(a=100))),
                            ('bar', NestedDict(dict(c=11)))])
        dict1.filter(dict2).asdict()   --> {'foo': {'a': 10}}
        dict1.filter(dict4).asdict()   --> {'bar': {'c': 11}, 'foo': {'a': 10}}
        dict1.filter(dict3).asdict()   --> KeyError - ('bar', 'd') not in dict1
    """

    def __init__(
        self,
        x: Union[
            Iterable[Tuple[StreamType, T]], _NestedMappingType, "NestedDict[T]", None
        ] = None,
    ):
        self._data = dict()  # type: Dict[str, Union[T, NestedDict[T]]]
        x = x or {}
        if isinstance(x, Mapping):
            for k, v in x.items():
                self[k] = v
        else:
            if not isinstance(x, Iterable):
                raise ValueError(f"Input must be a Mapping or Iterable, got {x}.")
            for k, v in x:
                self[k] = v

    def __contains__(self, k: _IndexType) -> bool:
        k = _flatten_index(k)
        if len(k) == 1:
            return k[0] in self._data
        else:
            if k[0] in self._data and isinstance(self._data[k[0]], NestedDict):
                return k[1:] in self._data[k[0]]
            else:
                return False

    def get(
        self, k: _IndexType, *, default: Optional[T] = None
    ) -> Union[T, "NestedDict[T]"]:
        """Returns self[k], partial indexing allowed."""
        if k not in self:
            if default is not None:
                return default
            else:
                raise KeyError(k)
        k = _flatten_index(k)
        if len(k) == 1:
            return self._data[k[0]]
        else:
            return self._data[k[0]].get(k[1:])

    def __getitem__(self, k: _IndexType) -> T:
        output = self.get(k)
        if isinstance(output, NestedDict):
            raise IndexError("Use get for partial indexing.")
        return output

    def __setitem__(self, k: _IndexType, v: Union[T, _NestedMappingType]) -> None:
        if not k:
            raise IndexError("Use valid index value.")
        k = _flatten_index(k)
        v = (
            NestedDict[T](v) if isinstance(v, Mapping) else v
        )  # type: Union[T, NestedDict[T]]
        if len(k) == 1:
            self._data[k[0]] = v
        else:
            if k[0] not in self._data:
                self._data[k[0]] = NestedDict[T]()
            if not isinstance(self._data[k[0]], NestedDict):
                raise IndexError("Trying to assign nested values to a leaf.")
            self._data[k[0]][k[1:]] = v

    def __iter__(self) -> Iterator[StreamType]:
        for k, v in self._data.items():
            if isinstance(v, NestedDict):
                for x in v:
                    if isinstance(x, Tuple):
                        yield (k,) + x
                    else:
                        yield (k, x)
            else:
                yield StrKey(k)

    def __delitem__(self, k: _IndexType) -> None:
        if k not in self:
            raise KeyError(k)
        k = _flatten_index(k)
        if len(k) == 1:
            del self._data[k[0]]
        else:
            del self._data[k[0]][k[1:]]
            if not self._data[k[0]]:
                del self._data[k[0]]

    def __len__(self) -> int:
        output = 0
        for v in self.values():
            if isinstance(v, NestedDict):
                output += len(v)
            else:
                output += 1
        return output

    def __str__(self) -> str:
        return str(self.asdict())

    def filter(
        self,
        other: Union[Sequence[StreamType], "NestedDict[Any]"],
        ignore_missing: bool = False,
    ) -> "NestedDict[T]":
        """Returns a NestedDict with only entries present in `other`."""
        output = NestedDict[T]()
        if isinstance(other, Sequence):
            keys = other
        else:
            keys = other.keys()
        for k in keys:
            if k not in self:
                if not ignore_missing:
                    raise KeyError(k)
            else:
                output[k] = self.get(k)
        return output

    def asdict(self) -> _NestedDictType:
        output = dict()
        for k, v in self._data.items():
            if isinstance(v, NestedDict):
                output[k] = v.asdict()
            else:
                output[k] = v
        return output

    def copy(self) -> "NestedDict[T]":
        output = NestedDict[T]()
        for k, v in self.items():
            output[k] = v
        return output

    def __copy__(self) -> "NestedDict[T]":
        return self.copy()

    def shallow_keys(self) -> AbstractSet[str]:
        return self._data.keys()


TensorDict = NestedDict[TensorType]


class SpecDict(NestedDict[TensorSpecs]):
    """A NestedDict containing a spec."""

    def validate(
        self,
        data: Union["SpecDict", NestedDict],
        exact_match: bool = False,
        num_leading_dims_to_ignore: int = 0,
        error_prefix: Optional[str] = None,
    ) -> None:
        """Checks whether the data matches the spec.

        Args:
          data: The data which should match the spec. It can also be a spec
          exact_match: If true, the data and the spec must be exactly identical.
            Otherwise, the data is validated as long as it contains at least the
            elements of the spec, but can contain more entries.
          num_leading_dims_to_ignore: The first n dimensions of the data are not
            part of the spec. They still must have the same size across data.
          error_prefix: An optional string to append before the error message.

        Raises:
          ValueError: If the data doesn't match the spec.
        """
        error_prefix = "" if error_prefix is None else f"[{error_prefix}] "

        missing_keys = set(self.keys()).difference(set(data.keys()))
        if missing_keys:
            raise ValueError(
                f"{error_prefix}The data does not match the spec. Keys "
                f"{missing_keys} are in the spec but not in the data."
            )
        if exact_match:
            data_spec_missing_keys = set(data.keys()).difference(set(self.keys()))
            if data_spec_missing_keys:
                raise ValueError(
                    f"{error_prefix}The data does not match the spec. "
                    f"Keys {data_spec_missing_keys} are in the data but "
                    "not in the spec, and exact_match is set to True."
                )
        for k, v in self.items():
            data_to_validate = data[k]
            if len(data_to_validate.shape) < num_leading_dims_to_ignore:
                raise ValueError(
                    f"{error_prefix}Error when validating spec {k}: not "
                    f"enough dimension (shape={data_to_validate.shape})."
                )
            for _ in range(num_leading_dims_to_ignore):
                data_to_validate = data_to_validate[0]
            if isinstance(data_to_validate, np.ndarray):
                try:
                    v.validate(data_to_validate)
                except ValueError as e:
                    raise ValueError(
                        f"{error_prefix}Error when validating spec {k}: {e}"
                    ) from e

    def copy(self) -> "SpecDict":
        output = SpecDict()
        for k, v in self.items():
            output[k] = v
        return output

    def __copy__(self) -> "SpecDict":
        return self.copy()
