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

"""Custom NestedDict datatype."""

import itertools
from typing import (
    AbstractSet,
    Any,
    Dict,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)


StreamType = Union[str, Sequence[str]]
_IndexType = Union[StreamType, Sequence[StreamType]]

_NestedDictType = Dict[str, Any]
_NestedMappingType = Mapping[_IndexType, Any]

NestedDictInputType = Union[
    Iterable[Tuple[StreamType, Any]], _NestedMappingType, "NestedDict"
]


def _flatten_index(index: _IndexType) -> Sequence[str]:
    if isinstance(index, str):
        return (index,)
    else:
        return tuple(itertools.chain.from_iterable([_flatten_index(y) for y in index]))


class StrKey(str):
    """A string that can be compared to a string or sequence of strings representing a
    StreamType. This is needed for the tree functions to work.
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


class NestedDict(MutableMapping[str, "NestedDict"]):
    """A nested dict type:
        * The nested dict gives access to nested elements as a sequence of
        strings.
        * These nested dicts can also be used to filter a superset into a subset of
        nested elements with the filter function.
        * This can be instantiated with any mapping of strings, or an iterable of
        key value tuples where the values can themselves be recursively the values
        that a nested dict can take.

    Args:
        x: a representation of a nested dict: it can be an iterable of `StreamType`
        to values. e.g. `[(("a", "b") , 1), ("b", 2)]` or a mapping of flattened
        keys to values. e.g. `{("a", "b"): 1, ("b",): 2}` or any nested mapping,
        e.g. `{"a": {"b": 1}, "b": 2}`.

    Example:
        Basic usage:
            >>> foo_dict = NestedDict()
            >>> # Setting elements, possibly nested:
            >>> foo_dict['a'] = 100         # foo_dict = {'a': 100}
            >>> foo_dict['b', 'c'] = 200    # foo_dict = {'a': 100, 'b': {'c': 200}}
            >>> foo_dict['b', 'd'] = 300    # foo_dict = {'a': 100,
            >>>                             #             'b': {'c': 200, 'd': 300}}
            >>> # Getting elements, possibly nested:
            >>> print(foo_dict['b', 'c'])   # 200
            >>> print(foo_dict['b']) # IndexError("Use get for partial indexing.")
            >>> print(foo_dict.get('b'))    # {'c': 200, 'd': 300}
            >>> print(foo_dict) # {'a': 100, 'b': {'c': 200, 'd': 300}}
            >>> # Converting to a dict:
            >>> foo_dict.asdict()  # {'a': 100, 'b': {'c': 200, 'd': 300}}
            >>> # len function:
            >>> print(len(foo_dict))  # 3
            >>> # Iterating:
            >>> foo_dict.keys()  # dict_keys(['a', ('b', 'c'), ('b', 'd')])
            >>> foo_dict.items() # dict_items([('a', 100), (('b', 'c'), 200), (('b',
            'd'), 300)])
            >>> foo_dict.shallow_keys()  # dict_keys(['a', 'b'])
        Filter:
            >>> dict1 = NestedDict([
                (('foo', 'a'), 10), (('foo', 'b'), 11),
                (('bar', 'c'), 11), (('bar', 'a'), 110)])
            >>> dict2 = NestedDict([('foo', NestedDict(dict(a=11)))])
            >>> dict3 = NestedDict([('foo', NestedDict(dict(a=100))),
                                    ('bar', NestedDict(dict(d=11)))])
            >>> dict4 = NestedDict([('foo', NestedDict(dict(a=100))),
                                    ('bar', NestedDict(dict(c=11)))])
            >>> dict1.filter(dict2).asdict()   # {'foo': {'a': 10}}
            >>> dict1.filter(dict4).asdict()   # {'bar': {'c': 11}, 'foo': {'a': 10}}
            >>> dict1.filter(dict3).asdict()   # KeyError - ('bar', 'd') not in dict1
    """

    def __init__(
        self,
        x: Optional[NestedDictInputType] = None,
    ):
        self._data = dict()  # type: Dict[str, Any]
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

    def get(self, k: _IndexType, *, default: Optional[Any] = None) -> Any:
        """Returns `self[k]`, with partial indexing allowed.
        If `k` is not in the `NestedDict`, returns default. If default is `None`,
        and `k` is not in the `NestedDict`, a `KeyError` is raised.

        Args:
            k: the key to get. This can be a string or a sequence of strings.
            default: the default value to return if `k` is not in the `NestedDict`. If
                default is `None`, and `k` is not in the `NestedDict`, a `KeyError` is
                raised.

        Returns:
            The value of `self[k]`.

        Raises:
            KeyError: if `k` is not in the `NestedDict` and default is None.
        """
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

    def __getitem__(self, k: _IndexType) -> Any:
        output = self.get(k)
        if isinstance(output, NestedDict):
            raise IndexError("Use get for partial indexing.")
        return output

    def __setitem__(self, k: _IndexType, v: Any) -> None:
        if not k:
            raise IndexError("Use valid index value.")
        k = _flatten_index(k)
        v = NestedDict(v) if isinstance(v, Mapping) else v
        if len(k) == 1:
            self._data[k[0]] = v
        else:
            if k[0] not in self._data:
                self._data[k[0]] = NestedDict()
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

    def __repr__(self) -> str:
        return repr(self.asdict())

    def filter(
        self,
        other: Union[Sequence[StreamType], "NestedDict"],
        ignore_missing: bool = False,
    ) -> "NestedDict":
        """Returns a NestedDict with only entries present in `other`. 
        The values in the `other` NestedDict are ignored. Only the keys are used.

        Args:
            other: a NestedDict or a sequence of keys to filter by.
            ignore_missing: if True, ignore missing keys in `other`.
        
        Returns:
            A NestedDict with only keys present in `other`.
        """
        output = NestedDict()
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
        """Returns a dictionary representation of the NestedDict."""
        output = dict()
        for k, v in self._data.items():
            if isinstance(v, NestedDict):
                output[k] = v.asdict()
            else:
                output[k] = v
        return output

    def copy(self) -> "NestedDict":
        output = NestedDict()
        for k, v in self.items():
            output[k] = v
        return output

    def __copy__(self) -> "NestedDict":
        return self.copy()

    def shallow_keys(self) -> AbstractSet[str]:
        """Returns a set of the keys at the top level of the NestedDict."""
        return self._data.keys()
