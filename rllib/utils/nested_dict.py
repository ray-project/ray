"""Custom NestedDict datatype."""

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

from collections import abc

from ray.rllib.utils.annotations import DeveloperAPI


SeqStrType = Union[str, Sequence[str]]
T = TypeVar("T")

_NestedDictType = Dict[str, Any]
_NestedMappingType = Mapping[SeqStrType, Any]

NestedDictInputType = Union[
    Iterable[Tuple[SeqStrType, T]], _NestedMappingType, "NestedDict[T]"
]


def _flatten_index(index: SeqStrType) -> Sequence[str]:
    if isinstance(index, str):
        return (index,)
    else:
        return tuple(itertools.chain.from_iterable([_flatten_index(y) for y in index]))


@DeveloperAPI
class StrKey(str):
    """A string that can be compared to a string or sequence of strings representing a
    SeqStrType. This is needed for the tree functions to work.
    """

    def __lt__(self, other: SeqStrType):
        if isinstance(other, str):
            return str(self) < other
        else:
            return (self,) < tuple(other)

    def __gt__(self, other: SeqStrType):
        if isinstance(other, str):
            return str(self) > other
        else:
            return (self,) > tuple(other)


@DeveloperAPI
class NestedDict(Generic[T], MutableMapping[str, Union[T, "NestedDict"]]):
    """A nested dict type:
        * The nested dict gives access to nested elements as a sequence of
        strings.
        * These nested dicts can also be used to filter a superset into a subset of
        nested elements with the filter function.
        * This can be instantiated with any mapping of strings, or an iterable of
        key value tuples where the values can themselves be recursively the values
        that a nested dict can take.

    Args:
        x: a representation of a nested dict: it can be an iterable of `SeqStrType`
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
            >>> foo_dict['b', 'e'] = {}     # foo_dict = {'a': 100,
            >>>                             #            'b': {'c': 200, 'd': 300}}
            >>> # Getting elements, possibly nested:
            >>> print(foo_dict['b', 'c'])   # 200
            >>> print(foo_dict['b'])        # {'c': 200, 'd': 300}
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
        # shallow dict
        self._data = dict()  # type: Dict[str, Union[T, NestedDict[T]]]
        x = x or {}
        if isinstance(x, NestedDict):
            self._data = x._data
        elif isinstance(x, abc.Mapping):
            for k in x:
                self[k] = x[k]
        elif isinstance(x, abc.Sequence):
            for k, v in x:
                self[k] = v
        else:
            raise ValueError(f"Input must be a Mapping or Iterable, got {type(x)}.")

    def __contains__(self, k: SeqStrType) -> bool:
        k = _flatten_index(k)

        data_ptr = self._data  # type: Dict[str, Any]
        for key in k:
            # this is to avoid the recursion on __contains__
            if isinstance(data_ptr, NestedDict):
                data_ptr = data_ptr._data
            if not isinstance(data_ptr, Mapping) or key not in data_ptr:
                return False
            data_ptr = data_ptr[key]

        return True

    def get(
        self, k: SeqStrType, default: Optional[T] = None
    ) -> Union[T, "NestedDict[T]"]:
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
        k = _flatten_index(k)

        if k not in self:
            if default is not None:
                return default
            else:
                raise KeyError(k)

        data_ptr = self._data
        for key in k:
            # This is to avoid the recursion on __getitem__
            if isinstance(data_ptr, NestedDict):
                data_ptr = data_ptr._data
            data_ptr = data_ptr[key]
        return data_ptr

    def __getitem__(self, k: SeqStrType) -> T:
        output = self.get(k)
        return output

    def __setitem__(self, k: SeqStrType, v: Union[T, _NestedMappingType]) -> None:
        """This is a zero-copy operation. The pointer to value if preserved in the
        internal data structure."""
        if isinstance(v, Mapping) and len(v) == 0:
            return
        if not k:
            raise IndexError(
                f"Key for {self.__class__.__name__} cannot be empty. Got {k}."
            )
        k = _flatten_index(k)
        v = self.__class__(v) if isinstance(v, Mapping) else v
        data_ptr = self._data
        for k_indx, key in enumerate(k):
            # this is done to avoid recursion over __setitem__
            if isinstance(data_ptr, NestedDict):
                data_ptr = data_ptr._data
            if k_indx == len(k) - 1:
                data_ptr[key] = v
            elif key not in data_ptr:
                data_ptr[key] = self.__class__()
            data_ptr = data_ptr[key]

    def __iter__(self) -> Iterator[SeqStrType]:
        data_ptr = self._data
        # do a DFS to get all the keys
        stack = [((StrKey(k),), v) for k, v in data_ptr.items()]
        while stack:
            k, v = stack.pop(0)
            if isinstance(v, NestedDict):
                stack = [(k + (StrKey(k2),), v) for k2, v in v._data.items()] + stack
            else:
                yield tuple(k)

    def __delitem__(self, k: SeqStrType) -> None:
        ks, ns = [], []
        data_ptr = self._data
        for k in _flatten_index(k):
            if isinstance(data_ptr, NestedDict):
                data_ptr = data_ptr._data
            if k not in data_ptr:
                raise KeyError(str(ks + [k]))
            ks.append(k)
            ns.append(data_ptr)
            data_ptr = data_ptr[k]

        del ns[-1][ks[-1]]

        for i in reversed(range(len(ks) - 1)):
            if not ns[i + 1]:
                del ns[i][ks[i]]

    def __len__(self) -> int:
        """Returns the number of leaf nodes in the `NestedDict` that
        are not of type Mappings.
        """

        # do a DFS to count the number of leaf nodes
        count = 0
        stack = [self._data]
        while stack:
            node = stack.pop()
            if isinstance(node, NestedDict):
                node = node._data
            if isinstance(node, Mapping):
                stack.extend(node.values())
            else:
                count += 1

        return count

    def __str__(self) -> str:
        return str(self.asdict())

    def __repr__(self) -> str:
        return f"NestedDict({repr(self._data)})"

    def filter(
        self,
        other: Union[Sequence[SeqStrType], "NestedDict"],
        ignore_missing: bool = False,
    ) -> "NestedDict[T]":
        """Returns a NestedDict with only entries present in `other`.
        The values in the `other` NestedDict are ignored. Only the keys are used.

        Args:
            other: a NestedDict or a sequence of keys to filter by.
            ignore_missing: if True, ignore missing keys in `other`.

        Returns:
            A NestedDict with only keys present in `other`.
        """
        output = self.__class__()
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

    def copy(self) -> "NestedDict[T]":
        """Returns a shallow copy of the NestedDict."""
        return NestedDict(self.items())

    def __copy__(self) -> "NestedDict[T]":
        return self.copy()

    def shallow_keys(self) -> AbstractSet[str]:
        """Returns a set of the keys at the top level of the NestedDict."""
        return self._data.keys()
