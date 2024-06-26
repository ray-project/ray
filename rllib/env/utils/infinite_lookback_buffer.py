from typing import Any, List, Optional, Union

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.utils.numpy import LARGE_INTEGER, one_hot, one_hot_multidiscrete
from ray.rllib.utils.spaces.space_utils import (
    batch,
    get_dummy_batch_for_space,
    get_base_struct_from_space,
)


class InfiniteLookbackBuffer:
    @property
    def space(self):
        return self._space

    @space.setter
    def space(self, value):
        self._space = value
        self.space_struct = get_base_struct_from_space(value)

    def __init__(
        self,
        data: Optional[Union[List, np.ndarray]] = None,
        lookback: int = 0,
        space: Optional[gym.Space] = None,
    ):
        self.data = data if data is not None else []
        self.lookback = min(lookback, len(self.data))
        self.finalized = not isinstance(self.data, list)
        self.space_struct = None
        self.space = space

    def append(self, item) -> None:
        """Appends the given item to the end of this buffer."""
        if self.finalized:
            self.data = tree.map_structure(
                lambda d, i: np.concatenate([d, [i]], axis=0), self.data, item
            )
        else:
            self.data.append(item)

    def extend(self, items) -> None:
        """Appends all items in `items` to the end of this buffer."""
        if self.finalized:
            # TODO (sven): When extending with a list of structs, we should
            #  probably rather do: `tree.map_structure(..., self.data,
            #  tree.map_structure(lambda *s: np.array(*s), *items)`)??
            self.data = tree.map_structure(
                lambda d, i: np.concatenate([d, i], axis=0), self.data, np.array(items)
            )
        else:
            for item in items:
                self.append(item)

    def concat(self, other: "InfiniteLookbackBuffer") -> None:
        """Concatenates the data of `other` (w/o its lookback) to `self`.

        Args:
            other: The other InfiniteLookbackBuffer to be concatenated to self.
        """
        self.data.extend(other.get())

    def pop(self, index: int = -1) -> None:
        """Removes the item at `index` from this buffer, but does NOT return it.

        Args:
            index: The index to pop out of this buffer (w/o returning it from this
                method).
        """
        if self.finalized:
            self.data = tree.map_structure(
                lambda s: np.delete(s, index, axis=0), self.data
            )
        else:
            self.data.pop(index)

    def finalize(self) -> None:
        """Finalizes this buffer by converting internal data lists into numpy arrays.

        Thereby, if the individual items in the list are nested structures, the
        resulting buffer content will be a nested struct of np.ndarrays (leafs).
        """
        if not self.finalized:
            self.data = batch(self.data)
            self.finalized = True

    def get(
        self,
        indices: Optional[Union[int, slice, List[int]]] = None,
        *,
        neg_indices_left_of_zero: bool = False,
        fill: Optional[Any] = None,
        one_hot_discrete: bool = False,
        _ignore_last_ts: bool = False,
        _add_last_ts_value: Optional[Any] = None,
    ) -> Any:
        """Returns data, based on the given args, from this buffer.

        Args:
            indices: A single int is interpreted as an index, from which to return the
                individual data stored at this index.
                A list of ints is interpreted as a list of indices from which to gather
                individual data in a batch of size len(indices).
                A slice object is interpreted as a range of data to be returned.
                Thereby, negative indices by default are interpreted as "before the end"
                unless the `neg_indices_left_of_zero=True` option is used, in which case
                negative indices are interpreted as "before ts=0", meaning going back
                into the lookback buffer.
            neg_indices_left_of_zero: If True, negative values in `indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, a buffer with data [4, 5, 6,  7, 8, 9],
                where [4, 5, 6] is the lookback buffer range (ts=0 item is 7), will
                respond to `get(-1, neg_indices_left_of_zero=True)` with `6` and to
                `get(slice(-2, 1), neg_indices_left_of_zero=True)` with `[5, 6,  7]`.
            fill: An optional float value to use for filling up the returned results at
                the boundaries. This filling only happens if the requested index range's
                start/stop boundaries exceed the buffer's boundaries (including the
                lookback buffer on the left side). This comes in very handy, if users
                don't want to worry about reaching such boundaries and want to zero-pad.
                For example, a buffer with data [10, 11,  12, 13, 14] and lookback
                buffer size of 2 (meaning `10` and `11` are part of the lookback buffer)
                will respond to `get(slice(-7, -2), fill=0.0)`
                with `[0.0, 0.0, 10, 11, 12]`.
            one_hot_discrete: If True, will return one-hot vectors (instead of
                int-values) for those sub-components of a (possibly complex) space
                that are Discrete or MultiDiscrete. Note that if `fill=0` and the
                requested `indices` are out of the range of our data, the returned
                one-hot vectors will actually be zero-hot (all slots zero).
            _ignore_last_ts: Whether to ignore the last record in our internal
                `self.data` when getting the provided indices.
            _add_last_ts_value: Whether to add the value of this arg to the end of
                the internal `self.data` buffer (just for the duration of this get
                operation, not permanently).
        """
        if indices is None:
            data = self._get_all_data(
                one_hot_discrete=one_hot_discrete,
                _ignore_last_ts=_ignore_last_ts,
            )
        elif isinstance(indices, slice):
            data = self._get_slice(
                indices,
                fill=fill,
                neg_indices_left_of_zero=neg_indices_left_of_zero,
                one_hot_discrete=one_hot_discrete,
                _ignore_last_ts=_ignore_last_ts,
                _add_last_ts_value=_add_last_ts_value,
            )
        elif isinstance(indices, list):
            data = [
                self._get_int_index(
                    idx,
                    fill=fill,
                    neg_indices_left_of_zero=neg_indices_left_of_zero,
                    one_hot_discrete=one_hot_discrete,
                    _ignore_last_ts=_ignore_last_ts,
                    _add_last_ts_value=_add_last_ts_value,
                )
                for idx in indices
            ]
            if self.finalized:
                data = batch(data)
        else:
            assert isinstance(indices, int)
            data = self._get_int_index(
                indices,
                fill=fill,
                neg_indices_left_of_zero=neg_indices_left_of_zero,
                one_hot_discrete=one_hot_discrete,
                _ignore_last_ts=_ignore_last_ts,
                _add_last_ts_value=_add_last_ts_value,
            )

        return data

    def __add__(
        self, other: Union[List, "InfiniteLookbackBuffer", int, float, complex]
    ) -> "InfiniteLookbackBuffer":
        """Adds another InfiniteLookbackBuffer object or list to the end of this one.

        Args:
            other: Another `InfiniteLookbackBuffer` or a `list` or a number.
                If a `InfiniteLookbackBuffer` its data (w/o its lookback buffer) gets
                concatenated to self's data. If a `list`, we concat it to self's data.
                If a number, we add this number to each element of self (if possible).

        Returns:
            A new `InfiniteLookbackBuffer` instance `self.data` containing
            concatenated data from `self` and `other` (or adding `other` to each element
            in self's data).
        """

        if self.finalized:
            raise RuntimeError(f"Cannot `add` to a finalized {type(self).__name__}.")
        else:
            # If `other` is an int, simply add it to all our values (if possible) and
            # use the result as the underlying data for the returned buffer.
            if isinstance(other, (int, float, complex)):
                data = [
                    (d + other) if isinstance(d, (int, float, complex)) else d
                    for d in self.data
                ]
            # If `other` is a InfiniteLookbackBuffer itself, do NOT include its
            # lookback buffer anymore. We assume that `other`'s lookback buffer i
            # already at the end of `self`.
            elif isinstance(other, InfiniteLookbackBuffer):
                data = self.data + other.data[other.lookback :]
            # `other` is a list, simply concat the two lists and use the result as
            # the underlying data for the returned buffer.
            else:
                data = self.data + other

            return InfiniteLookbackBuffer(
                data=data,
                lookback=self.lookback,
                space=self.space,
            )

    def __getitem__(self, item):
        """Support squared bracket syntax, e.g. buffer[:5]."""
        return self.get(item)

    def __setitem__(self, key, value):
        self.set(new_data=value, at_indices=key)

    def set(
        self,
        new_data,
        *,
        at_indices: Optional[Union[int, slice, List[int]]] = None,
        neg_indices_left_of_zero: bool = False,
    ) -> None:
        """Overwrites all or some of the data in this buffer with the provided data.

        Args:
            new_data: The new data to overwrite existing records with.
            at_indices: A single int is interpreted as an index, at which to overwrite
                the individual record stored at this index with `new_data`.
                A list of ints is interpreted as a list of indices, which to overwrite
                with `new_data`, which must be a batch of size `len(at_indices)`.
                A slice object is interpreted as a range, which to overwrite with
                `new_data`. Thereby, negative indices by default are interpreted as
                "before the end" unless the `neg_indices_left_of_zero=True` option is
                used, in which case negative indices are interpreted as
                "before ts=0", meaning going back into the lookback buffer.
            neg_indices_left_of_zero: If True, negative values in `at_indices` are
                interpreted as "before ts=0", meaning going back into the lookback
                buffer. For example, a buffer with data [4, 5, 6,  7, 8, 9],
                where [4, 5, 6] is the lookback buffer range (ts=0 item is 7), will
                handle a call `set(99, at_indices=-1, neg_indices_left_of_zero=True)`
                with `6` being replaced by 99 and to `set([98, 99, 100],
                at_indices=slice(-2, 1), neg_indices_left_of_zero=True)` with
                `[5, 6,  7]` being replaced by `[98, 99,  100]`.
        """
        # `at_indices` is None -> Override all our data (excluding the lookback buffer).
        if at_indices is None:
            self._set_all_data(new_data)

        elif isinstance(at_indices, slice):
            self._set_slice(
                new_data,
                slice_=at_indices,
                neg_indices_left_of_zero=neg_indices_left_of_zero,
            )
        elif isinstance(at_indices, list):
            for i, idx in enumerate(at_indices):
                self._set_int_index(
                    new_data[i],
                    idx=idx,
                    neg_indices_left_of_zero=neg_indices_left_of_zero,
                )
        else:
            assert isinstance(at_indices, int)
            self._set_int_index(
                new_data,
                idx=at_indices,
                neg_indices_left_of_zero=neg_indices_left_of_zero,
            )

    def __len__(self):
        """Return the length of our data, excluding the lookback buffer."""
        len_ = self.len_incl_lookback()
        # Only count the data after the lookback.
        return max(len_ - self.lookback, 0)

    def len_incl_lookback(self):
        if self.finalized:
            return len(tree.flatten(self.data)[0])
        else:
            return len(self.data)

    def __repr__(self):
        return (
            f"{type(self).__name__}({self.data[:self.lookback]} <- "
            f"lookback({self.lookback}) | {self.data[self.lookback:]})"
        )

    def _get_all_data(self, one_hot_discrete=False, _ignore_last_ts=False):
        data = self[: (None if not _ignore_last_ts else -1)]
        if one_hot_discrete:
            data = self._one_hot(data, space_struct=self.space_struct)
        return data

    def _set_all_data(self, new_data):
        self._set_slice(new_data, slice(0, None))

    def _get_slice(
        self,
        slice_,
        fill=None,
        neg_indices_left_of_zero=False,
        one_hot_discrete=False,
        _ignore_last_ts=False,
        _add_last_ts_value=None,
    ):
        data_to_use = self.data
        if _ignore_last_ts:
            if self.finalized:
                data_to_use = tree.map_structure(lambda s: s[:-1], self.data)
            else:
                data_to_use = self.data[:-1]
        if _add_last_ts_value is not None:
            if self.finalized:
                data_to_use = tree.map_structure(
                    lambda s, t: np.append(s, t),
                    data_to_use.copy(),
                    _add_last_ts_value,
                )
            else:
                data_to_use = np.append(data_to_use.copy(), _add_last_ts_value)

        slice_, slice_len, fill_left_count, fill_right_count = self._interpret_slice(
            slice_,
            neg_indices_left_of_zero,
            len_self_plus_lookback=(
                self.len_incl_lookback()
                + int(_add_last_ts_value is not None)
                - int(_ignore_last_ts)
            ),
        )

        # Perform the actual slice.
        data_slice = None
        if slice_len > 0:
            if self.finalized:
                data_slice = tree.map_structure(lambda s: s[slice_], data_to_use)
            else:
                data_slice = data_to_use[slice_]

            if one_hot_discrete:
                data_slice = self._one_hot(data_slice, space_struct=self.space_struct)

        # Data is shorter than the range requested -> Fill the rest with `fill` data.
        if fill is not None and (fill_right_count > 0 or fill_left_count > 0):
            if self.finalized:
                if fill_left_count:
                    if self.space is None:
                        fill_batch = np.array([fill] * fill_left_count)
                    else:
                        fill_batch = get_dummy_batch_for_space(
                            self.space,
                            fill_value=fill,
                            batch_size=fill_left_count,
                            one_hot_discrete=one_hot_discrete,
                        )
                    if data_slice is not None:
                        data_slice = tree.map_structure(
                            lambda s0, s: np.concatenate([s0, s]),
                            fill_batch,
                            data_slice,
                        )
                    else:
                        data_slice = fill_batch
                if fill_right_count:
                    if self.space is None:
                        fill_batch = np.array([fill] * fill_right_count)
                    else:
                        fill_batch = get_dummy_batch_for_space(
                            self.space,
                            fill_value=fill,
                            batch_size=fill_right_count,
                            one_hot_discrete=one_hot_discrete,
                        )
                    if data_slice is not None:
                        data_slice = tree.map_structure(
                            lambda s0, s: np.concatenate([s, s0]),
                            fill_batch,
                            data_slice,
                        )
                    else:
                        data_slice = fill_batch

            else:
                if self.space is None:
                    fill_batch = [fill]
                else:
                    fill_batch = [
                        get_dummy_batch_for_space(
                            self.space,
                            fill_value=fill,
                            batch_size=0,
                            one_hot_discrete=one_hot_discrete,
                        )
                    ]
                data_slice = (
                    fill_batch * fill_left_count
                    + (data_slice if data_slice is not None else [])
                    + fill_batch * fill_right_count
                )

        if data_slice is None:
            if self.finalized:
                return tree.map_structure(lambda s: s[slice_], data_to_use)
            else:
                return data_to_use[slice_]
        return data_slice

    def _set_slice(
        self,
        new_data,
        slice_,
        neg_indices_left_of_zero=False,
    ):
        slice_, _, _, _ = self._interpret_slice(slice_, neg_indices_left_of_zero)

        # Check, whether the setting to new_data changes the length of self
        # (it shouldn't). If it does, raise an error.
        try:
            if self.finalized:

                def __set(s, n):
                    if self.space:
                        assert self.space.contains(n[0])
                    assert len(s[slice_]) == len(n)
                    s[slice_] = n

                tree.map_structure(__set, self.data, new_data)
            else:
                assert len(self.data[slice_]) == len(new_data)
                self.data[slice_] = new_data
        except AssertionError:
            raise IndexError(
                f"Cannot `set()` value via at_indices={slice_} (option "
                f"neg_indices_left_of_zero={neg_indices_left_of_zero})! Slice of data "
                "does NOT have the same size as `new_data`."
            )

    def _get_int_index(
        self,
        idx: int,
        fill=None,
        neg_indices_left_of_zero=False,
        one_hot_discrete=False,
        _ignore_last_ts=False,
        _add_last_ts_value=None,
    ):
        data_to_use = self.data
        if _ignore_last_ts:
            data_to_use = self.data[:-1]
        if _add_last_ts_value is not None:
            data_to_use = np.append(data_to_use.copy(), _add_last_ts_value)

        # If index >= 0 -> Ignore lookback buffer.
        # Otherwise, include lookback buffer.
        if idx >= 0 or neg_indices_left_of_zero:
            idx = self.lookback + idx
        # Negative indices mean: Go to left into lookback buffer starting from idx=0.
        # But if we pass the lookback buffer, the index should be invalid and we will
        # have to fill, if required. Invalidate the index by setting it to one larger
        # than max.
        if neg_indices_left_of_zero and idx < 0:
            idx = len(self) + self.lookback - (_ignore_last_ts is True)

        try:
            if self.finalized:
                data = tree.map_structure(lambda s: s[idx], data_to_use)
            else:
                data = data_to_use[idx]
        # Out of range index -> If `fill`, use a fill dummy (B=0), if not, error out.
        except IndexError as e:
            if fill is not None:
                if self.space is None:
                    return fill
                return get_dummy_batch_for_space(
                    self.space,
                    fill_value=fill,
                    batch_size=0,
                    one_hot_discrete=one_hot_discrete,
                )
            else:
                raise e

        # Convert discrete/multi-discrete components to one-hot vectors, if required.
        if one_hot_discrete:
            data = self._one_hot(data, self.space_struct)
        return data

    def _set_int_index(self, new_data, idx, neg_indices_left_of_zero):
        actual_idx = idx
        # If index >= 0 -> Ignore lookback buffer.
        # Otherwise, include lookback buffer.
        if actual_idx >= 0 or neg_indices_left_of_zero:
            actual_idx = self.lookback + actual_idx
        # Negative indices mean: Go to left into lookback buffer starting from idx=0.
        # But if we pass the lookback buffer, the index should be invalid and we will
        # have to fill, if required. Invalidate the index by setting it to one larger
        # than max.
        if neg_indices_left_of_zero and actual_idx < 0:
            actual_idx = len(self) + self.lookback

        try:
            if self.finalized:

                def __set(s, n):
                    if self.space:
                        assert self.space.contains(n), n
                    s[actual_idx] = n

                tree.map_structure(__set, self.data, new_data)
            else:
                self.data[actual_idx] = new_data
        except IndexError:
            raise IndexError(
                f"Cannot `set()` value at index {idx} (option "
                f"neg_indices_left_of_zero={neg_indices_left_of_zero})! Out of range "
                f"of buffer data."
            )

    def _interpret_slice(
        self,
        slice_,
        neg_indices_left_of_zero,
        len_self_plus_lookback=None,
    ):
        if len_self_plus_lookback is None:
            len_self_plus_lookback = len(self) + self.lookback

        # Re-interpret slice bounds as absolute positions (>=0) within our
        # internal data.
        start = slice_.start
        stop = slice_.stop

        # Start is None -> Exclude lookback buffer.
        if start is None:
            start = self.lookback
        # Start is negative.
        elif start < 0:
            # `neg_indices_left_of_zero=True` -> User wants to index into the lookback
            # range.
            if neg_indices_left_of_zero:
                start = self.lookback + start
            # Interpret index as counting "from end".
            else:
                start = len_self_plus_lookback + start
        # Start is 0 or positive -> timestep right after lookback is interpreted as 0.
        else:
            start = self.lookback + start

        # Stop is None -> Set stop to very last index + 1 of our internal data.
        if stop is None:
            stop = len_self_plus_lookback
        # Stop is negative.
        elif stop < 0:
            # `neg_indices_left_of_zero=True` -> User wants to index into the lookback
            # range. Set to 0 (beginning of lookback buffer) if result is a negative
            # index.
            if neg_indices_left_of_zero:
                stop = self.lookback + stop
            # Interpret index as counting "from end". Set to 0 (beginning of actual
            # episode) if result is a negative index.
            else:
                stop = len_self_plus_lookback + stop
        # Stop is positive -> Add lookback range to it.
        else:
            stop = self.lookback + stop

        fill_left_count = fill_right_count = 0
        # Both start and stop are on left side.
        if start < 0 and stop < 0:
            fill_left_count = abs(start - stop)
            fill_right_count = 0
            start = stop = 0
        # Both start and stop are on right side.
        elif start >= len_self_plus_lookback and stop >= len_self_plus_lookback:
            fill_right_count = abs(start - stop)
            fill_left_count = 0
            start = stop = len_self_plus_lookback
        # Set to 0 (beginning of actual episode) if result is a negative index.
        elif start < 0:
            fill_left_count = -start
            start = 0
        elif stop >= len_self_plus_lookback:
            fill_right_count = stop - len_self_plus_lookback
            stop = len_self_plus_lookback
        # Only `stop` might be < 0, when slice has negative step and start is > 0.
        elif stop < 0:
            if start >= len_self_plus_lookback:
                fill_left_count = start - len_self_plus_lookback + 1
                start = len_self_plus_lookback - 1
            fill_right_count = -stop - 1
            stop = -LARGE_INTEGER

        assert start >= 0 and (stop >= 0 or stop == -LARGE_INTEGER), (start, stop)

        step = slice_.step if slice_.step is not None else 1
        slice_ = slice(start, stop, step)
        slice_len = max(0, (stop - start + (step - (1 if step > 0 else -1))) // step)
        return slice_, slice_len, fill_left_count, fill_right_count

    def _one_hot(self, data, space_struct):
        if space_struct is None:
            raise ValueError(
                f"Cannot `one_hot` data in `{type(self).__name__}` if a "
                "gym.Space was NOT provided during construction!"
            )

        def _convert(dat_, space):
            if isinstance(space, gym.spaces.Discrete):
                return one_hot(dat_, depth=space.n)
            elif isinstance(space, gym.spaces.MultiDiscrete):
                return one_hot_multidiscrete(dat_, depths=space.nvec)
            return dat_

        if isinstance(data, list):
            data = [
                tree.map_structure(_convert, dslice, space_struct) for dslice in data
            ]
        else:
            data = tree.map_structure(_convert, data, space_struct)
        return data
