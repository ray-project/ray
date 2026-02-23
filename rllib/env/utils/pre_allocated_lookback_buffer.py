"""Pre-allocated lookback buffer for efficient episode finalization.

``InfiniteLookbackBuffer.finalize()`` converts an internal Python list to a
batched numpy struct via ``batch()``, which is O(n) and allocates a second
full copy of the data.  ``PreAllocatedLookbackBuffer`` allocates numpy arrays
up-front and writes samples directly into them.  ``finalize()`` then becomes a
near-free trim of the excess capacity.
"""
from typing import Any, Dict, List, Optional

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree
from gymnasium.spaces import Graph, OneOf, Sequence, Text
from gymnasium.utils.env_checker import data_equivalence

from ray.rllib.env.utils.lookback_buffer_base import BaseLookbackBuffer
from ray.rllib.env.utils.space_utils import (
    create_mutable_array,
    read_from_buffer,
    write_to_buffer,
)
from ray.rllib.utils.numpy import LARGE_INTEGER, one_hot, one_hot_multidiscrete
from ray.rllib.utils.serialization import gym_space_from_dict, gym_space_to_dict
from ray.rllib.utils.spaces.space_utils import (
    batch,
    get_base_struct_from_space,
    get_dummy_batch_for_space,
)
from ray.util.annotations import DeveloperAPI

# Space types whose buffers are Python lists rather than numpy arrays.
_LIST_SPACES = (Graph, Text, Sequence, OneOf)


@DeveloperAPI
class PreAllocatedLookbackBuffer(BaseLookbackBuffer):
    """A lookback buffer that pre-allocates numpy arrays for O(1) finalize().

    Unlike :class:`InfiniteLookbackBuffer`, which collects samples in a Python
    list and batch-converts them on ``finalize()``, this buffer allocates a
    numpy array up-front and writes samples directly into it.  ``finalize()``
    trims excess capacity rather than performing a full ``batch()`` conversion.

    Requires a ``gym.Space`` at construction time.  If no space is available,
    fall back to :class:`InfiniteLookbackBuffer`.

    Args:
        data: Optional initial data as a Python list of samples.
        lookback: Number of initial items to treat as lookback buffer.
        space: The gymnasium observation space.  **Required.**
        initial_capacity: Number of rows to pre-allocate in each leaf array.
    """

    def __init__(
        self,
        data: Optional[List] = None,
        lookback: int = 0,
        space: Optional[gym.Space] = None,
        initial_capacity: int = 40,
    ):
        if space is None:
            raise ValueError(
                "PreAllocatedLookbackBuffer requires `space` to be provided. "
                "If space is not available use InfiniteLookbackBuffer instead."
            )

        # Initialise space property (sets _space and _space_struct).
        self._space: Optional[gym.Space] = None
        self._space_struct = None
        self.space = space

        # Always in batch (numpy) format.
        self.finalized = False
        # True while we can write into pre-allocated slots.
        self._growing = True

        if data is not None and len(data) > 0:
            n = len(data)
            self._capacity = max(initial_capacity, n * 2)
            self._length = n
            self.lookback = min(lookback, n)
            self.data = create_mutable_array(space, n=self._capacity)
            # `data` may be a Python list of individual samples (from the episode
            # constructor) or an already-batched struct-of-arrays (from another
            # buffer's .get() call, e.g. in _slice()).  Only call batch() for lists.
            batched = batch(data) if isinstance(data, list) else data
            write_to_buffer(space, self.data, slice(0, n), batched)
        else:
            self._capacity = initial_capacity
            self._length = 0
            self.lookback = 0
            self.data = create_mutable_array(space, n=self._capacity)

    # ------------------------------------------------------------------
    # Space property
    # ------------------------------------------------------------------

    @property
    def space(self) -> Optional[gym.Space]:
        return self._space

    @space.setter
    def space(self, value: Optional[gym.Space]) -> None:
        self._space = value
        self._space_struct = get_base_struct_from_space(value)

    @property
    def space_struct(self):
        return self._space_struct

    # ------------------------------------------------------------------
    # Length / capacity
    # ------------------------------------------------------------------

    def len_incl_lookback(self) -> int:
        """Return valid data length (incl. lookback), NOT allocated capacity."""
        return self._length

    def __len__(self) -> int:
        """Return the length of our data, excluding the lookback buffer."""
        return max(self._length - self.lookback, 0)

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def append(self, item) -> None:
        """Appends *item* to the buffer.

        While growing: writes directly into the next pre-allocated slot.
        After ``finalize()`` (sealed mode): falls back to ``np.concatenate``
        — this path is rare (replay-buffer merge) and acceptable.
        """
        if not self._growing:
            # Sealed: concatenate (rare post-finalize merge path).
            self.data = tree.map_structure(
                lambda d, i: np.concatenate([d, np.array([i])], axis=0),
                self.data,
                item,
            )
            self._length += 1
            self._capacity += 1
            return

        if self._length >= self._capacity:
            self._grow()

        write_to_buffer(self.space, self.data, self._length, item)
        self._length += 1

    def extend(self, items) -> None:
        """Appends all items in *items* to the end of this buffer."""
        for item in items:
            self.append(item)

    def concat(self, other: "BaseLookbackBuffer") -> None:
        """Concatenates the data of *other* (w/o its lookback) to self."""
        self.extend(other.get())

    def pop(self, index: int = -1) -> None:
        """Removes the item at *index* from this buffer (does NOT return it).

        Matches finalized ``InfiniteLookbackBuffer`` semantics: *index* is a
        direct array index (negative means from end of valid data).
        """
        abs_idx = self._length + index if index < 0 else index
        if abs_idx < 0 or abs_idx >= self._length:
            raise IndexError(
                f"pop index {index} is out of range for buffer length "
                f"{self._length}."
            )
        # Shift elements [abs_idx+1 .. _length-1] one position to the left.
        if abs_idx < self._length - 1:
            tail = read_from_buffer(
                self.space, self.data, slice(abs_idx + 1, self._length)
            )
            write_to_buffer(
                self.space, self.data, slice(abs_idx, self._length - 1), tail
            )
        self._length -= 1

    def finalize(self) -> None:
        """Trim excess capacity so the buffer contains exactly valid rows.

        After ``finalize()``, ``_growing=False`` and ``_capacity==_length``.
        Subsequent ``append()`` calls fall back to ``np.concatenate`` (the
        rare post-finalize merge path).
        """
        self.finalized = True
        if self._growing:
            if self._capacity > self._length:
                trimmed = create_mutable_array(self.space, n=self._length)
                if self._length > 0:
                    write_to_buffer(
                        self.space,
                        trimmed,
                        slice(0, self._length),
                        read_from_buffer(self.space, self.data, slice(0, self._length)),
                    )
                self.data = trimmed
                self._capacity = self._length
            self._growing = False

    def _grow(self) -> None:
        """Double the allocated capacity and migrate existing valid data."""
        new_capacity = max(self._capacity * 2, 1)
        new_data = create_mutable_array(self.space, n=new_capacity)
        if self._length > 0:
            write_to_buffer(
                self.space,
                new_data,
                slice(0, self._length),
                read_from_buffer(self.space, self.data, slice(0, self._length)),
            )
        self.data = new_data
        self._capacity = new_capacity

    # ------------------------------------------------------------------
    # Data view helper
    # ------------------------------------------------------------------

    def _get_base_data(self):
        """Return data trimmed to the *_length* valid rows.

        For numpy-leaf spaces this returns numpy *views* (no copy).  For
        list-based spaces (Graph / Text / Sequence / OneOf) it returns a
        shallow list slice.

        When ``_growing=False`` and ``_capacity==_length`` (the normal sealed
        state), we return ``self.data`` directly with no overhead.  The extra
        ``_capacity > _length`` guard also handles the edge case where
        ``pop()`` is called in sealed mode, leaving a single orphaned slot
        at the end of the backing array.
        """
        if self._capacity > self._length:
            if isinstance(self.space, _LIST_SPACES):
                # self.data is a flat Python list; plain slice avoids dm-tree
                # descending into list elements.
                return self.data[: self._length]
            return tree.map_structure(lambda s: s[: self._length], self.data)
        return self.data

    # ------------------------------------------------------------------
    # get / set  (mirrors InfiniteLookbackBuffer finalized=True branches)
    # ------------------------------------------------------------------

    def get(
        self,
        indices=None,
        *,
        neg_index_as_lookback: bool = False,
        fill=None,
        one_hot_discrete: bool = False,
        _ignore_last_ts: bool = False,
        _add_last_ts_value=None,
    ) -> Any:
        """Returns data from this buffer.  Identical API to
        :meth:`InfiniteLookbackBuffer.get`.
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
                neg_index_as_lookback=neg_index_as_lookback,
                one_hot_discrete=one_hot_discrete,
                _ignore_last_ts=_ignore_last_ts,
                _add_last_ts_value=_add_last_ts_value,
            )
        elif isinstance(indices, list):
            data = [
                self._get_int_index(
                    idx,
                    fill=fill,
                    neg_index_as_lookback=neg_index_as_lookback,
                    one_hot_discrete=one_hot_discrete,
                    _ignore_last_ts=_ignore_last_ts,
                    _add_last_ts_value=_add_last_ts_value,
                )
                for idx in indices
            ]
            data = batch(data)
        else:
            assert isinstance(indices, int)
            data = self._get_int_index(
                indices,
                fill=fill,
                neg_index_as_lookback=neg_index_as_lookback,
                one_hot_discrete=one_hot_discrete,
                _ignore_last_ts=_ignore_last_ts,
                _add_last_ts_value=_add_last_ts_value,
            )
        return data

    def set(
        self,
        new_data,
        *,
        at_indices=None,
        neg_index_as_lookback: bool = False,
    ) -> None:
        """Overwrites data in this buffer.  Identical API to
        :meth:`InfiniteLookbackBuffer.set`.
        """
        if at_indices is None:
            self._set_all_data(new_data)
        elif isinstance(at_indices, slice):
            self._set_slice(
                new_data,
                slice_=at_indices,
                neg_index_as_lookback=neg_index_as_lookback,
            )
        elif isinstance(at_indices, list):
            for i, idx in enumerate(at_indices):
                self._set_int_index(
                    new_data[i], idx=idx, neg_index_as_lookback=neg_index_as_lookback
                )
        else:
            assert isinstance(at_indices, int)
            self._set_int_index(
                new_data, idx=at_indices, neg_index_as_lookback=neg_index_as_lookback
            )

    def __getitem__(self, item):
        return self.get(item)

    def __setitem__(self, key, value):
        self.set(new_data=value, at_indices=key)

    def __add__(self, other):
        raise RuntimeError(
            f"Cannot `add` to a {type(self).__name__}.  "
            "Use append() / extend() / concat() instead."
        )

    def __eq__(self, other: "PreAllocatedLookbackBuffer") -> bool:
        return (
            isinstance(other, PreAllocatedLookbackBuffer)
            and data_equivalence(self._get_base_data(), other._get_base_data())
            and self.lookback == other.lookback
            and self.finalized == other.finalized
            and self.space_struct == other.space_struct
            and self.space == other.space
        )

    def __repr__(self) -> str:
        bd = self._get_base_data()
        try:
            lb_part = bd[: self.lookback]
            rest_part = bd[self.lookback :]
        except Exception:
            lb_part = rest_part = "?"
        return (
            f"{type(self).__name__}({lb_part} <- "
            f"lookback({self.lookback}) | {rest_part})"
        )

    # ------------------------------------------------------------------
    # State serialization
    # ------------------------------------------------------------------

    def get_state(self) -> Dict[str, Any]:
        """Returns the pickable state.  Serialises only valid *_length* rows
        so unused capacity is not included in the snapshot.
        """
        if self._length > 0:
            serialised_data = read_from_buffer(
                self.space, self.data, slice(0, self._length)
            )
        else:
            serialised_data = None
        return {
            "data": serialised_data,
            "lookback": self.lookback,
            "space": gym_space_to_dict(self.space) if self.space else None,
            "_length": self._length,
            "_growing": self._growing,
        }

    @classmethod
    def from_state(cls, state: Dict[str, Any]) -> "PreAllocatedLookbackBuffer":
        """Creates a new :class:`PreAllocatedLookbackBuffer` from a state dict.

        Args:
            state: The state dict, as returned by :meth:`get_state`.

        Returns:
            A new ``PreAllocatedLookbackBuffer`` with the data and metadata
            from the state dict.  The restored buffer starts in sealed mode
            (``_growing=False``) because a deserialized episode is complete.
        """
        space = gym_space_from_dict(state["space"]) if state["space"] else None
        n = state.get("_length", 0)

        buf = cls.__new__(cls)
        buf._space = None
        buf._space_struct = None
        buf.space = space
        buf.finalized = True
        buf._growing = False  # deserialized episodes are sealed
        buf._length = n
        buf._capacity = n
        buf.lookback = state["lookback"]

        if n > 0 and state.get("data") is not None:
            buf.data = state["data"]  # already a struct-of-arrays
        else:
            buf.data = create_mutable_array(space, n=0) if space is not None else None

        return buf

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

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
        neg_index_as_lookback=False,
        one_hot_discrete=False,
        _ignore_last_ts=False,
        _add_last_ts_value=None,
    ):
        # _get_base_data() returns views trimmed to _length, which is
        # essential for the _add_last_ts_value path: np.append must append
        # after the last *valid* element, not after the allocated capacity.
        base_data = self._get_base_data()
        data_to_use = base_data

        if _ignore_last_ts:
            if isinstance(self.space, _LIST_SPACES):
                data_to_use = base_data[:-1]
            else:
                data_to_use = tree.map_structure(lambda s: s[:-1], base_data)

        if _add_last_ts_value is not None:
            if isinstance(self.space, _LIST_SPACES):
                data_to_use = data_to_use + [_add_last_ts_value]
            else:
                data_to_use = tree.map_structure(
                    lambda s, t: np.append(s, t), data_to_use, _add_last_ts_value
                )

        slice_, slice_len, fill_left_count, fill_right_count = self._interpret_slice(
            slice_,
            neg_index_as_lookback,
            len_self_plus_lookback=(
                self.len_incl_lookback()
                + int(_add_last_ts_value is not None)
                - int(_ignore_last_ts)
            ),
        )

        data_slice = None
        if slice_len > 0:
            if isinstance(self.space, _LIST_SPACES):
                data_slice = data_to_use[slice_]
            else:
                data_slice = tree.map_structure(lambda s: s[slice_], data_to_use)
            if one_hot_discrete:
                data_slice = self._one_hot(data_slice, space_struct=self.space_struct)

        if fill is not None and (fill_right_count > 0 or fill_left_count > 0):
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

        if data_slice is None:
            if isinstance(self.space, _LIST_SPACES):
                return data_to_use[slice_]
            return tree.map_structure(lambda s: s[slice_], data_to_use)
        return data_slice

    def _set_slice(
        self,
        new_data,
        slice_,
        neg_index_as_lookback=False,
    ):
        slice_, _, _, _ = self._interpret_slice(slice_, neg_index_as_lookback)

        try:
            if isinstance(self.space, _LIST_SPACES):
                assert len(self.data[slice_]) == len(new_data)
                self.data[slice_] = new_data
            else:

                def __set(s, n):
                    if self.space:
                        assert self.space.contains(n[0])
                    assert len(s[slice_]) == len(n)
                    s[slice_] = n

                tree.map_structure(__set, self.data, new_data)
        except AssertionError:
            raise IndexError(
                f"Cannot `set()` value via at_indices={slice_} (option "
                f"neg_index_as_lookback={neg_index_as_lookback})! Slice of data "
                "does NOT have the same size as `new_data`."
            )

    def _get_int_index(
        self,
        idx: int,
        fill=None,
        neg_index_as_lookback=False,
        one_hot_discrete=False,
        _ignore_last_ts=False,
        _add_last_ts_value=None,
    ):
        # Same _get_base_data() fix: avoids reading uninitialized capacity rows.
        base_data = self._get_base_data()
        data_to_use = base_data

        if _ignore_last_ts:
            if isinstance(self.space, _LIST_SPACES):
                data_to_use = base_data[:-1]
            else:
                data_to_use = tree.map_structure(lambda s: s[:-1], base_data)

        if _add_last_ts_value is not None:
            if isinstance(self.space, _LIST_SPACES):
                data_to_use = data_to_use + [_add_last_ts_value]
            else:
                data_to_use = tree.map_structure(
                    lambda s, last: np.append(s, last), data_to_use, _add_last_ts_value
                )

        if idx >= 0 or neg_index_as_lookback:
            idx = self.lookback + idx
        if neg_index_as_lookback and idx < 0:
            idx = len(self) + self.lookback - (_ignore_last_ts is True)

        try:
            if isinstance(self.space, _LIST_SPACES):
                data = data_to_use[idx]
            else:
                data = tree.map_structure(lambda s: s[idx], data_to_use)
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
                raise e from ValueError(
                    f"Trying to get index {idx} from buffer of length {self._length}"
                )

        if one_hot_discrete:
            data = self._one_hot(data, self.space_struct)
        return data

    def _set_int_index(self, new_data, idx, neg_index_as_lookback):
        actual_idx = idx
        if actual_idx >= 0 or neg_index_as_lookback:
            actual_idx = self.lookback + actual_idx
        elif actual_idx < 0:
            # Negative index (not lookback): resolve against valid data length so
            # that e.g. -1 refers to the last *filled* element, not the last
            # element of the (potentially over-allocated) backing array.
            actual_idx = self._length + actual_idx
        if neg_index_as_lookback and actual_idx < 0:
            actual_idx = len(self) + self.lookback

        try:
            if isinstance(self.space, _LIST_SPACES):
                self.data[actual_idx] = new_data
            else:

                def __set(s, n):
                    if self.space:
                        assert self.space.contains(n), n
                    s[actual_idx] = n

                tree.map_structure(__set, self.data, new_data)
        except IndexError:
            raise IndexError(
                f"Cannot `set()` value at index {idx} (option "
                f"neg_index_as_lookback={neg_index_as_lookback})! Out of range "
                f"of buffer data."
            )

    def _interpret_slice(
        self,
        slice_,
        neg_index_as_lookback,
        len_self_plus_lookback=None,
    ):
        """Identical to InfiniteLookbackBuffer._interpret_slice."""
        if len_self_plus_lookback is None:
            len_self_plus_lookback = len(self) + self.lookback

        start = slice_.start
        stop = slice_.stop

        if start is None:
            start = self.lookback
        elif start < 0:
            if neg_index_as_lookback:
                start = self.lookback + start
            else:
                start = len_self_plus_lookback + start
        else:
            start = self.lookback + start

        if stop is None:
            stop = len_self_plus_lookback
        elif stop < 0:
            if neg_index_as_lookback:
                stop = self.lookback + stop
            else:
                stop = len_self_plus_lookback + stop
        else:
            stop = self.lookback + stop

        fill_left_count = fill_right_count = 0
        if start < 0 and stop < 0:
            fill_left_count = abs(start - stop)
            fill_right_count = 0
            start = stop = 0
        elif start >= len_self_plus_lookback and stop >= len_self_plus_lookback:
            fill_right_count = abs(start - stop)
            fill_left_count = 0
            start = stop = len_self_plus_lookback
        elif start < 0:
            fill_left_count = -start
            start = 0
        elif stop >= len_self_plus_lookback:
            fill_right_count = stop - len_self_plus_lookback
            stop = len_self_plus_lookback
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
