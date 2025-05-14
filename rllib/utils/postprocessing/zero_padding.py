from collections import deque
from typing import List, Tuple, Union

import numpy as np
import tree  # pip install dm_tree

from ray.rllib.utils.spaces.space_utils import batch, BatchedNdArray
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
def create_mask_and_seq_lens(episode_len: int, T: int) -> Tuple[List, List]:
    """Creates loss mask and a seq_lens array, given an episode length and T.

    Args:
        episode_lens: A list of episode lengths to infer the loss mask and seq_lens
            array from.
        T: The maximum number of timesteps in each "row", also known as the maximum
            sequence length (max_seq_len). Episodes are split into chunks that are at
            most `T` long and remaining timesteps will be zero-padded (and masked out).

    Returns:
         Tuple consisting of a) list of the loss masks to use (masking out areas that
         are past the end of an episode (or rollout), but had to be zero-added due to
         the added extra time rank (of length T) and b) the list of sequence lengths
         resulting from splitting the given episodes into chunks of at most `T`
         timesteps.
    """
    mask = []
    seq_lens = []

    len_ = min(episode_len, T)
    seq_lens.append(len_)
    row = np.array([1] * len_ + [0] * (T - len_), np.bool_)
    mask.append(row)

    # Handle sequence lengths greater than T.
    overflow = episode_len - T
    while overflow > 0:
        len_ = min(overflow, T)
        seq_lens.append(len_)
        extra_row = np.array([1] * len_ + [0] * (T - len_), np.bool_)
        mask.append(extra_row)
        overflow -= T

    return mask, seq_lens


@DeveloperAPI
def split_and_zero_pad(
    item_list: List[Union[BatchedNdArray, np._typing.NDArray, float]],
    max_seq_len: int,
) -> List[np._typing.NDArray]:
    """Splits the contents of `item_list` into a new list of ndarrays and returns it.

    In the returned list, each item is one ndarray of len (axis=0) `max_seq_len`.
    The last item in the returned list may be (right) zero-padded, if necessary, to
    reach `max_seq_len`.

    If `item_list` contains one or more `BatchedNdArray` (instead of individual
    items), these will be split accordingly along their axis=0 to yield the returned
    structure described above.

    .. testcode::

        from ray.rllib.utils.postprocessing.zero_padding import (
            BatchedNdArray,
            split_and_zero_pad,
        )
        from ray.rllib.utils.test_utils import check

        # Simple case: `item_list` contains individual floats.
        check(
            split_and_zero_pad([0, 1, 2, 3, 4, 5, 6, 7], 5),
            [[0, 1, 2, 3, 4], [5, 6, 7, 0, 0]],
        )

        # `item_list` contains BatchedNdArray (ndarrays that explicitly declare they
        # have a batch axis=0).
        check(
            split_and_zero_pad([
                BatchedNdArray([0, 1]),
                BatchedNdArray([2, 3, 4, 5]),
                BatchedNdArray([6, 7, 8]),
            ], 5),
            [[0, 1, 2, 3, 4], [5, 6, 7, 8, 0]],
        )

    Args:
        item_list: A list of individual items or BatchedNdArrays to be split into
            `max_seq_len` long pieces (the last of which may be zero-padded).
        max_seq_len: The maximum length of each item in the returned list.

    Returns:
        A list of np.ndarrays (all of length `max_seq_len`), which contains the same
        data as `item_list`, but split into sub-chunks of size `max_seq_len`.
        The last item in the returned list may be zero-padded, if necessary.
    """
    zero_element = tree.map_structure(
        lambda s: np.zeros_like([s[0]] if isinstance(s, BatchedNdArray) else s),
        item_list[0],
    )

    # The replacement list (to be returned) for `items_list`.
    # Items list contains n individual items.
    # -> ret will contain m batched rows, where m == n // T and the last row
    # may be zero padded (until T).
    ret = []

    # List of the T-axis item, collected to form the next row.
    current_time_row = []
    current_t = 0

    item_list = deque(item_list)
    while len(item_list) > 0:
        item = item_list.popleft()
        t = max_seq_len - current_t

        # In case `item` is a complex struct.
        item_flat = tree.flatten(item)
        item_list_append = []
        current_time_row_flat_items = []
        add_to_current_t = 0

        for itm in item_flat:
            # `itm` is already a batched np.array: Split if necessary.
            if isinstance(itm, BatchedNdArray):
                current_time_row_flat_items.append(itm[:t])
                if len(itm) <= t:
                    add_to_current_t = len(itm)
                else:
                    add_to_current_t = t
                    item_list_append.append(itm[t:])
            # `itm` is a single item (no batch axis): Append and continue with next
            # item.
            else:
                current_time_row_flat_items.append(itm)
                add_to_current_t = 1

        current_t += add_to_current_t
        current_time_row.append(tree.unflatten_as(item, current_time_row_flat_items))
        if item_list_append:
            item_list.appendleft(tree.unflatten_as(item, item_list_append))

        # `current_time_row` is "full" (max_seq_len): Append as ndarray (with batch
        # axis) to `ret`.
        if current_t == max_seq_len:
            ret.append(
                batch(
                    current_time_row,
                    individual_items_already_have_batch_dim="auto",
                )
            )
            current_time_row = []
            current_t = 0

    # `current_time_row` is unfinished: Pad, if necessary and append to `ret`.
    if current_t > 0 and current_t < max_seq_len:
        current_time_row.extend([zero_element] * (max_seq_len - current_t))
        ret.append(
            batch(current_time_row, individual_items_already_have_batch_dim="auto")
        )

    return ret


@DeveloperAPI
def split_and_zero_pad_n_episodes(
    nd_array: np._typing.NDArray,
    episode_lens: List[int],
    max_seq_len: int,
) -> List[np._typing.NDArray]:
    """Splits and zero-pads a single np.ndarray based on episode lens and a maxlen.

    Args:
        nd_array: The single np.ndarray to be split into n chunks, based on the given
            `episode_lens` and the `max_seq_len` argument. For example, if `nd_array`
            has a batch dimension (axis 0) of 21, `episode_lens` is [15, 3, 3], and
            `max_seq_len` is 6, then the returned list would have np.ndarrays in it of
            batch dimensions (axis 0): [6, 6, 6 (zero-padded), 6 (zero-padded),
            6 (zero-padded)].
            Note that this function doesn't work on nested data, such as dicts of
            ndarrays.
        episode_lens: A list of episode lengths along which to split and zero-pad the
            given `nd_array`.
        max_seq_len: The maximum sequence length to split at (and zero-pad).

    Returns: A list of n np.ndarrays, resulting from splitting and zero-padding the
        given `nd_array`.
    """
    ret = []

    cursor = 0
    for episode_len in episode_lens:
        items = BatchedNdArray(nd_array[cursor : cursor + episode_len])
        ret.extend(split_and_zero_pad([items], max_seq_len))
        cursor += episode_len

    return ret


@DeveloperAPI
def unpad_data_if_necessary(
    episode_lens: List[int],
    data: np._typing.NDArray,
) -> np._typing.NDArray:
    """Removes right-side zero-padding from data based on `episode_lens`.

    ..testcode::

        from ray.rllib.utils.postprocessing.zero_padding import unpad_data_if_necessary
        import numpy as np

        unpadded = unpad_data_if_necessary(
            episode_lens=[4, 2],
            data=np.array([
                [2, 4, 5, 3, 0, 0, 0, 0],
                [-1, 3, 0, 0, 0, 0, 0, 0],
            ]),
        )
        assert (unpadded == [2, 4, 5, 3, -1, 3]).all()

        unpadded = unpad_data_if_necessary(
            episode_lens=[1, 5],
            data=np.array([
                [2, 0, 0, 0, 0],
                [-1, -2, -3, -4, -5],
            ]),
        )
        assert (unpadded == [2, -1, -2, -3, -4, -5]).all()

    Args:
        episode_lens: A list of actual episode lengths.
        data: A 2D np.ndarray with right-side zero-padded rows.

    Returns:
        A 1D np.ndarray resulting from concatenation of the un-padded
        input data along the 0-axis.
    """
    # If data des NOT have time dimension, return right away.
    if len(data.shape) == 1:
        return data

    # Assert we only have B and T dimensions (meaning this function only operates
    # on single-float data, such as value function predictions, advantages, or rewards).
    assert len(data.shape) == 2

    new_data = []
    row_idx = 0

    T = data.shape[1]
    for len_ in episode_lens:
        # Calculate how many full rows this array occupies and how many elements are
        # in the last, potentially partial row.
        num_rows, col_idx = divmod(len_, T)

        # If the array spans multiple full rows, fully include these rows.
        for i in range(num_rows):
            new_data.append(data[row_idx])
            row_idx += 1

        # If there are elements in the last, potentially partial row, add this
        # partial row as well.
        if col_idx > 0:
            new_data.append(data[row_idx, :col_idx])

            # Move to the next row for the next array (skip the zero-padding zone).
            row_idx += 1

    return np.concatenate(new_data)
