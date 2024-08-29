from collections import deque
from typing import List, Tuple

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
def split_and_zero_pad(item_list, T: int):
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
        if isinstance(item, BatchedNdArray):
            t = T - current_t
            current_time_row.append(item[:t])
            if len(item) <= t:
                current_t += len(item)
            else:
                current_t += t
                item_list.appendleft(item[t:])
        else:
            current_time_row.append(item)
            current_t += 1

        if current_t == T:
            ret.append(
                batch(
                    current_time_row,
                    individual_items_already_have_batch_dim="auto",
                )
            )
            current_time_row = []
            current_t = 0

    if current_t > 0 and current_t < T:
        current_time_row.extend([zero_element] * (T - current_t))
        ret.append(
            batch(current_time_row, individual_items_already_have_batch_dim="auto")
        )

    return ret


@DeveloperAPI
def unpad_data_if_necessary(episode_lens, data):
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
