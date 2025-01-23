from typing import List, Tuple, Union

import numpy as np

from ray.rllib.utils.spaces.space_utils import BatchedNdArray
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
@DeveloperAPI
def split_and_zero_pad(
    item_list: List[Union[BatchedNdArray, np.ndarray, float]],
    max_seq_len: int,
) -> List[np.ndarray]:
    """Splits/reshapes data into sub-chunks of size `max_seq_len`, zero-padding as needed.

    This is an optimized, single-pass version of the original `split_and_zero_pad`.

    Args:
        item_list: A list of either:
            - individual float/int items,
            - np.ndarray items (shape [D...]),
            - or BatchedNdArray items (shape [B, D...]) signifying a batch dimension.

            These will be combined ("flattened") into one array and then split
            into rows of length `max_seq_len`. The last row is zero-padded if needed.
        max_seq_len: The chunk size (T). Each chunk in the returned list will
            be a NumPy array of shape [T, ...].

    Returns:
        A list of NumPy arrays, each of length `max_seq_len` along axis 0.
        The very last array may be zero-padded at the end if the total number
        of items in `item_list` is not an exact multiple of `max_seq_len`.

    Example:
        >>> from ray.rllib.utils.spaces.space_utils import BatchedNdArray
        >>> data = [0, 1, 2, 3, 4, 5, 6]
        >>> out = split_and_zero_pad(data, 4)
        >>> # out = [array([0, 1, 2, 3]), array([4, 5, 6, 0])]

        >>> # Mixed single items and batched items:
        >>> data = [
        ...     BatchedNdArray([10, 11]),  # shape [2]
        ...     12,
        ...     BatchedNdArray([13, 14, 15]),  # shape [3]
        ... ]
        >>> out = split_and_zero_pad(data, 4)
        >>> # Flattened would be [10, 11, 12, 13, 14, 15].
        >>> # out = [array([10, 11, 12, 13]), array([14, 15,  0,  0])]
    """
    # If the input list is empty, return an empty result.
    if not item_list:
        return []

    # 1) Flatten everything into a single NumPy array.
    flattened_values = []
    for item in item_list:
        if isinstance(item, BatchedNdArray):
            # item is array-like with shape [B, ...]
            flattened_values.extend(item)
        else:
            # item is scalar or a normal np.ndarray (assume shape [D...])
            flattened_values.append(item)

    # Convert the flattened Python list to a single NumPy array.
    flat_data = np.array(flattened_values)
    # flat_data.shape = [N, ...]  (N = total count)

    N = flat_data.shape[0]
    num_chunks = (N + max_seq_len - 1) // max_seq_len  # ceil(N / max_seq_len)

    # 2) Create the output array with shape [num_chunks, max_seq_len, ...].
    out_shape = (num_chunks, max_seq_len) + flat_data.shape[1:]
    out = np.zeros(out_shape, dtype=flat_data.dtype)

    # 3) Fill the output array in a single pass, zero-padding where necessary.
    start_idx = 0
    for i in range(num_chunks):
        end_idx = min(start_idx + max_seq_len, N)
        length = end_idx - start_idx
        out[i, :length] = flat_data[start_idx:end_idx]
        start_idx += length

    # 4) Return a list of the sub-arrays, each shape [max_seq_len, ...].
    return [out[i] for i in range(num_chunks)]


@DeveloperAPI
def split_and_zero_pad_n_episodes(nd_array, episode_lens, max_seq_len):
    ret = []

    # item_list = deque(item_list)
    cursor = 0
    for episode_len in episode_lens:
        # episode_item_list = []
        items = BatchedNdArray(nd_array[cursor : cursor + episode_len])
        # episode_item_list.append(items)
        ret.extend(split_and_zero_pad([items], max_seq_len))
        cursor += episode_len

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
