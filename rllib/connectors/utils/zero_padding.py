from typing import List, Tuple

import numpy as np


def create_mask_and_seq_lens(
    episode_lens: List[int],
    T: int,
) -> Tuple[np._typing.NDArray, np._typing.NDArray]:
    """Creates loss mask and a seq_lens array, given a list of episode lengths and T.

    Args:
        episode_lens: A list of episode lengths to infer the loss mask and seq_lens
            array from.
        T: The maximum number of timesteps in each "row", also known as the maximum
            sequence length (max_seq_len). Episodes are split into chunks that are at
            most `T` long and remaining timesteps will be zero-padded (and masked out).

    Returns:
         Tuple consisting of a) the loss mask to use (masking out areas that are past
         the end of an episode (or rollout), but had to be zero-added due to the added
         extra time rank (of length T) and b) the array of sequence lengths resulting
         from splitting the given episodes into chunks of at most `T` timesteps.
    """
    mask = []
    seq_lens = []
    for episode_len in episode_lens:
        len_ = min(episode_len, T)
        seq_lens.append(len_)
        row = [1] * len_ + [0] * (T - len_)
        mask.append(row)

        # Handle sequence lengths greater than T.
        overflow = episode_len - T
        while overflow > 0:
            len_ = min(overflow, T)
            seq_lens.append(len_)
            extra_row = [1] * len_ + [0] * (T - len_)
            mask.append(extra_row)
            overflow -= T

    return np.array(mask, dtype=np.bool_), np.array(seq_lens, dtype=np.int32)


def split_and_pad(data_chunks: List[np._typing.NDArray], T: int) -> np._typing.NDArray:
    """Splits and zero-pads data from episodes into a single ndarray with a fixed T-axis.

    Processes each data chunk in `data_chunks`, coming from one episode by splitting
    the chunk into smaller sub-chunks, each of a maximum size `T`. If a sub-chunk is
    smaller than `T`, it is right-padded with zeros to match the desired size T.
    All sub-chunks are then re-combined (concatenated) into a single ndarray, which is
    reshaped to include the new time dimension `T` as axis 1 (axis 0 is the batch
    axis). The resulting output array has dimensions (B=number of sub-chunks, T, ...),
    where '...' represents the original dimensions of the input data (excluding the
    batch dimension).

    Args:
        data_chunks: A list where each element is a NumPy array representing
            an episode. Each array's shape should be (episode_length, ...)
            where '...' represents any number of additional dimensions.
        T: The desired time dimension size for each chunk.

    Returns:
        A np.ndarray containing the reshaped and padded chunks. The shape of the
        array will be (B, T, ...) where B is automatically determined by the number
        of chunks in `data_chunks` and `T`.
        '...' represents the original dimensions of the input data, excluding the
        batch dimension.
    """
    all_chunks = []

    for data_chunk in data_chunks:
        num_sub_chunks = int(np.ceil(data_chunk.shape[0] / T))

        for i in range(num_sub_chunks):
            start_index = i * T
            end_index = start_index + T

            # Extract the chunk.
            sub_chunk = data_chunk[start_index:end_index]

            # Pad the chunk if it's shorter than T
            if sub_chunk.shape[0] < T:
                padding_shape = [(0, T - sub_chunk.shape[0])] + [
                    (0, 0) for _ in range(sub_chunk.ndim - 1)
                ]
                sub_chunk = np.pad(sub_chunk, pad_width=padding_shape, mode="constant")

            all_chunks.append(sub_chunk)

    # Combine all chunks into a single array.
    result = np.concatenate(all_chunks, axis=0)

    # Reshape the array to include the time dimension T.
    # The new shape should be (-1, T) + original dimensions (excluding the
    # batch dimension).
    result = result.reshape((-1, T) + result.shape[1:])

    return result


def split_and_pad_single_record(
    data: np._typing.NDArray, episode_lengths: List[int], T: int
):
    """See `split_and_pad`, but initial data has already been concatenated over episodes.

    Given an np.ndarray of data that is the result of a concatenation of data chunks
    coming from different episodes, the lengths of these episodes, as well as the
    maximum time dimension, split and possibly right-zero-pad this input data, such that
    the resulting shape of the returned np.ndarray is (B', T, ...), where B' is the
    number of generated sub-chunks and ... is the original shape of the data (excluding
    the batch dim). T is the size of the newly inserted time axis (on which zero-padding
    is applied if necessary).

    Args:
        data: The single np.ndarray input data to be split, zero-added, and reshaped.
        episode_lengths: The list of episode lengths, from which `data` was originally
            concat'd.
        T: The maximum number of timesteps on the T-axis in the resulting np.ndarray.

    Returns:
        A single np.ndarray, which contains the same data as `data`, but split into sub-
        chunks of max. size T (zero-padded if necessary at the end of individual
        episodes), then reshaped to (B', T, ...).
    """
    # Chop up `data` into chunks of max len=T, based on the lengths of the episodes
    # where this data came from.
    episodes_data = []
    idx = 0
    for episode_len in episode_lengths:
        episodes_data.append(data[idx : idx + episode_len])
        idx += episode_len
    # Send everything through `split_and_pad` to perform the actual splitting into
    # sub-chunks of max len=T and zero-padding.
    return split_and_pad(episodes_data, T)
