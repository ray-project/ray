"""RNN utils for RLlib.

The main trick here is that we add the time dimension at the last moment.
The non-LSTM layers of the model see their inputs as one flat batch. Before
the LSTM cell, we reshape the input to add the expected time dimension. During
postprocessing, we dynamically pad the experience batches so that this
reshaping is possible.

Note that this padding strategy only works out if we assume zero inputs don't
meaningfully affect the loss function. This happens to be true for all the
current algorithms: https://github.com/ray-project/ray/issues/2992
"""

import logging
import numpy as np
import tree  # pip install dm_tree
from typing import List, Optional

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.typing import TensorType, ViewRequirementsDict
from ray.util import log_once
from ray.rllib.utils.typing import SampleBatchType

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

logger = logging.getLogger(__name__)


@DeveloperAPI
def pad_batch_to_sequences_of_same_size(
    batch: SampleBatch,
    max_seq_len: int,
    shuffle: bool = False,
    batch_divisibility_req: int = 1,
    feature_keys: Optional[List[str]] = None,
    view_requirements: Optional[ViewRequirementsDict] = None,
):
    """Applies padding to `batch` so it's choppable into same-size sequences.

    Shuffles `batch` (if desired), makes sure divisibility requirement is met,
    then pads the batch ([B, ...]) into same-size chunks ([B, ...]) w/o
    adding a time dimension (yet).
    Padding depends on episodes found in batch and `max_seq_len`.

    Args:
        batch: The SampleBatch object. All values in here have
            the shape [B, ...].
        max_seq_len: The max. sequence length to use for chopping.
        shuffle: Whether to shuffle batch sequences. Shuffle may
            be done in-place. This only makes sense if you're further
            applying minibatch SGD after getting the outputs.
        batch_divisibility_req: The int by which the batch dimension
            must be dividable.
        feature_keys: An optional list of keys to apply sequence-chopping
            to. If None, use all keys in batch that are not
            "state_in/out_"-type keys.
        view_requirements: An optional Policy ViewRequirements dict to
            be able to infer whether e.g. dynamic max'ing should be
            applied over the seq_lens.
    """
    # If already zero-padded, skip.
    if batch.zero_padded:
        return

    batch.zero_padded = True

    if batch_divisibility_req > 1:
        meets_divisibility_reqs = (
            len(batch[SampleBatch.CUR_OBS]) % batch_divisibility_req == 0
            # not multiagent
            and max(batch[SampleBatch.AGENT_INDEX]) == 0
        )
    else:
        meets_divisibility_reqs = True

    states_already_reduced_to_init = False

    # RNN/attention net case. Figure out whether we should apply dynamic
    # max'ing over the list of sequence lengths.
    if "state_in_0" in batch or "state_out_0" in batch:
        # Check, whether the state inputs have already been reduced to their
        # init values at the beginning of each max_seq_len chunk.
        if batch.get(SampleBatch.SEQ_LENS) is not None and len(
            batch["state_in_0"]
        ) == len(batch[SampleBatch.SEQ_LENS]):
            states_already_reduced_to_init = True

        # RNN (or single timestep state-in): Set the max dynamically.
        if view_requirements["state_in_0"].shift_from is None:
            dynamic_max = True
        # Attention Nets (state inputs are over some range): No dynamic maxing
        # possible.
        else:
            dynamic_max = False
    # Multi-agent case.
    elif not meets_divisibility_reqs:
        max_seq_len = batch_divisibility_req
        dynamic_max = False
        batch.max_seq_len = max_seq_len
    # Simple case: No RNN/attention net, nor do we need to pad.
    else:
        if shuffle:
            batch.shuffle()
        return

    # RNN, attention net, or multi-agent case.
    state_keys = []
    feature_keys_ = feature_keys or []
    for k, v in batch.items():
        if k.startswith("state_in_"):
            state_keys.append(k)
        elif (
            not feature_keys
            and not k.startswith("state_out_")
            and k not in [SampleBatch.INFOS, SampleBatch.SEQ_LENS]
        ):
            feature_keys_.append(k)

    feature_sequences, initial_states, seq_lens = chop_into_sequences(
        feature_columns=[batch[k] for k in feature_keys_],
        state_columns=[batch[k] for k in state_keys],
        episode_ids=batch.get(SampleBatch.EPS_ID),
        unroll_ids=batch.get(SampleBatch.UNROLL_ID),
        agent_indices=batch.get(SampleBatch.AGENT_INDEX),
        seq_lens=batch.get(SampleBatch.SEQ_LENS),
        max_seq_len=max_seq_len,
        dynamic_max=dynamic_max,
        states_already_reduced_to_init=states_already_reduced_to_init,
        shuffle=shuffle,
        handle_nested_data=True,
    )

    for i, k in enumerate(feature_keys_):
        batch[k] = tree.unflatten_as(batch[k], feature_sequences[i])
    for i, k in enumerate(state_keys):
        batch[k] = initial_states[i]
    batch[SampleBatch.SEQ_LENS] = np.array(seq_lens)
    if dynamic_max:
        batch.max_seq_len = max(seq_lens)

    if log_once("rnn_ma_feed_dict"):
        logger.info(
            "Padded input for RNN/Attn.Nets/MA:\n\n{}\n".format(
                summarize(
                    {
                        "features": feature_sequences,
                        "initial_states": initial_states,
                        "seq_lens": seq_lens,
                        "max_seq_len": max_seq_len,
                    }
                )
            )
        )


@DeveloperAPI
def add_time_dimension(
    padded_inputs: TensorType,
    *,
    seq_lens: TensorType,
    framework: str = "tf",
    time_major: bool = False,
):
    """Adds a time dimension to padded inputs.

    Args:
        padded_inputs: a padded batch of sequences. That is,
            for seq_lens=[1, 2, 2], then inputs=[A, *, B, B, C, C], where
            A, B, C are sequence elements and * denotes padding.
        seq_lens: A 1D tensor of sequence lengths, denoting the non-padded length
            in timesteps of each rollout in the batch.
        framework: The framework string ("tf2", "tf", "tfe", "torch").
        time_major: Whether data should be returned in time-major (TxB)
            format or not (BxT).

    Returns:
        TensorType: Reshaped tensor of shape [B, T, ...] or [T, B, ...].
    """

    # Sequence lengths have to be specified for LSTM batch inputs. The
    # input batch must be padded to the max seq length given here. That is,
    # batch_size == len(seq_lens) * max(seq_lens)
    if framework in ["tf2", "tf", "tfe"]:
        assert time_major is False, "time-major not supported yet for tf!"
        padded_batch_size = tf.shape(padded_inputs)[0]
        # Dynamically reshape the padded batch to introduce a time dimension.
        new_batch_size = tf.shape(seq_lens)[0]
        time_size = padded_batch_size // new_batch_size
        new_shape = tf.concat(
            [
                tf.expand_dims(new_batch_size, axis=0),
                tf.expand_dims(time_size, axis=0),
                tf.shape(padded_inputs)[1:],
            ],
            axis=0,
        )
        return tf.reshape(padded_inputs, new_shape)
    else:
        assert framework == "torch", "`framework` must be either tf or torch!"
        padded_batch_size = padded_inputs.shape[0]

        # Dynamically reshape the padded batch to introduce a time dimension.
        new_batch_size = seq_lens.shape[0]
        time_size = padded_batch_size // new_batch_size
        batch_major_shape = (new_batch_size, time_size) + padded_inputs.shape[1:]
        padded_outputs = padded_inputs.view(batch_major_shape)

        if time_major:
            # Swap the batch and time dimensions
            padded_outputs = padded_outputs.transpose(0, 1)
        return padded_outputs


@DeveloperAPI
def chop_into_sequences(
    *,
    feature_columns,
    state_columns,
    max_seq_len,
    episode_ids=None,
    unroll_ids=None,
    agent_indices=None,
    dynamic_max=True,
    shuffle=False,
    seq_lens=None,
    states_already_reduced_to_init=False,
    handle_nested_data=False,
    _extra_padding=0,
):
    """Truncate and pad experiences into fixed-length sequences.

    Args:
        feature_columns: List of arrays containing features.
        state_columns: List of arrays containing LSTM state values.
        max_seq_len: Max length of sequences. Sequences longer than max_seq_len
            will be split into subsequences that span the batch dimension
            and sum to max_seq_len.
        episode_ids (List[EpisodeID]): List of episode ids for each step.
        unroll_ids (List[UnrollID]): List of identifiers for the sample batch.
            This is used to make sure sequences are cut between sample batches.
        agent_indices (List[AgentID]): List of agent ids for each step. Note
            that this has to be combined with episode_ids for uniqueness.
        dynamic_max: Whether to dynamically shrink the max seq len.
            For example, if max len is 20 and the actual max seq len in the
            data is 7, it will be shrunk to 7.
        shuffle: Whether to shuffle the sequence outputs.
        handle_nested_data: If True, assume that the data in
            `feature_columns` could be nested structures (of data).
            If False, assumes that all items in `feature_columns` are
            only np.ndarrays (no nested structured of np.ndarrays).
        _extra_padding: Add extra padding to the end of sequences.

    Returns:
        f_pad: Padded feature columns. These will be of shape
            [NUM_SEQUENCES * MAX_SEQ_LEN, ...].
        s_init: Initial states for each sequence, of shape
            [NUM_SEQUENCES, ...].
        seq_lens: List of sequence lengths, of shape [NUM_SEQUENCES].

    Examples:
        >>> from ray.rllib.policy.rnn_sequencing import chop_into_sequences
        >>> f_pad, s_init, seq_lens = chop_into_sequences( # doctest: +SKIP
        ...     episode_ids=[1, 1, 5, 5, 5, 5],
        ...     unroll_ids=[4, 4, 4, 4, 4, 4],
        ...     agent_indices=[0, 0, 0, 0, 0, 0],
        ...     feature_columns=[[4, 4, 8, 8, 8, 8],
        ...                      [1, 1, 0, 1, 1, 0]],
        ...     state_columns=[[4, 5, 4, 5, 5, 5]],
        ...     max_seq_len=3)
        >>> print(f_pad) # doctest: +SKIP
        [[4, 4, 0, 8, 8, 8, 8, 0, 0],
         [1, 1, 0, 0, 1, 1, 0, 0, 0]]
        >>> print(s_init) # doctest: +SKIP
        [[4, 4, 5]]
        >>> print(seq_lens)
        [2, 3, 1]
    """

    if seq_lens is None or len(seq_lens) == 0:
        prev_id = None
        seq_lens = []
        seq_len = 0
        unique_ids = np.add(
            np.add(episode_ids, agent_indices),
            np.array(unroll_ids, dtype=np.int64) << 32,
        )
        for uid in unique_ids:
            if (prev_id is not None and uid != prev_id) or seq_len >= max_seq_len:
                seq_lens.append(seq_len)
                seq_len = 0
            seq_len += 1
            prev_id = uid
        if seq_len:
            seq_lens.append(seq_len)
        seq_lens = np.array(seq_lens, dtype=np.int32)

    # Dynamically shrink max len as needed to optimize memory usage
    if dynamic_max:
        max_seq_len = max(seq_lens) + _extra_padding

    feature_sequences = []
    for col in feature_columns:
        if isinstance(col, list):
            col = np.array(col)
        feature_sequences.append([])

        for f in tree.flatten(col):
            # Save unnecessary copy.
            if not isinstance(f, np.ndarray):
                f = np.array(f)

            length = len(seq_lens) * max_seq_len
            if f.dtype == object or f.dtype.type is np.str_:
                f_pad = [None] * length
            else:
                # Make sure type doesn't change.
                f_pad = np.zeros((length,) + np.shape(f)[1:], dtype=f.dtype)
            seq_base = 0
            i = 0
            for len_ in seq_lens:
                for seq_offset in range(len_):
                    f_pad[seq_base + seq_offset] = f[i]
                    i += 1
                seq_base += max_seq_len
            assert i == len(f), f
            feature_sequences[-1].append(f_pad)

    if states_already_reduced_to_init:
        initial_states = state_columns
    else:
        initial_states = []
        for s in state_columns:
            # Skip unnecessary copy.
            if not isinstance(s, np.ndarray):
                s = np.array(s)
            s_init = []
            i = 0
            for len_ in seq_lens:
                s_init.append(s[i])
                i += len_
            initial_states.append(np.array(s_init))

    if shuffle:
        permutation = np.random.permutation(len(seq_lens))
        for i, f in enumerate(tree.flatten(feature_sequences)):
            orig_shape = f.shape
            f = np.reshape(f, (len(seq_lens), -1) + f.shape[1:])
            f = f[permutation]
            f = np.reshape(f, orig_shape)
            feature_sequences[i] = f
        for i, s in enumerate(initial_states):
            s = s[permutation]
            initial_states[i] = s
        seq_lens = seq_lens[permutation]

    # Classic behavior: Don't assume data in feature_columns are nested
    # structs. Don't return them as flattened lists, but as is (index 0).
    if not handle_nested_data:
        feature_sequences = [f[0] for f in feature_sequences]

    return feature_sequences, initial_states, seq_lens


@DeveloperAPI
def timeslice_along_seq_lens_with_overlap(
    sample_batch: SampleBatchType,
    seq_lens: Optional[List[int]] = None,
    zero_pad_max_seq_len: int = 0,
    pre_overlap: int = 0,
    zero_init_states: bool = True,
) -> List["SampleBatch"]:
    """Slices batch along `seq_lens` (each seq-len item produces one batch).

    Args:
        sample_batch: The SampleBatch to timeslice.
        seq_lens (Optional[List[int]]): An optional list of seq_lens to slice
            at. If None, use `sample_batch[SampleBatch.SEQ_LENS]`.
        zero_pad_max_seq_len: If >0, already zero-pad the resulting
            slices up to this length. NOTE: This max-len will include the
            additional timesteps gained via setting pre_overlap (see Example).
        pre_overlap: If >0, will overlap each two consecutive slices by
            this many timesteps (toward the left side). This will cause
            zero-padding at the very beginning of the batch.
        zero_init_states: Whether initial states should always be
            zero'd. If False, will use the state_outs of the batch to
            populate state_in values.

    Returns:
        List[SampleBatch]: The list of (new) SampleBatches.

    Examples:
        assert seq_lens == [5, 5, 2]
        assert sample_batch.count == 12
        # self = 0 1 2 3 4 | 5 6 7 8 9 | 10 11 <- timesteps
        slices = timeslice_along_seq_lens_with_overlap(
            sample_batch=sample_batch.
            zero_pad_max_seq_len=10,
            pre_overlap=3)
        # Z = zero padding (at beginning or end).
        #             |pre (3)|     seq     | max-seq-len (up to 10)
        # slices[0] = | Z Z Z |  0  1 2 3 4 | Z Z
        # slices[1] = | 2 3 4 |  5  6 7 8 9 | Z Z
        # slices[2] = | 7 8 9 | 10 11 Z Z Z | Z Z
        # Note that `zero_pad_max_seq_len=10` includes the 3 pre-overlaps
        #  count (makes sure each slice has exactly length 10).
    """
    if seq_lens is None:
        seq_lens = sample_batch.get(SampleBatch.SEQ_LENS)
    else:
        if sample_batch.get(SampleBatch.SEQ_LENS) is not None and log_once(
            "overriding_sequencing_information"
        ):
            logger.warning(
                "Found sequencing information in a batch that will be "
                "ignored when slicing. Ignore this warning if you know "
                "what you are doing."
            )

    if seq_lens is None:
        max_seq_len = zero_pad_max_seq_len - pre_overlap
        if log_once("no_sequence_lengths_available_for_time_slicing"):
            logger.warning(
                "Trying to slice a batch along sequences without "
                "sequence lengths being provided in the batch. Batch will "
                "be sliced into slices of size "
                "{} = {} - {} = zero_pad_max_seq_len - pre_overlap.".format(
                    max_seq_len, zero_pad_max_seq_len, pre_overlap
                )
            )
        num_seq_lens, last_seq_len = divmod(len(sample_batch), max_seq_len)
        seq_lens = [zero_pad_max_seq_len] * num_seq_lens + (
            [last_seq_len] if last_seq_len else []
        )

    assert (
        seq_lens is not None and len(seq_lens) > 0
    ), "Cannot timeslice along `seq_lens` when `seq_lens` is empty or None!"
    # Generate n slices based on seq_lens.
    start = 0
    slices = []
    for seq_len in seq_lens:
        pre_begin = start - pre_overlap
        slice_begin = start
        end = start + seq_len
        slices.append((pre_begin, slice_begin, end))
        start += seq_len

    timeslices = []
    for begin, slice_begin, end in slices:
        zero_length = None
        data_begin = 0
        zero_init_states_ = zero_init_states
        if begin < 0:
            zero_length = pre_overlap
            data_begin = slice_begin
            zero_init_states_ = True
        else:
            eps_ids = sample_batch[SampleBatch.EPS_ID][begin if begin >= 0 else 0 : end]
            is_last_episode_ids = eps_ids == eps_ids[-1]
            if not is_last_episode_ids[0]:
                zero_length = int(sum(1.0 - is_last_episode_ids))
                data_begin = begin + zero_length
                zero_init_states_ = True

        if zero_length is not None:
            data = {
                k: np.concatenate(
                    [
                        np.zeros(shape=(zero_length,) + v.shape[1:], dtype=v.dtype),
                        v[data_begin:end],
                    ]
                )
                for k, v in sample_batch.items()
                if k != SampleBatch.SEQ_LENS
            }
        else:
            data = {
                k: v[begin:end]
                for k, v in sample_batch.items()
                if k != SampleBatch.SEQ_LENS
            }

        if zero_init_states_:
            i = 0
            key = "state_in_{}".format(i)
            while key in data:
                data[key] = np.zeros_like(sample_batch[key][0:1])
                # Del state_out_n from data if exists.
                data.pop("state_out_{}".format(i), None)
                i += 1
                key = "state_in_{}".format(i)
        # TODO: This will not work with attention nets as their state_outs are
        #  not compatible with state_ins.
        else:
            i = 0
            key = "state_in_{}".format(i)
            while key in data:
                data[key] = sample_batch["state_out_{}".format(i)][begin - 1 : begin]
                del data["state_out_{}".format(i)]
                i += 1
                key = "state_in_{}".format(i)

        timeslices.append(SampleBatch(data, seq_lens=[end - begin]))

    # Zero-pad each slice if necessary.
    if zero_pad_max_seq_len > 0:
        for ts in timeslices:
            ts.right_zero_pad(max_seq_len=zero_pad_max_seq_len, exclude_states=True)

    return timeslices
