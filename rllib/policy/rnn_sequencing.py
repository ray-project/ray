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

from ray.util import log_once
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.framework import try_import_tf, try_import_torch

tf = try_import_tf()
torch, _ = try_import_torch()

logger = logging.getLogger(__name__)


@DeveloperAPI
def pad_batch_to_sequences_of_same_size(batch,
                                        max_seq_len,
                                        shuffle=False,
                                        batch_divisibility_req=1,
                                        feature_keys=None):
    """Applies padding to `batch` so it's choppable into same-size sequences.

    Shuffles `batch` (if desired), makes sure divisibility requirement is met,
    then pads the batch ([B, ...]) into same-size chunks ([B, ...]) w/o
    adding a time dimension (yet).
    Padding depends on episodes found in batch and `max_seq_len`.

    Args:
        batch (SampleBatch): The SampleBatch object. All values in here have
            the shape [B, ...].
        max_seq_len (int): The max. sequence length to use for chopping.
        shuffle (bool): Whether to shuffle batch sequences. Shuffle may
            be done in-place. This only makes sense if you're further
            applying minibatch SGD after getting the outputs.
        batch_divisibility_req (int): The int by which the batch dimension
            must be dividable.
        feature_keys (Optional[List[str]]): An optional list of keys to apply
            sequence-chopping to. If None, use all keys in batch that are not
            "state_in/out_"-type keys.
    """
    if batch_divisibility_req > 1:
        meets_divisibility_reqs = (
            len(batch[SampleBatch.CUR_OBS]) % batch_divisibility_req == 0
            # not multiagent
            and max(batch[SampleBatch.AGENT_INDEX]) == 0)
    else:
        meets_divisibility_reqs = True

    # RNN-case.
    if "state_in_0" in batch:
        dynamic_max = True
    # Multi-agent case.
    elif not meets_divisibility_reqs:
        max_seq_len = batch_divisibility_req
        dynamic_max = False
    # Simple case: not RNN nor do we need to pad.
    else:
        if shuffle:
            batch.shuffle()
        return

    # RNN or multi-agent case.
    state_keys = []
    feature_keys_ = feature_keys or []
    for k in batch.keys():
        if "state_in_" in k:
            state_keys.append(k)
        elif not feature_keys and "state_out_" not in k and k != "infos":
            feature_keys_.append(k)

    feature_sequences, initial_states, seq_lens = \
        chop_into_sequences(
            batch[SampleBatch.EPS_ID],
            batch[SampleBatch.UNROLL_ID],
            batch[SampleBatch.AGENT_INDEX],
            [batch[k] for k in feature_keys_],
            [batch[k] for k in state_keys],
            max_seq_len,
            dynamic_max=dynamic_max,
            shuffle=shuffle)
    for i, k in enumerate(feature_keys_):
        batch[k] = feature_sequences[i]
    for i, k in enumerate(state_keys):
        batch[k] = initial_states[i]
    batch["seq_lens"] = seq_lens

    if log_once("rnn_ma_feed_dict"):
        logger.info("Padded input for RNN:\n\n{}\n".format(
            summarize({
                "features": feature_sequences,
                "initial_states": initial_states,
                "seq_lens": seq_lens,
                "max_seq_len": max_seq_len,
            })))


@DeveloperAPI
def add_time_dimension(padded_inputs, seq_lens, framework="tf"):
    """Adds a time dimension to padded inputs.

    Arguments:
        padded_inputs (Tensor): a padded batch of sequences. That is,
            for seq_lens=[1, 2, 2], then inputs=[A, *, B, B, C, C], where
            A, B, C are sequence elements and * denotes padding.
        seq_lens (Tensor): the sequence lengths within the input batch,
            suitable for passing to tf.nn.dynamic_rnn().

    Returns:
        Reshaped tensor of shape [NUM_SEQUENCES, MAX_SEQ_LEN, ...].
    """

    # Sequence lengths have to be specified for LSTM batch inputs. The
    # input batch must be padded to the max seq length given here. That is,
    # batch_size == len(seq_lens) * max(seq_lens)
    if framework == "tf":
        padded_batch_size = tf.shape(padded_inputs)[0]
        max_seq_len = padded_batch_size // tf.shape(seq_lens)[0]

        # Dynamically reshape the padded batch to introduce a time dimension.
        new_batch_size = padded_batch_size // max_seq_len
        new_shape = ([new_batch_size, max_seq_len] +
                     padded_inputs.get_shape().as_list()[1:])
        return tf.reshape(padded_inputs, new_shape)
    else:
        assert framework == "torch", "`framework` must be either tf or torch!"
        padded_batch_size = padded_inputs.shape[0]
        max_seq_len = padded_batch_size // seq_lens.shape[0]

        # Dynamically reshape the padded batch to introduce a time dimension.
        new_batch_size = padded_batch_size // max_seq_len
        new_shape = (new_batch_size, max_seq_len) + padded_inputs.shape[1:]
        return torch.reshape(padded_inputs, new_shape)


@DeveloperAPI
def chop_into_sequences(episode_ids,
                        unroll_ids,
                        agent_indices,
                        feature_columns,
                        state_columns,
                        max_seq_len,
                        dynamic_max=True,
                        shuffle=False,
                        _extra_padding=0):
    """Truncate and pad experiences into fixed-length sequences.

    Args:
        episode_ids (list): List of episode ids for each step.
        unroll_ids (list): List of identifiers for the sample batch. This is
            used to make sure sequences are cut between sample batches.
        agent_indices (list): List of agent ids for each step. Note that this
            has to be combined with episode_ids for uniqueness.
        feature_columns (list): List of arrays containing features.
        state_columns (list): List of arrays containing LSTM state values.
        max_seq_len (int): Max length of sequences before truncation.
        dynamic_max (bool): Whether to dynamically shrink the max seq len.
            For example, if max len is 20 and the actual max seq len in the
            data is 7, it will be shrunk to 7.
        shuffle (bool): Whether to shuffle the sequence outputs.
        _extra_padding (int): Add extra padding to the end of sequences.

    Returns:
        f_pad (list): Padded feature columns. These will be of shape
            [NUM_SEQUENCES * MAX_SEQ_LEN, ...].
        s_init (list): Initial states for each sequence, of shape
            [NUM_SEQUENCES, ...].
        seq_lens (list): List of sequence lengths, of shape [NUM_SEQUENCES].

    Examples:
        >>> f_pad, s_init, seq_lens = chop_into_sequences(
                episode_ids=[1, 1, 5, 5, 5, 5],
                unroll_ids=[4, 4, 4, 4, 4, 4],
                agent_indices=[0, 0, 0, 0, 0, 0],
                feature_columns=[[4, 4, 8, 8, 8, 8],
                                 [1, 1, 0, 1, 1, 0]],
                state_columns=[[4, 5, 4, 5, 5, 5]],
                max_seq_len=3)
        >>> print(f_pad)
        [[4, 4, 0, 8, 8, 8, 8, 0, 0],
         [1, 1, 0, 0, 1, 1, 0, 0, 0]]
        >>> print(s_init)
        [[4, 4, 5]]
        >>> print(seq_lens)
        [2, 3, 1]
    """

    prev_id = None
    seq_lens = []
    seq_len = 0
    unique_ids = np.add(
        np.add(episode_ids, agent_indices),
        np.array(unroll_ids) << 32)
    for uid in unique_ids:
        if (prev_id is not None and uid != prev_id) or \
                seq_len >= max_seq_len:
            seq_lens.append(seq_len)
            seq_len = 0
        seq_len += 1
        prev_id = uid
    if seq_len:
        seq_lens.append(seq_len)
    assert sum(seq_lens) == len(unique_ids)
    seq_lens = np.array(seq_lens)

    # Dynamically shrink max len as needed to optimize memory usage
    if dynamic_max:
        max_seq_len = max(seq_lens) + _extra_padding

    feature_sequences = []
    for f in feature_columns:
        f = np.array(f)
        f_pad = np.zeros((len(seq_lens) * max_seq_len, ) + np.shape(f)[1:])
        seq_base = 0
        i = 0
        for l in seq_lens:
            for seq_offset in range(l):
                f_pad[seq_base + seq_offset] = f[i]
                i += 1
            seq_base += max_seq_len
        assert i == len(unique_ids), f
        feature_sequences.append(f_pad)

    initial_states = []
    for s in state_columns:
        s = np.array(s)
        s_init = []
        i = 0
        for l in seq_lens:
            s_init.append(s[i])
            i += l
        initial_states.append(np.array(s_init))

    if shuffle:
        permutation = np.random.permutation(len(seq_lens))
        for i, f in enumerate(feature_sequences):
            orig_shape = f.shape
            f = np.reshape(f, (len(seq_lens), -1) + f.shape[1:])
            f = f[permutation]
            f = np.reshape(f, orig_shape)
            feature_sequences[i] = f
        for i, s in enumerate(initial_states):
            s = s[permutation]
            initial_states[i] = s
        seq_lens = seq_lens[permutation]

    return feature_sequences, initial_states, seq_lens
