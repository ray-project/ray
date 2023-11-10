from functools import partial
from typing import Any

import numpy as np
import tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.connector_context_v2 import ConnectorContextV2
from ray.rllib.core.models.base import STATE_IN, STATE_OUT
from ray.rllib.policy.sample_batch import SampleBatch


class DefaultLearnerConnector(ConnectorV2):
    """Connector added by default by RLlib to the end of the learner connector pipeline.

    If provided with `episodes` data, this connector piece makes sure that the final
    train batch going into the RLModule for updating (`forward_train()` call) contains
    at the minimum:
    - Observations: From all episodes under the SampleBatch.OBS key.
    - Actions, rewards, terminal/truncation flags: From all episodes under the
    respective keys.
    - All data inside the episodes' `extra_model_outs` property, e.g. action logp and
    action probs.
    - States: If the RLModule is stateful, the episodes' STATE_OUTS will be extracted
    and restructured under a new STATE_IN key in such a way that the resulting STATE_IN
    batch has the shape (B', ...). Here, B' is the sum of splits we have to do over
    the given episodes, such that each chunk is at most `max_seq_len` long (T-axis).
    Also, all other data will be properly reshaped into (B, T=max_seq_len, ...) and
    will be zero-padded, if necessary.

    If the user wants to customize their own data under the given keys (e.g. obs,
    actions, ...), they can extract from the episodes or recompute from `input_`
    their own data and store it under those keys (in `input_`). In such a case, this
    connector will not touch the data under these keys.
    """
    def __call__(self, input_: Any, episodes, ctx: ConnectorContextV2, **kwargs):
        # If episodes are provided, extract the essential data from them, but only if
        # this data is not present yet in `input_`.
        if not episodes:
            return input_

        # Get data dicts for all episodes.
        data_dicts = [episode.get_data_dict() for episode in episodes]

        state_in = None
        T = ctx.rl_module.config.model_config_dict.get("max_seq_len")

        # Special handling of STATE_OUT/STATE_IN keys:
        if ctx.rl_module.is_stateful() and STATE_IN not in input_:
            if T is None:
                raise ValueError(
                    "You are using a stateful RLModule and are not providing custom "
                    f"'{STATE_IN}' data through your connector(s)! Therefore, you need "
                    "to provide the 'max_seq_len' key inside your model config dict. "
                    "You can set this dict and/or override keys in it via "
                    "`config.training(model={'max_seq_len': x})`."
                )
            # Get model init state.
            init_state = ctx.rl_module.get_initial_state()
            # Get STATE_OUTs for all episodes and only keep those (as STATE_INs) that
            # are located at the `max_seq_len` edges (state inputs to RNNs only have a
            # B-axis, no T-axis).
            state_ins = []
            for episode, data_dict in zip(episodes, data_dicts):
                # Remove state outs (should not be part of the T-axis rearrangements).
                state_outs = data_dict.pop(STATE_OUT)
                state_ins.append(tree.map_structure(
                    # [::T] = only keep every Tth (max_seq_len) state in.
                    # [:-1] = shift state outs by one (ignore very last state out, but
                    # therefore add the init state at the beginning).
                    lambda i, o: np.concatenate([[i.numpy()], o[:-1]])[::T],
                    (
                        # Episode has a (reset) beginning -> Prepend initial state.
                        init_state if episode.t_started == 0
                        # Episode starts somewhere in the middle (is a cut continuation
                        # chunk) -> Use previous chunk's last STATE_OUT as initial state.
                        else episode.get_extra_model_outputs(
                            key=STATE_OUT, indices=-len(episode)-1
                        )
                    ),
                    state_outs,
                ))
            # Concatenate the individual episodes' state ins.
            state_in = tree.map_structure(lambda *s: np.concatenate(s), *state_ins)

            # Before adding anything else to the `input_`, add the time axis to existing
            # data.
            input_ = tree.map_structure(
                lambda s: split_and_pad_single_record(s, episodes, T=T),
                input_,
            )

            # Set the reduce function for all the data we might still have to extract
            # from our list of episodes. This function takes a list of data (e.g. obs)
            # with each item in the list representing one episode and properly
            # splits along the time axis and zero-pads if necessary (based on
            # max_seq_len).
            reduce_fn = partial(split_and_pad, T=T)

        # No stateful module, normal batch (w/o T-axis or zero-padding).
        else:
            # Set the reduce function for all the data we might still have to extract
            # from our list of episodes. Simply concatenate the data from the different
            # episodes along the batch axis (axis=0).
            reduce_fn = np.concatenate

        # Extract all data from the episodes, if not already in `input_`.
        for key in [
            SampleBatch.OBS,
            SampleBatch.ACTIONS,
            SampleBatch.REWARDS,
            SampleBatch.TERMINATEDS,
            SampleBatch.TRUNCATEDS,
            SampleBatch.T,#TODO: remove (normally not needed in train batch)
            *episodes[0].extra_model_outputs.keys()
        ]:
            if key not in input_ and key != STATE_OUT:
                # Concatenate everything together (along B-axis=0).
                input_[key] = tree.map_structure(
                    lambda *s: reduce_fn(s),
                    *[d[key] for d in data_dicts],
                )

        # Infos (always as lists).
        if SampleBatch.INFOS not in input_:
            input_[SampleBatch.INFOS] = sum(
                [d[SampleBatch.INFOS] for d in data_dicts],
                [],
            )

        if ctx.rl_module.is_stateful():
            # Now that all "normal" fields are time-dim'd and zero-padded, add
            # the STATE_IN column to `input_`.
            input_[STATE_IN] = state_in
            # Create the zero-padding loss mask.
            input_["loss_mask"], input_[SampleBatch.SEQ_LENS] = create_mask_and_seq_lens(
                episode_lens=[len(episode) for episode in episodes],
                T=T,
            )

        return input_


def split_and_pad(episodes_data, T):
    all_chunks = []

    for data in episodes_data:
        num_chunks = int(np.ceil(data.shape[0] / T))

        for i in range(num_chunks):
            start_index = i * T
            end_index = start_index + T

            # Extract the chunk
            chunk = data[start_index:end_index]

            # Pad the chunk if it's shorter than T
            if chunk.shape[0] < T:
                padding_shape = [(0, T - chunk.shape[0])] + [(0, 0) for _ in range(chunk.ndim - 1)]
                chunk = np.pad(chunk, pad_width=padding_shape, mode="constant")

            all_chunks.append(chunk)

    # Combine all chunks into a single array
    result = np.concatenate(all_chunks, axis=0)

    # Reshape the array to include the time dimension T
    # The new shape should be (-1, T) + original dimensions (excluding the batch dimension)
    result = result.reshape((-1, T) + result.shape[1:])

    return result


def split_and_pad_single_record(data, episodes, T):
    episodes_data = []
    idx = 0
    for episode in episodes:
        len_ = len(episode)
        episodes_data.append(data[idx:idx + len_])
        idx += len_
    return split_and_pad(episodes_data, T)


def create_mask_and_seq_lens(episode_lens, T):
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
