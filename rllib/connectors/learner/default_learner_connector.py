from collections import defaultdict
from functools import partial
from typing import Any, List, Optional

import numpy as np
import tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.models.base import STATE_IN, STATE_OUT
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.policy.sample_batch import (
    DEFAULT_POLICY_ID,
    MultiAgentBatch,
    SampleBatch,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import EpisodeType


class DefaultLearnerConnector(ConnectorV2):
    """Connector added by default by RLlib to the end of any learner connector pipeline.

    If provided with `episodes` data, this connector piece makes sure that the final
    train batch going into the RLModule for updating (`forward_train()` call) contains
    at the minimum:
    - Observations: From all episodes under the SampleBatch.OBS key.
    - Actions, rewards, terminal/truncation flags: From all episodes under the
    respective keys.
    - All data inside the episodes' `extra_model_outs` property, e.g. action logp and
    action probs under the respective keys.
    - States: If the RLModule is stateful, the episodes' STATE_OUTS will be extracted
    and restructured under a new STATE_IN key in such a way that the resulting STATE_IN
    batch has the shape (B', ...). Here, B' is the sum of splits we have to do over
    the given episodes, such that each chunk is at most `max_seq_len` long (T-axis).
    Also, all other data will be properly reshaped into (B, T=max_seq_len, ...) and
    will be zero-padded, if necessary.

    If the user wants to customize their own data under the given keys (e.g. obs,
    actions, ...), they can extract from the episodes or recompute from `data`
    their own data and store it in `data` under those keys. In this case, the default
    connector will not change the data under these keys and simply act as a
    pass-through.
    """

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        data: Any,
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        # If episodes are provided, extract the essential data from them, but only if
        # respective keys are not present yet in `data`.
        if not episodes:
            return data

        if isinstance(episodes[0], MultiAgentEpisode):
            # TODO (sven): Support multi-agent cases, in which user defined learner
            #  connector pieces before this (default) one here.
            #  Probably the only solution here would be to introduce the connector
            #  input/output types.
            # assert not data
            module_to_episodes = defaultdict(list)
            for ma_episode in episodes:
                for agent_id, sa_episode in ma_episode.agent_episodes.items():
                    module_to_episodes[ma_episode.agent_to_module_map[agent_id]].append(
                        sa_episode
                    )
        else:
            module_to_episodes = {DEFAULT_POLICY_ID: episodes}

        return_data = {}
        for module_id, episode_list in module_to_episodes.items():
            # Get the data dicts for all episodes.
            data_dicts = [eps.get_data_dict() for eps in episode_list]
            sa_data = data.get(module_id, {})

            state_in = None
            T = rl_module.config.modules[module_id].model_config_dict.get("max_seq_len")

            # RLModule is stateful and STATE_IN is not found in `data` (user's custom
            # connectors have not provided this information yet) -> Perform separate
            # handling of STATE_OUT/STATE_IN keys:
            if rl_module.is_stateful() and STATE_IN not in sa_data:
                if T is None:
                    raise ValueError(
                        "You are using a stateful RLModule and are not providing "
                        "custom '{STATE_IN}' data through your connector(s)! "
                        "Therefore, you need to provide the 'max_seq_len' key inside "
                        "your model config dict. You can set this dict and/or override "
                        "keys in it via `config.training(model={'max_seq_len': x})`."
                    )
                # Get model init state.
                init_state = convert_to_numpy(rl_module.get_initial_state())
                # Get STATE_OUTs for all episodes and only keep those (as STATE_INs)
                # that are located at the `max_seq_len` edges (state inputs to RNNs only
                # have a B-axis, no T-axis).
                state_ins = []
                for episode, data_dict in zip(episode_list, data_dicts):
                    # Remove state outs (should not be part of the T-axis
                    # rearrangements).
                    state_outs = data_dict.pop(STATE_OUT)
                    state_ins.append(
                        tree.map_structure(
                            # [::T] = only keep every Tth (max_seq_len) state in.
                            # [:-1] = shift state outs by one (ignore very last state
                            # out, but therefore add the init state at the beginning).
                            lambda i, o: np.concatenate([[i], o[:-1]])[::T],
                            (
                                # Episode has a (reset) beginning -> Prepend initial
                                # state.
                                init_state
                                if episode.t_started == 0
                                # Episode starts somewhere in the middle (is a cut
                                # continuation chunk) -> Use previous chunk's last
                                # STATE_OUT as initial state.
                                else episode.get_extra_model_outputs(
                                    key=STATE_OUT,
                                    indices=-1,
                                    neg_indices_left_of_zero=True,
                                )
                            ),
                            state_outs,
                        )
                    )
                # Concatenate the individual episodes' STATE_INs.
                state_in = tree.map_structure(lambda *s: np.concatenate(s), *state_ins)

                # Before adding anything else to the `data`, add the time axis to
                # existing data.
                sa_data = tree.map_structure(
                    lambda s: split_and_pad_single_record(s, episode_list, T=T),
                    sa_data,
                )

                # Set the reduce function for all the data we might still have to
                # extract from our list of episodes. This function takes a list of data
                # (e.g. obs) with each item in the list representing one episode and
                # properly splits along the time axis and zero-pads if necessary (based
                # on T=max_seq_len).
                reduce_fn = partial(split_and_pad, T=T)

            # No stateful module, normal batch (w/o T-axis or zero-padding).
            else:
                # Set the reduce function for all the data we might still have to
                # extract from our list of episodes. Simply concatenate the data from
                # the different episodes along the batch axis (axis=0).
                reduce_fn = np.concatenate

            # Extract all data from the episodes and add to `data`, if not already in
            # `data`.
            for key in [
                SampleBatch.OBS,
                SampleBatch.ACTIONS,
                SampleBatch.REWARDS,
                SampleBatch.TERMINATEDS,
                SampleBatch.TRUNCATEDS,
                SampleBatch.T,  # TODO: remove (normally not needed in train batch)
                *episode_list[0].extra_model_outputs.keys(),
            ]:
                if key not in sa_data and key != STATE_OUT:
                    # Concatenate everything together (along B-axis=0).
                    sa_data[key] = tree.map_structure(
                        lambda *s: reduce_fn(s),
                        *[d[key] for d in data_dicts],
                    )

            # Handle infos (always lists, not numpy arrays).
            if SampleBatch.INFOS not in sa_data:
                sa_data[SampleBatch.INFOS] = sum(
                    [d[SampleBatch.INFOS] for d in data_dicts],
                    [],
                )

            # Now that all "normal" fields are time-dim'd and zero-padded, add
            # the STATE_IN column to `data`.
            if rl_module.is_stateful():
                sa_data[STATE_IN] = state_in
                # Also, create the loss mask (b/c of our now possibly zero-padded data)
                # as well as the seq_lens array and add these to `data` as well.
                (
                    sa_data["loss_mask"],
                    sa_data[SampleBatch.SEQ_LENS],
                ) = create_mask_and_seq_lens(
                    episode_lens=[len(eps) for eps in episode_list],
                    T=T,
                )

            # TODO (sven): Convert data to proper tensor formats, depending on framework
            #  used by the RLModule. We cannot do this right now as the RLModule does
            #  NOT know its own device. Only the Learner knows the device. Also, on the
            #  EnvRunner side, we assume that it's always the CPU (even though one could
            #  imagine a GPU-based EnvRunner + RLModule for sampling).
            # if rl_module.framework == "torch":
            #    sa_data = convert_to_torch_tensor(sa_data, device=??)
            # elif rl_module.framework == "tf2":
            #    sa_data =

            return_data[module_id] = sa_data

        # Convert to MultiAgentBatch.
        return MultiAgentBatch(
            policy_batches={
                module_id: SampleBatch(batch)
                for module_id, batch in return_data.items()
            },
            env_steps=sum(len(e) for e in episodes),
        )


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
                padding_shape = [(0, T - chunk.shape[0])] + [
                    (0, 0) for _ in range(chunk.ndim - 1)
                ]
                chunk = np.pad(chunk, pad_width=padding_shape, mode="constant")

            all_chunks.append(chunk)

    # Combine all chunks into a single array
    result = np.concatenate(all_chunks, axis=0)

    # Reshape the array to include the time dimension T.
    # The new shape should be (-1, T) + original dimensions (excluding the batch
    # dimension)
    result = result.reshape((-1, T) + result.shape[1:])

    return result


def split_and_pad_single_record(data, episodes, T):
    episodes_data = []
    idx = 0
    for episode in episodes:
        len_ = len(episode)
        episodes_data.append(data[idx : idx + len_])
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
