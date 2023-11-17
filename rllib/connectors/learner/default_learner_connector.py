from functools import partial
from typing import Any, List

import numpy as np
import tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.connector_context_v2 import ConnectorContextV2
from ray.rllib.connectors.utils.zero_padding import (
    create_mask_and_seq_lens,
    split_and_pad,
    split_and_pad_single_record,
)
from ray.rllib.core.models.base import STATE_IN, STATE_OUT
from ray.rllib.policy.sample_batch import SampleBatch
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
    actions, ...), they can extract from the episodes or recompute from `input_`
    their own data and store it in `input_` under those keys. In this case, the default
    connector will not change the data under these keys and simply act as a
    pass-through.
    """

    def __call__(
        self,
        input_: Any,
        episodes: List[EpisodeType],
        ctx: ConnectorContextV2,
        **kwargs,
    ) -> Any:
        # If episodes are provided, extract the essential data from them, but only if
        # respective keys are not present yet in `input_`.
        if not episodes:
            return input_

        # Get the data dicts for all episodes.
        data_dicts = [episode.get_data_dict() for episode in episodes]

        state_in = None
        T = ctx.rl_module.config.model_config_dict.get("max_seq_len")

        # RLModule is stateful and STATE_IN is not found in `input_` (user's custom
        # connectors have not provided this information yet) -> Perform separate
        # handling of STATE_OUT/STATE_IN keys:
        if ctx.rl_module.is_stateful() and STATE_IN not in input_:
            if T is None:
                raise ValueError(
                    "You are using a stateful RLModule and are not providing custom "
                    f"'{STATE_IN}' data through your connector(s)! Therefore, you need "
                    "to provide the 'max_seq_len' key inside your model config dict. "
                    "You can set this dict and/or override keys in it via "
                    "`config.training(model={'max_seq_len': x})`."
                )

            # Before adding anything to `input_`, add the time axis to existing data.
            input_ = tree.map_structure(
                lambda s: split_and_pad_single_record(s, episodes, T=T),
                input_,
            )

            # Get STATE_OUTs for all episodes and only keep those (as STATE_INs) that
            # are located at the `max_seq_len` edges (state inputs to RNNs only have a
            # B-axis, no T-axis).
            init_state = convert_to_numpy(ctx.rl_module.get_initial_state())
            state_ins = []
            for episode, data_dict in zip(episodes, data_dicts):
                # Remove state outs (should not be part of the T-axis rearrangements).
                state_outs = data_dict.pop(STATE_OUT)
                state_ins.append(
                    tree.map_structure(
                        # [::T] = only keep every Tth (max_seq_len) state in.
                        # [:-1] = shift state outs by one (ignore very last state out,
                        # but therefore add the init state at the beginning).
                        lambda i, o: np.concatenate([[i], o[:-1]])[::T],
                        (
                            # Episode has a (reset) beginning -> Prepend initial state.
                            init_state
                            if episode.t_started == 0
                            # Episode starts somewhere in the middle (is a cut
                            # continuation chunk) -> Use previous chunk's last STATE_OUT
                            # as initial state.
                            else episode.get_extra_model_outputs(
                                key=STATE_OUT, indices=-len(episode) - 1
                            )
                        ),
                        state_outs,
                    )
                )
            # Concatenate the individual episodes' STATE_INs.
            state_in = tree.map_structure(lambda *s: np.concatenate(s), *state_ins)

            # Set the reduce function for all the data we might still have to extract
            # from our list of episodes. This function takes a list of data (e.g. obs)
            # with each item in the list representing one episode and properly
            # splits along the time axis and zero-pads if necessary (based on
            # T=max_seq_len).
            reduce_fn = partial(split_and_pad, T=T)

        # No stateful module, normal batch (w/o T-axis or zero-padding).
        else:
            # Set the reduce function for all the data we might still have to extract
            # from our list of episodes. Simply concatenate the data from the different
            # episodes along the batch axis (axis=0).
            reduce_fn = np.concatenate

        # Extract all data from the episodes and add to `input_`, if not already in
        # `input_`.
        for key in [
            SampleBatch.OBS,
            SampleBatch.ACTIONS,
            SampleBatch.REWARDS,
            SampleBatch.TERMINATEDS,
            SampleBatch.TRUNCATEDS,
            SampleBatch.T,  # TODO: remove (normally not needed in train batch)
            *episodes[0].extra_model_outputs.keys(),
        ]:
            if key not in input_ and key != STATE_OUT:
                # Concatenate everything together (along B-axis=0).
                input_[key] = tree.map_structure(
                    lambda *s: reduce_fn(s),
                    *[d[key] for d in data_dicts],
                )

        # Handle infos (always lists, not numpy arrays).
        if SampleBatch.INFOS not in input_:
            input_[SampleBatch.INFOS] = sum(
                [d[SampleBatch.INFOS] for d in data_dicts],
                [],
            )

        # Now that all "normal" fields are time-dim'd and zero-padded, add
        # the STATE_IN column to `input_`.
        if ctx.rl_module.is_stateful():
            input_[STATE_IN] = state_in
            # Also, create the loss mask (b/c of our now possibly zero-padded data) as
            # well as the seq_lens array and add these to `input_` as well.
            (
                input_["loss_mask"],
                input_[SampleBatch.SEQ_LENS],
            ) = create_mask_and_seq_lens(
                episode_lens=[len(episode) for episode in episodes],
                T=T,
            )

        return input_
