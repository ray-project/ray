from collections import deque
import math
from typing import Any, List, Optional

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.spaces.space_utils import batch, BatchedNdArray
from ray.rllib.utils.typing import EpisodeType


class AddStatesFromEpisodesToBatch(ConnectorV2):
    """Gets last STATE_OUT from running episode and adds it as STATE_IN to the batch.

    If the RLModule is stateful, the episodes' STATE_OUTS will be extracted
    and restructured under a new STATE_IN key.
    As a Learner connector, the resulting STATE_IN batch has the shape (B', ...).
    Here, B' is the sum of splits we have to do over the given episodes, such that each
    chunk is at most `max_seq_len` long (T-axis).
    As a EnvToModule connector, the resulting STATE_IN batch simply consists of n
    states coming from n vectorized environments/episodes.

    Also, all other data (observations, rewards, etc.. if applicable) will be properly
    reshaped into (B, T=max_seq_len (learner) or 1 (env-to-module), ...) and will be
    zero-padded, if necessary.

    - Operates on a list of Episode objects.
    - Gets the most recent STATE_OUT from all the given episodes and adds them under
    the STATE_IN key to the batch under construction.
    - Does NOT alter any data in the given episodes.
    - Can be used in EnvToModule and Learner connector pipelines.

    .. testcode::

        from ray.rllib.connectors.common import AddStatesFromEpisodesToBatch
        from ray.rllib.core.columns import Columns
        from ray.rllib.env.single_agent_episode import SingleAgentEpisode
        from ray.rllib.utils.test_utils import check

        # Create a simple dummy class, pretending to be an RLModule with
        # `get_initial_state` overridden:
        class MyStateModule:
            def is_stateful(self):
                return True

            def get_initial_state(self):
                return 0.0


        # Create an empty episode. The connector should use the RLModule's initial state
        # to populate STATE_IN for the next forward pass.
        episode = SingleAgentEpisode()

        rl_module = MyStateModule()
        rl_module_init_state = rl_module.get_initial_state()

        # Create an instance of this class (as a env-to-module connector).
        connector = AddStatesFromEpisodesToBatch(as_learner_connector=False)

        # Call the connector.
        output_data = connector(
            rl_module=rl_module,
            data={},
            episodes=[episode],
            shared_data={},
        )
        # The output data's STATE_IN key should now contain the RLModule's initial state
        # plus the one state out found in the episode in a "per-episode organized"
        # fashion.
        check(
            output_data[Columns.STATE_IN],
            {
                (episode.id_,): [rl_module_init_state],
            },
        )

        # Create a SingleAgentEpisodes containing 5 observations,
        # 4 actions and 4 rewards, and 4 STATE_OUTs.
        # The same connector should now use the episode-stored last STATE_OUT as
        # STATE_IN for the next forward pass.
        episode = SingleAgentEpisode(
            observations=[0, 1, 2, 3, 4],
            actions=[1, 2, 3, 4],
            rewards=[1.0, 2.0, 3.0, 4.0],
            # STATE_OUT in episode will show up under STATE_IN in the batch.
            extra_model_outputs={
                Columns.STATE_OUT: [-4.0, -3.0, -2.0, -1.0],
            },
            len_lookback_buffer = 0,
        )

        # Call the connector.
        output_data = connector(
            rl_module=rl_module,
            data={},
            episodes=[episode],
            shared_data={},
        )
        # The output data's STATE_IN key should now contain the episode's last
        # STATE_OUT, NOT the RLModule's initial state in a "per-episode organized"
        # fashion.
        check(
            output_data[Columns.STATE_IN],
            {
                # Expect the episode's last STATE_OUT.
                (episode.id_,): [-1.0],
            },
        )

        # Create a new connector as a learner connector with a RNN seq len of 2 (for
        # testing purposes only). Passing the same data through this learner connector,
        # we expect the STATE_IN data to contain a) the initial module state and then
        # every 2nd STATE_OUT stored in the episode.
        connector = AddStatesFromEpisodesToBatch(
            as_learner_connector=True,
            max_seq_len=2,
        )

        # Call the connector.
        output_data = connector(
            rl_module=rl_module,
            data={},
            episodes=[episode.finalize()],
            shared_data={},
        )
        check(
            output_data[Columns.STATE_IN],
            {
                # Expect initial module state + every 2nd STATE_OUT from episode, but
                # not the very last one (just like the very last observation, this data
                # is NOT passed through the forward_train, b/c there is nothing to learn
                # at that timestep, unless we need to compute e.g. bootstrap value
                # predictions).
                # Also note that the different STATE_IN timesteps are already present
                # as one batched item per episode in the list.
                (episode.id_,): [[rl_module_init_state, -3.0]],
            },
        )
    """

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        max_seq_len: Optional[int] = None,
        as_learner_connector: bool = False,
        **kwargs,
    ):
        """Initializes a AddObservationsFromEpisodesToBatch instance.

        Args:
            as_learner_connector: Whether this connector is part of a Learner connector
                pipeline, as opposed to a env-to-module pipeline. As a Learner
                connector, it will add an entire Episode's observations (each timestep)
                to the batch.
        """
        super().__init__(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            **kwargs,
        )

        self._as_learner_connector = as_learner_connector
        self.max_seq_len = max_seq_len
        if self._as_learner_connector and self.max_seq_len is None:
            raise ValueError(
                "Cannot run `AddStatesFromEpisodesToBatch` as Learner connector without"
                " `max_seq_len` constructor argument!"
            )

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        data: Optional[Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        # If not stateful OR STATE_IN already in data, early out.
        if not rl_module.is_stateful() or Columns.STATE_IN in data:
            return data

        # Make all inputs (other than STATE_IN) have an additional T-axis.
        # Since data has not been batched yet (we are still operating on lists in the
        # batch), we add this time axis as 0 (not 1). When we batch, the batch axis will
        # be 0 and the time axis will be 1.
        # Also, let module-to-env pipeline know that we had added a single timestep
        # time rank to the data (to remove it again).
        if not self._as_learner_connector:
            for column, column_data in data.copy().items():
                self.foreach_batch_item_change_in_place(
                    batch=data,
                    column=column,
                    func=lambda item, eps_id, aid, mid: (
                        item
                        if mid is not None and not rl_module[mid].is_stateful()
                        # Expand on axis 0 (the to-be-time-dim) if item has not been
                        # batched yet, otherwise axis=1 (the time-dim).
                        else tree.map_structure(
                            lambda s: np.expand_dims(
                                s, axis=(1 if isinstance(s, BatchedNdArray) else 0)
                            ),
                            item,
                        )
                    ),
                )
            shared_data["_added_single_ts_time_rank"] = True
        else:
            # Before adding STATE_IN to the `data`, zero-pad existing data and batch
            # into max_seq_len chunks.
            for column, column_data in data.copy().items():
                for key, item_list in column_data.items():
                    if column != Columns.INFOS:
                        column_data[key] = split_and_zero_pad_list(
                            item_list, T=self.max_seq_len
                        )

        for sa_episode in self.single_agent_episode_iterator(
            episodes,
            # If Learner connector, get all episodes (for train batch).
            # If EnvToModule, get only those ongoing episodes that just had their
            # agent step (b/c those are the ones we need to compute actions for next).
            agents_that_stepped_only=not self._as_learner_connector,
        ):
            if self._as_learner_connector:
                assert sa_episode.is_finalized

                # Multi-agent case: Extract correct single agent RLModule (to get the
                # state for individually).
                sa_module = rl_module
                if sa_episode.module_id is not None:
                    sa_module = rl_module[sa_episode.module_id]
                else:
                    sa_module = (
                        rl_module[DEFAULT_MODULE_ID]
                        if isinstance(rl_module, MultiAgentRLModule)
                        else rl_module
                    )
                # This single-agent RLModule is NOT stateful -> Skip.
                if not sa_module.is_stateful():
                    continue

                if self.max_seq_len is None:
                    raise ValueError(
                        "You are using a stateful RLModule and are not providing "
                        f"custom '{Columns.STATE_IN}' data through your connector(s)! "
                        "Therefore, you need to provide the 'max_seq_len' key inside "
                        "your model config dict. You can set this dict and/or override "
                        "keys in it via `config.training(model={'max_seq_len': x})`."
                    )

                # look_back_state.shape=([state-dim],)
                look_back_state = (
                    # Episode has a (reset) beginning -> Prepend initial
                    # state.
                    convert_to_numpy(sa_module.get_initial_state())
                    if sa_episode.t_started == 0
                    # Episode starts somewhere in the middle (is a cut
                    # continuation chunk) -> Use previous chunk's last
                    # STATE_OUT as initial state.
                    else sa_episode.get_extra_model_outputs(
                        key=Columns.STATE_OUT,
                        indices=-1,
                        neg_indices_left_of_zero=True,
                    )
                )
                # state_outs.shape=(T,[state-dim])  T=episode len
                state_outs = sa_episode.get_extra_model_outputs(key=Columns.STATE_OUT)
                self.add_n_batch_items(
                    batch=data,
                    column=Columns.STATE_IN,
                    # items_to_add.shape=(B,[state-dim])  # B=episode len // max_seq_len
                    items_to_add=tree.map_structure(
                        # Explanation:
                        # [::max_seq_len]: only keep every Tth state.
                        # [:-1]: Shift state outs by one, ignore very last
                        # STATE_OUT (but therefore add the lookback/init state at
                        # the beginning).
                        lambda i, o: np.concatenate([[i], o[:-1]])[:: self.max_seq_len],
                        look_back_state,
                        state_outs,
                    ),
                    num_items=int(math.ceil(len(sa_episode) / self.max_seq_len)),
                    single_agent_episode=sa_episode,
                )

                # Also, create the loss mask (b/c of our now possibly zero-padded data)
                # as well as the seq_lens array and add these to `data` as well.
                mask, seq_lens = create_mask_and_seq_lens(
                    len(sa_episode), self.max_seq_len
                )
                self.add_n_batch_items(
                    batch=data,
                    column=Columns.SEQ_LENS,
                    items_to_add=seq_lens,
                    num_items=len(seq_lens),
                    single_agent_episode=sa_episode,
                )
                self.add_n_batch_items(
                    batch=data,
                    column=Columns.LOSS_MASK,
                    items_to_add=mask,
                    num_items=len(mask),
                    single_agent_episode=sa_episode,
                )
            else:
                assert not sa_episode.is_finalized

                # Multi-agent case: Extract correct single agent RLModule (to get the
                # state for individually).
                sa_module = rl_module
                if sa_episode.module_id is not None:
                    sa_module = rl_module[sa_episode.module_id]
                # This single-agent RLModule is NOT stateful -> Skip.
                if not sa_module.is_stateful():
                    continue

                # Episode just started -> Get initial state from our RLModule.
                if sa_episode.t_started == 0 and len(sa_episode) == 0:
                    state = sa_module.get_initial_state()
                # Episode is already ongoing -> Use most recent STATE_OUT.
                else:
                    state = sa_episode.get_extra_model_outputs(
                        key=Columns.STATE_OUT, indices=-1
                    )
                self.add_batch_item(
                    data,
                    Columns.STATE_IN,
                    item_to_add=state,
                    single_agent_episode=sa_episode,
                )

        return data


def split_and_zero_pad_list(item_list, T: int):
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


def create_mask_and_seq_lens(episode_len, T):
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
