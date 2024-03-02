import math
from typing import Any, List, Optional

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.models.base import STATE_IN, STATE_OUT
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.spaces.space_utils import batch, unbatch
from ray.rllib.utils.typing import EpisodeType


class AddStatesFromEpisodesToBatch(ConnectorV2):
    """Gets last STATE_OUT from running episode and adds it as STATE_IN to the batch.

    - Operates on a list of Episode objects.
    - Gets the most recent STATE_OUT from all the given episodes and adds them under
    the STATE_IN key to the batch under construction.
    - Does NOT alter any data in the given episodes.
    - Can be used in EnvToModule and Learner connector pipelines.

    .. testcode::

        import gymnasium as gym
        import numpy as np

        from ray.rllib.connectors.common import AddStatesFromEpisodesToBatch
        from ray.rllib.env.single_agent_episode import SingleAgentEpisode
        from ray.rllib.utils.test_utils import check

        # Create two dummy SingleAgentEpisodes, each containing 2 observations,
        # 1 action and 1 reward (both are length=1).
        obs_space = gym.spaces.Box(-1.0, 1.0, (2,), np.float32)
        act_space = gym.spaces.Discrete(2)

        episodes = [SingleAgentEpisode(
            observation_space=obs_space,
            observations=[obs_space.sample(), obs_space.sample()],
            actions=[act_space.sample()],
            rewards=[1.0],
            len_lookback_buffer=0,
        ) for _ in range(2)]
        eps_1_last_obs = episodes[0].get_observations(-1)
        eps_2_last_obs = episodes[1].get_observations(-1)
        print(f"1st Episode's last obs is {eps_1_last_obs}")
        print(f"2nd Episode's last obs is {eps_2_last_obs}")

        # Create an instance of this class, providing the obs- and action spaces.
        connector = AddObservationsFromEpisodesToBatch(obs_space, act_space)

        # Call the connector with the two created episodes.
        # Note that this particular connector works without an RLModule, so we
        # simplify here for the sake of this example.
        output_data = connector(
            rl_module=None,
            data={},
            episodes=episodes,
            explore=True,
            shared_data={},
        )
        # The output data should now contain the last observations of both episodes.
        check(output_data, {"obs": [eps_1_last_obs, eps_2_last_obs]})
    """

    def __init__(
        self,
        input_observation_space: gym.Space = None,
        input_action_space: gym.Space = None,
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
        if not rl_module.is_stateful() or STATE_IN in data:
            return data

        # Make all inputs (other than STATE_IN) have an additional T-axis.
        # Since data has not been batched yet (we are still operating on lists in the
        # batch), we add this time axis as 0 (not 1). When we batch, the batch axis will
        # be 0 and the time axis will be 1.
        # Also, let module-to-env pipeline know that we had added a single timestep
        # time rank to the data (to remove it again).
        if not self._as_learner_connector:
            data = tree.map_structure(lambda s: np.expand_dims(s, axis=0), data)
            shared_data["_added_single_ts_time_rank"] = True
        else:
            # Before adding STATE_IN to the `data`, zero-pad existing data and batch
            # into max_seq_len chunks.
            for column, column_data in data.copy().items():
                for key, item_list in column_data.items():
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
                    sa_module = rl_module[DEFAULT_POLICY_ID]

                if self.max_seq_len is None:
                    raise ValueError(
                        "You are using a stateful RLModule and are not providing "
                        "custom '{STATE_IN}' data through your connector(s)! "
                        "Therefore, you need to provide the 'max_seq_len' key inside "
                        "your model config dict. You can set this dict and/or override "
                        "keys in it via `config.training(model={'max_seq_len': x})`."
                    )

                look_back_state = (
                    # Episode has a (reset) beginning -> Prepend initial
                    # state.
                    convert_to_numpy(sa_module.get_initial_state())
                    if sa_episode.t_started == 0
                    # Episode starts somewhere in the middle (is a cut
                    # continuation chunk) -> Use previous chunk's last
                    # STATE_OUT as initial state.
                    else sa_episode.get_extra_model_outputs(
                        key=STATE_OUT,
                        indices=-1,
                        neg_indices_left_of_zero=True,
                    )
                )
                state_outs = sa_episode.get_extra_model_outputs(key=STATE_OUT)
                self.add_n_batch_items(
                    batch=data,
                    column=STATE_IN,
                    items_to_add=unbatch(
                        tree.map_structure(
                            # [::max_seq_len]: only keep every Tth state in value.
                            # [:-1]: Shift state outs by one (ignore very last state
                            # out, but therefore add the lookback/init state at the
                            # beginning).
                            lambda i, o: np.concatenate([[i], o[:-1]])[
                                :: self.max_seq_len
                            ],
                            look_back_state,
                            state_outs,
                        )
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
                    column=SampleBatch.SEQ_LENS,
                    items_to_add=seq_lens,
                    num_items=len(seq_lens),
                    single_agent_episode=sa_episode,
                )
                self.add_n_batch_items(
                    batch=data,
                    column="loss_mask",
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

                # Episode just started -> Get initial state from our RLModule.
                if len(sa_episode) == 0:
                    state = sa_module.get_initial_state()
                # Episode is already ongoing -> Use most recent STATE_OUT.
                else:
                    state = sa_episode.get_extra_model_outputs(
                        key=STATE_OUT, indices=-1
                    )
                self.add_batch_item(
                    data,
                    STATE_IN,
                    item_to_add=state,
                    single_agent_episode=sa_episode,
                )

        return data


def split_and_zero_pad_list(item_list, T: int):
    zero_element = tree.map_structure(lambda s: np.zeros_like(s), item_list[0])

    # The replacement list (to be returned) for `items_list`.
    # Items list contains n individual items.
    # -> ret will contain m batched rows, where m == n // T and the last row
    # may be zero padded (until T).
    ret = []

    # List of the T-axis item, collected to form the next row.
    current_time_row = []
    current_t = 0

    for item in item_list:
        current_time_row.append(item)
        current_t += 1

        if current_t == T:
            ret.append(batch(current_time_row))
            current_time_row = []
            current_t = 0

    if current_t > 0 and current_t < T:
        current_time_row.extend([zero_element] * (T - current_t))
        ret.append(batch(current_time_row))

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
