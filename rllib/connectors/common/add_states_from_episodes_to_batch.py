import math
from typing import Any, Dict, List, Optional

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class AddStatesFromEpisodesToBatch(ConnectorV2):
    """Gets last STATE_OUT from running episode and adds it as STATE_IN to the batch.

    Note: This is one of the default env-to-module or Learner ConnectorV2 pieces that
    are added automatically by RLlib into every env-to-module/Learner connector
    pipeline, unless `config.add_default_connectors_to_env_to_module_pipeline` or
    `config.add_default_connectors_to_learner_pipeline ` are set to
    False.

    The default env-to-module connector pipeline is:
    [
        [0 or more user defined ConnectorV2 pieces],
        AddObservationsFromEpisodesToBatch,
        AddTimeDimToBatchAndZeroPad,
        AddStatesFromEpisodesToBatch,
        AgentToModuleMapping,  # only in multi-agent setups!
        BatchIndividualItems,
        NumpyToTensor,
    ]
    The default Learner connector pipeline is:
    [
        [0 or more user defined ConnectorV2 pieces],
        AddObservationsFromEpisodesToBatch,
        AddColumnsFromEpisodesToTrainBatch,
        AddTimeDimToBatchAndZeroPad,
        AddStatesFromEpisodesToBatch,
        AgentToModuleMapping,  # only in multi-agent setups!
        BatchIndividualItems,
        NumpyToTensor,
    ]

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

    This ConnectorV2:
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
        # `get_initial_state`, `is_stateful` and `model_config` property defined:
        class MyStateModule:
            # dummy config
            model_config = {"max_seq_len": 2}

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
        output_batch = connector(
            rl_module=rl_module,
            batch={},
            episodes=[episode],
            shared_data={},
        )
        # The output data's STATE_IN key should now contain the RLModule's initial state
        # plus the one state out found in the episode in a "per-episode organized"
        # fashion.
        check(
            output_batch[Columns.STATE_IN],
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
        output_batch = connector(
            rl_module=rl_module,
            batch={},
            episodes=[episode],
            shared_data={},
        )
        # The output data's STATE_IN key should now contain the episode's last
        # STATE_OUT, NOT the RLModule's initial state in a "per-episode organized"
        # fashion.
        check(
            output_batch[Columns.STATE_IN],
            {
                # Expect the episode's last STATE_OUT.
                (episode.id_,): [-1.0],
            },
        )

        # Create a new connector as a learner connector with a RNN seq len of 2 (for
        # testing purposes only). Passing the same data through this learner connector,
        # we expect the STATE_IN data to contain a) the initial module state and then
        # every 2nd STATE_OUT stored in the episode.
        connector = AddStatesFromEpisodesToBatch(as_learner_connector=True)

        # Call the connector.
        output_batch = connector(
            rl_module=rl_module,
            batch={},
            episodes=[episode],
            shared_data={},
        )
        check(
            output_batch[Columns.STATE_IN],
            {
                # Expect initial module state + every 2nd STATE_OUT from episode, but
                # not the very last one (just like the very last observation, this data
                # is NOT passed through the forward_train, b/c there is nothing to learn
                # at that timestep, unless we need to compute e.g. bootstrap value
                # predictions).
                # Also note that the different STATE_IN timesteps are already present
                # as one batched item per episode in the list.
                (episode.id_,): [rl_module_init_state, -3.0],
            },
        )
    """

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
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

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Dict[str, Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        # If not stateful OR STATE_IN already in data, early out.
        if not rl_module.is_stateful() or Columns.STATE_IN in batch:
            return batch

        for sa_episode in self.single_agent_episode_iterator(
            episodes,
            # If Learner connector, get all episodes (for train batch).
            # If EnvToModule, get only those ongoing episodes that just had their
            # agent step (b/c those are the ones we need to compute actions for next).
            agents_that_stepped_only=not self._as_learner_connector,
        ):
            if self._as_learner_connector:
                # Multi-agent case: Extract correct single agent RLModule (to get its
                # individual state).
                if sa_episode.module_id is not None:
                    sa_module = rl_module[sa_episode.module_id]
                else:
                    sa_module = (
                        rl_module[DEFAULT_MODULE_ID]
                        if isinstance(rl_module, MultiRLModule)
                        else rl_module
                    )
                # This single-agent RLModule is NOT stateful -> Skip.
                if not sa_module.is_stateful():
                    continue

                max_seq_len = sa_module.model_config["max_seq_len"]

                # look_back_state.shape=([state-dim],)
                look_back_state = (
                    # Episode has a (reset) beginning -> Prepend initial
                    # state.
                    convert_to_numpy(sa_module.get_initial_state())
                    if sa_episode.t_started == 0
                    or (Columns.STATE_OUT not in sa_episode.extra_model_outputs)
                    # Episode starts somewhere in the middle (is a cut
                    # continuation chunk) -> Use previous chunk's last
                    # STATE_OUT as initial state.
                    else sa_episode.get_extra_model_outputs(
                        key=Columns.STATE_OUT,
                        indices=-1,
                        neg_index_as_lookback=True,
                    )
                )
                # If we have `"state_out"`s (e.g. from rollouts) use them for the
                # `"state_in"`s.
                if Columns.STATE_OUT in sa_episode.extra_model_outputs:
                    # state_outs.shape=(T,[state-dim])  T=episode len
                    state_outs = sa_episode.get_extra_model_outputs(
                        key=Columns.STATE_OUT
                    )
                # Otherwise, we have no `"state_out"` (e.g. because we are sampling
                # from offline data and the expert policy was not stateful).
                else:
                    # Then simply use the `look_back_state`, i.e. in this case the
                    # initial state as `"state_in` in training.
                    if sa_episode.is_numpy:
                        state_outs = tree.map_structure(
                            lambda a, _sae=sa_episode: np.repeat(
                                a[np.newaxis, ...], len(_sae), axis=0
                            ),
                            look_back_state,
                        )
                    else:
                        state_outs = [look_back_state for _ in range(len(sa_episode))]
                # Explanation:
                # B=episode len // max_seq_len
                # [::max_seq_len]: only keep every Tth state.
                # [:-1]: Shift state outs by one; ignore very last
                # STATE_OUT, but therefore add the lookback/init state at
                # the beginning.
                items_to_add = (
                    tree.map_structure(
                        lambda i, o, m=max_seq_len: np.concatenate([[i], o[:-1]])[::m],
                        look_back_state,
                        state_outs,
                    )
                    if sa_episode.is_numpy
                    else ([look_back_state] + state_outs[:-1])[::max_seq_len]
                )
                self.add_n_batch_items(
                    batch=batch,
                    column=Columns.STATE_IN,
                    items_to_add=items_to_add,
                    num_items=int(math.ceil(len(sa_episode) / max_seq_len)),
                    single_agent_episode=sa_episode,
                )
                if Columns.NEXT_OBS in batch:
                    items_to_add = (
                        tree.map_structure(
                            lambda i, m=max_seq_len: i[::m],
                            state_outs,
                        )
                        if sa_episode.is_numpy
                        else state_outs[::max_seq_len]
                    )
                    self.add_n_batch_items(
                        batch=batch,
                        column=Columns.NEXT_STATE_IN,
                        items_to_add=items_to_add,
                        num_items=int(math.ceil(len(sa_episode) / max_seq_len)),
                        single_agent_episode=sa_episode,
                    )

            else:
                assert not sa_episode.is_numpy

                # Multi-agent case: Extract correct single agent RLModule (to get the
                # state for individually).
                sa_module = rl_module
                if sa_episode.module_id is not None:
                    sa_module = rl_module[sa_episode.module_id]
                # This single-agent RLModule is NOT stateful -> Skip.
                if not sa_module.is_stateful():
                    continue

                # Episode just started or has no `"state_out"` (e.g. in offline
                # sampling) -> Get initial state from our RLModule.
                if (sa_episode.t_started == 0 and len(sa_episode) == 0) or (
                    Columns.STATE_OUT not in sa_episode.extra_model_outputs
                ):
                    state = sa_module.get_initial_state()
                # Episode is already ongoing -> Use most recent STATE_OUT.
                else:
                    state = sa_episode.get_extra_model_outputs(
                        key=Columns.STATE_OUT,
                        indices=-1,
                    )
                self.add_batch_item(
                    batch,
                    Columns.STATE_IN,
                    item_to_add=state,
                    single_agent_episode=sa_episode,
                )

        return batch
