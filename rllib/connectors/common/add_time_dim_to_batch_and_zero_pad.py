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
from ray.rllib.utils.postprocessing.zero_padding import (
    create_mask_and_seq_lens,
    split_and_zero_pad,
)
from ray.rllib.utils.spaces.space_utils import BatchedNdArray
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class AddTimeDimToBatchAndZeroPad(ConnectorV2):
    """Adds an extra time dim (axis=1) to all data currently in the batch.

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

    If the RLModule is stateful, an extra time dim at axis=1 is added to all data in the
    batch.

    Also, all data (observations, rewards, etc.. if applicable) will be properly
    reshaped into (B, T=max_seq_len (learner) or 1 (env-to-module), ...) and will be
    zero-padded, if necessary.

    This ConnectorV2:
    - Operates on a list of Episode objects.
    - Adds a time dim at axis=1 to all columns already in the batch.
    - In case of a learner connector pipeline, zero-pads the data according to the
    module's `self.model_config["max_seq_len"]` setting and reshapes all data to
    (B, T, ...). The connector also adds SEQ_LENS information and loss mask
    information to the batch based on the added zero-padding.
    - Does NOT alter any data in the given episodes.
    - Can be used in EnvToModule and Learner connector pipelines.

    .. testcode::

        from ray.rllib.connectors.common import AddTimeDimToBatchAndZeroPad
        from ray.rllib.core.columns import Columns
        from ray.rllib.env.single_agent_episode import SingleAgentEpisode
        from ray.rllib.utils.test_utils import check


        # Create a simple dummy class, pretending to be an RLModule with
        # `get_initial_state`, `is_stateful` and `model_config` property defined:
        class MyStateModule:
            # dummy config
            model_config = {"max_seq_len": 3}

            def is_stateful(self):
                return True

            def get_initial_state(self):
                return 0.0


        # Create an already reset episode. Expect the connector to add a time-dim to the
        # reset observation.
        episode = SingleAgentEpisode(observations=[0])

        rl_module = MyStateModule()

        # Create an instance of this class (as an env-to-module connector).
        connector = AddTimeDimToBatchAndZeroPad(as_learner_connector=False)

        # Call the connector.
        output_batch = connector(
            rl_module=rl_module,
            batch={Columns.OBS: [0]},
            episodes=[episode],
            shared_data={},
        )
        # The output data's OBS key should now be reshaped to (B, T)
        check(output_batch[Columns.OBS], [[0]])

        # Create a SingleAgentEpisodes containing 5 observations,
        # 4 actions and 4 rewards.
        episode = SingleAgentEpisode(
            observations=[0, 1, 2, 3, 4],
            actions=[1, 2, 3, 4],
            rewards=[1.0, 2.0, 3.0, 4.0],
            len_lookback_buffer=0,
        )

        # Call the connector.
        output_batch = connector(
            rl_module=rl_module,
            batch={Columns.OBS: [4]},
            episodes=[episode],
            shared_data={},
        )
        # The output data's OBS, ACTIONS, and REWARDS keys should now all have a time
        # rank.
        check(
            # Expect the episode's last OBS.
            output_batch[Columns.OBS], [[4]],
        )

        # Create a new connector as a learner connector with a RNN seq len of 4 (for
        # testing purposes only). Passing the same data through this learner connector,
        # we expect the data to also be zero-padded.
        connector = AddTimeDimToBatchAndZeroPad(as_learner_connector=True)

        # Call the connector.
        output_batch = connector(
            rl_module=rl_module,
            batch={Columns.OBS: {(episode.id_,): [0, 1, 2, 3]}},
            episodes=[episode],
            shared_data={},
        )
        check(output_batch[Columns.OBS], {(episode.id_,): [[0, 1, 2], [3, 0, 0]]})
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

        # Make all inputs (other than STATE_IN) have an additional T-axis.
        # Since data has not been batched yet (we are still operating on lists in the
        # batch), we add this time axis as 0 (not 1). When we batch, the batch axis will
        # be 0 and the time axis will be 1.
        # Also, let module-to-env pipeline know that we had added a single timestep
        # time rank to the data (to remove it again).
        if not self._as_learner_connector:
            for column in batch.keys():
                self.foreach_batch_item_change_in_place(
                    batch=batch,
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
            for column, column_data in batch.copy().items():
                # Do not zero-pad INFOS column.
                if column == Columns.INFOS:
                    continue
                for key, item_list in column_data.items():
                    # Multi-agent case AND RLModule is not stateful -> Do not zero-pad
                    # for this model.
                    assert isinstance(key, tuple)
                    mid = None
                    if len(key) == 3:
                        eps_id, aid, mid = key
                        if not rl_module[mid].is_stateful():
                            continue
                    column_data[key] = split_and_zero_pad(
                        item_list,
                        max_seq_len=self._get_max_seq_len(rl_module, module_id=mid),
                    )
                    # TODO (sven): Remove this hint/hack once we are not relying on
                    #  SampleBatch anymore (which has to set its property
                    #  zero_padded=True when shuffling).
                    shared_data[
                        (
                            "_zero_padded_for_mid="
                            f"{mid if mid is not None else DEFAULT_MODULE_ID}"
                        )
                    ] = True

            for sa_episode in self.single_agent_episode_iterator(
                # If Learner connector, get all episodes (for train batch).
                # If EnvToModule, get only those ongoing episodes that just had their
                # agent step (b/c those are the ones we need to compute actions for next).
                episodes,
                agents_that_stepped_only=False,
            ):
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

                # Also, create the loss mask (b/c of our now possibly zero-padded data)
                # as well as the seq_lens array and add these to `data` as well.
                mask, seq_lens = create_mask_and_seq_lens(len(sa_episode), max_seq_len)
                self.add_n_batch_items(
                    batch=batch,
                    column=Columns.SEQ_LENS,
                    items_to_add=seq_lens,
                    num_items=len(seq_lens),
                    single_agent_episode=sa_episode,
                )
                if not shared_data.get("_added_loss_mask_for_valid_episode_ts"):
                    self.add_n_batch_items(
                        batch=batch,
                        column=Columns.LOSS_MASK,
                        items_to_add=mask,
                        num_items=len(mask),
                        single_agent_episode=sa_episode,
                    )

        return batch

    def _get_max_seq_len(self, rl_module, module_id=None):
        if not isinstance(rl_module, MultiRLModule):
            mod = rl_module
        elif module_id:
            mod = rl_module[module_id]
        else:
            mod = next(iter(rl_module.values()))
        if "max_seq_len" not in mod.model_config:
            raise ValueError(
                "You are using a stateful RLModule and are not providing a "
                "'max_seq_len' key inside your `model_config`. You can set this "
                "dict and/or override keys in it via `config.rl_module("
                "model_config={'max_seq_len': [some int]})`."
            )
        return mod.model_config["max_seq_len"]
