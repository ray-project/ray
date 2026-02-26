from typing import Any, Dict, List, Optional

import numpy as np
import tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import BatchedNdArray
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class AddColumnsFromEpisodesToTrainBatch(ConnectorV2):
    """Adds actions/rewards/terminateds/... to train batch. Excluding the infos column.

    Note: This is one of the default Learner ConnectorV2 pieces that are added
    automatically by RLlib into every Learner connector pipeline, unless
    `config.add_default_connectors_to_learner_pipeline` is set to False.

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

    Does NOT add observations or infos to train batch.
    Observations should have already been added by another ConnectorV2 piece:
    `AddObservationsToTrainBatch` in the same pipeline.
    Infos can be added manually by the user through setting this in the config:

    .. testcode::
        :skipif: True

        from ray.rllib.connectors.learner import AddInfosFromEpisodesToTrainBatch
        config.training(
            learner_connector=lambda obs_sp, act_sp: AddInfosFromEpisodesToTrainBatch()
        )`

    If provided with `episodes` data, this connector piece makes sure that the final
    train batch going into the RLModule for updating (`forward_train()` call) contains
    at the minimum:
    - Observations: From all episodes under the Columns.OBS key.
    - Actions, rewards, terminal/truncation flags: From all episodes under the
    respective keys.
    - All data inside the episodes' `extra_model_outs` property, e.g. action logp and
    action probs under the respective keys.
    - Internal states: These will NOT be added to the batch by this connector piece
    as this functionality is handled by a different default connector piece:
    `AddStatesFromEpisodesToBatch`.

    If the user wants to customize their own data under the given keys (e.g. obs,
    actions, ...), they can extract from the episodes or recompute from `data`
    their own data and store it in `data` under those keys. In this case, the default
    connector will not change the data under these keys and simply act as a
    pass-through.


    We expect that `episodes` are numpy'ized.
    In standard training flows, episodes have already been numpy'ized before they enter this connector via EnvRunners, ReplayBuffers or offline sampling.
    """

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Optional[Dict[str, Any]],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:

        # Determine which standard columns still need to be filled (skip any that a
        # custom upstream connector has already populated).
        need_actions = Columns.ACTIONS not in batch
        need_rewards = Columns.REWARDS not in batch
        need_terminateds = Columns.TERMINATEDS not in batch
        need_truncateds = Columns.TRUNCATEDS not in batch

        # Extra model outputs (except for STATE_OUT, which will be handled by another
        # default connector piece). Also, like with all the fields above, skip
        # those that the user already seemed to have populated via custom connector
        # pieces.
        skip_columns = set(batch.keys()) | {Columns.STATE_IN, Columns.STATE_OUT}

        # Single pass over all episodes: compute len(sa_episode) and sub_key once and
        # reuse them for every column.
        for sa_episode in self.single_agent_episode_iterator(
            episodes,
            agents_that_stepped_only=False,
        ):
            n = len(sa_episode)

            # For single-agent episodes (agent_id=None), sub_key is always (id_,).
            # We can use a fast path to extend the batch.
            # For multi-agent episodes, we need to use add_n_batch_items path.
            if sa_episode.agent_id is None:
                # Fast path: inline the dict operations, skipping the function call
                # overhead of add_n_batch_items / add_batch_item and the repeated
                # sub_key computation those would do per column.
                sub_key = (sa_episode.id_,)

                if need_actions:
                    # Actions may be a composite action space, so we need to map the structure.
                    data = sa_episode.get_actions(slice(0, n))
                    # Check if the we have to call tree.map_structure to convert the data to a BatchedNdArray.
                    if isinstance(data, np.ndarray):
                        data = data.view(BatchedNdArray)
                    else:
                        data = tree.map_structure(BatchedNdArray, data)
                    col = batch.get(Columns.ACTIONS)
                    if col is None:
                        batch[Columns.ACTIONS] = {sub_key: [data]}
                    else:
                        col[sub_key].append(data)

                if need_rewards:
                    data = sa_episode.get_rewards(slice(0, n)).view(BatchedNdArray)
                    col = batch.get(Columns.REWARDS)
                    if col is None:
                        batch[Columns.REWARDS] = {sub_key: [data]}
                    else:
                        col[sub_key].append(data)

                if need_terminateds:
                    terminateds = np.zeros(n, dtype=np.bool_)
                    if n > 0:
                        terminateds[-1] = sa_episode.is_terminated
                    data = terminateds.view(BatchedNdArray)
                    col = batch.get(Columns.TERMINATEDS)
                    if col is None:
                        batch[Columns.TERMINATEDS] = {sub_key: [data]}
                    else:
                        col[sub_key].append(data)

                if need_truncateds:
                    truncateds = np.zeros(n, dtype=np.bool_)
                    if n > 0:
                        truncateds[-1] = sa_episode.is_truncated
                    data = truncateds.view(BatchedNdArray)
                    col = batch.get(Columns.TRUNCATEDS)
                    if col is None:
                        batch[Columns.TRUNCATEDS] = {sub_key: [data]}
                    else:
                        col[sub_key].append(data)

                for column in sa_episode.extra_model_outputs.keys():
                    if column not in skip_columns:
                        # Extra model outputs may be a composite space, so we need to map the structure.
                        data = tree.map_structure(
                            BatchedNdArray,
                            sa_episode.get_extra_model_outputs(
                                key=column, indices=slice(0, n)
                            ),
                        )
                        col = batch.get(column)
                        if col is None:
                            batch[column] = {sub_key: [data]}
                        else:
                            col[sub_key].append(data)

            else:
                # General (multi-agent) path: use the standard API.
                if need_actions:
                    self.add_n_batch_items(
                        batch,
                        Columns.ACTIONS,
                        items_to_add=sa_episode.get_actions(slice(0, n)),
                        num_items=n,
                        single_agent_episode=sa_episode,
                    )
                if need_rewards:
                    self.add_n_batch_items(
                        batch,
                        Columns.REWARDS,
                        items_to_add=sa_episode.get_rewards(slice(0, n)),
                        num_items=n,
                        single_agent_episode=sa_episode,
                    )
                if need_terminateds:
                    terminateds = np.zeros(n, dtype=np.bool_)
                    if n > 0:
                        terminateds[-1] = sa_episode.is_terminated
                    self.add_n_batch_items(
                        batch,
                        Columns.TERMINATEDS,
                        items_to_add=terminateds,
                        num_items=n,
                        single_agent_episode=sa_episode,
                    )
                if need_truncateds:
                    truncateds = np.zeros(n, dtype=np.bool_)
                    if n > 0:
                        truncateds[-1] = sa_episode.is_truncated
                    self.add_n_batch_items(
                        batch,
                        Columns.TRUNCATEDS,
                        items_to_add=truncateds,
                        num_items=n,
                        single_agent_episode=sa_episode,
                    )
                for column in sa_episode.extra_model_outputs.keys():
                    if column not in skip_columns:
                        self.add_n_batch_items(
                            batch,
                            column,
                            items_to_add=sa_episode.get_extra_model_outputs(
                                key=column, indices=slice(0, n)
                            ),
                            num_items=n,
                            single_agent_episode=sa_episode,
                        )

        return batch
