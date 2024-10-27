from typing import Any, Dict, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import batch as batch_fn
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class BatchIndividualItems(ConnectorV2):
    """Batches individual data-items (in lists) into tensors (with batch dimension).

    Note: This is one of the default env-to-module or Learner ConnectorV2 pieces that
    are added automatically by RLlib into every env-to-module/Learner connector
    pipeline, unless `config.add_default_connectors_to_env_to_module_pipeline` or
    `config.add_default_connectors_to_learner_pipeline ` are set to
    False.

    The default env-to-module connector pipeline is:
    [
        [0 or more user defined ConnectorV2 pieces],
        AddObservationsFromEpisodesToBatch,
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
        AddStatesFromEpisodesToBatch,
        AgentToModuleMapping,  # only in multi-agent setups!
        BatchIndividualItems,
        NumpyToTensor,
    ]

    This ConnectorV2:
    - Operates only on the input `data`, NOT the incoming list of episode objects
    (ignored).
    - In the single-agent case, `data` must already be a dict, structured as follows by
    prior connector pieces of the same pipeline:
    [col0] -> {[(eps_id,)]: [list of individual batch items]}
    - In the multi-agent case, `data` must already be a dict, structured as follows by
    prior connector pieces of the same pipeline (in particular the
    `AgentToModuleMapping` piece):
    [module_id] -> [col0] -> [list of individual batch items]
    - Translates the above data under the different columns (e.g. "obs") into final
    (batched) structures. For the single-agent case, the output `data` looks like this:
    [col0] -> [possibly complex struct of batches (at the leafs)].
    For the multi-agent case, the output `data` looks like this:
    [module_id] -> [col0] -> [possibly complex struct of batches (at the leafs)].

    .. testcode::

        from ray.rllib.connectors.common import BatchIndividualItems
        from ray.rllib.utils.test_utils import check

        single_agent_batch = {
            "obs": {
                # Note that at this stage, next-obs is not part of the data anymore ..
                ("MA-EPS0",): [0, 1],
                ("MA-EPS1",): [2, 3],
            },
            "actions": {
                # .. so we have as many actions per episode as we have observations.
                ("MA-EPS0",): [4, 5],
                ("MA-EPS1",): [6, 7],
            },
        }

        # Create our (single-agent) connector piece.
        connector = BatchIndividualItems()

        # Call the connector (and thereby batch the individual items).
        output_batch = connector(
            rl_module=None,  # This particular connector works without an RLModule.
            batch=single_agent_batch,
            episodes=[],  # This particular connector works without a list of episodes.
            explore=True,
            shared_data={},
        )

        # `output_batch` should now be batched (episode IDs should have been removed
        # from the struct).
        check(
            output_batch,
            {"obs": [0, 1, 2, 3], "actions": [4, 5, 6, 7]},
        )
    """

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        multi_agent: bool = False,
        **kwargs,
    ):
        """Initializes a BatchIndividualItems instance.

        Args:
            multi_agent: Whether this is a connector operating on a multi-agent
                observation space mapping AgentIDs to individual agents' observations.
        """
        super().__init__(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            **kwargs,
        )
        self._multi_agent = multi_agent

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
        is_multi_rl_module = isinstance(rl_module, MultiRLModule)

        # Convert lists of individual items into properly batched data.
        for column, column_data in batch.copy().items():
            # Multi-agent case: This connector piece should only be used after(!)
            # the AgentToModuleMapping connector has already been applied, leading
            # to a batch structure of:
            # [module_id] -> [col0] -> [list of individual batch items]
            if is_multi_rl_module and column in rl_module:
                # Case, in which a column has already been properly batched before this
                # connector piece is called.
                if not self._multi_agent:
                    continue
                # If MA Off-Policy and independent sampling we need to overcome this
                # check.
                module_data = column_data
                for col, col_data in module_data.copy().items():
                    if isinstance(col_data, list) and col != Columns.INFOS:
                        module_data[col] = batch_fn(
                            col_data,
                            individual_items_already_have_batch_dim="auto",
                        )

            # Simple case: There is a list directly under `column`:
            # Batch the list.
            elif isinstance(column_data, list):
                batch[column] = batch_fn(
                    column_data,
                    individual_items_already_have_batch_dim="auto",
                )

            # Single-agent case: There is a dict under `column` mapping
            # `eps_id` to lists of items:
            # Concat all these lists, then batch.
            elif not self._multi_agent:
                # TODO: only really need this in non-Learner connector pipeline
                memorized_map_structure = []
                list_to_be_batched = []
                for (eps_id,) in column_data.keys():
                    for item in column_data[(eps_id,)]:
                        # Only record structure for OBS column.
                        if column == Columns.OBS:
                            memorized_map_structure.append(eps_id)
                        list_to_be_batched.append(item)
                # INFOS should not be batched (remain a list).
                batch[column] = (
                    list_to_be_batched
                    if column == Columns.INFOS
                    else batch_fn(
                        list_to_be_batched,
                        individual_items_already_have_batch_dim="auto",
                    )
                )
                if is_multi_rl_module:
                    if DEFAULT_MODULE_ID not in batch:
                        batch[DEFAULT_MODULE_ID] = {}
                    batch[DEFAULT_MODULE_ID][column] = batch.pop(column)

                # Only record structure for OBS column.
                if column == Columns.OBS:
                    shared_data["memorized_map_structure"] = memorized_map_structure
            # Multi-agent case: This should already be covered above.
            # This connector piece should only be used after(!)
            # the AgentToModuleMapping connector has already been applied, leading
            # to a batch structure of:
            # [module_id] -> [col0] -> [list of items]
            else:
                raise NotImplementedError

        return batch
