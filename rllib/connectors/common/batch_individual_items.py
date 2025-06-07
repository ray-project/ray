from typing import Any, Dict, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2, ConnectorV2BatchFormats
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
        AddTimeDimToBatchAndZeroPad,
        AddStatesFromEpisodesToBatch,
        RemapModuleToColumns,  # only in single-agent setups!
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
        RemapModuleToColumns,  # only in single-agent setups!
        AgentToModuleMapping,  # only in multi-agent setups!
        BatchIndividualItems,
        NumpyToTensor,
    ]

    This ConnectorV2:
    - Operates only on the input `batch`, NOT the incoming list of episode objects
    (ignored).
    - In the single-agent case, `batch` must already be a dict, structured as follows by
    prior connector pieces of the same pipeline:
    [col0] -> {[(eps_id,)]: [list of individual batch items]}
    - In the multi-agent case, `batch` must already be a dict, structured as follows by
    prior connector pieces of the same pipeline (in particular the
    `AgentToModuleMapping` piece):
    [module_id] -> [col0] -> [list of individual batch items]
    - Translates the above data under the different columns (e.g. "obs") into final
    (batched) structures. For the single-agent case, the output `batch` looks like this:
    [col0] -> [possibly complex struct of batches (at the leafs)].
    For the multi-agent case, the output `batch` looks like this:
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

    # Incoming batches have the format:
    # [moduleID] -> [column name] -> [.. individual items]
    # For more details on the various possible batch formats, see the
    # `ray.rllib.connectors.connector_v2.ConnectorV2BatchFormats` Enum.
    INPUT_BATCH_FORMAT = (
        ConnectorV2BatchFormats.BATCH_FORMAT_MODULE_TO_COLUMN_TO_INDIVIDUAL_ITEMS
    )
    # Returned batches have the format:
    # [moduleID] -> [column name] -> [tensors]
    # For more details on the various possible batch formats, see the
    # `ray.rllib.connectors.connector_v2.ConnectorV2BatchFormats` Enum.
    OUTPUT_BATCH_FORMAT = (
        ConnectorV2BatchFormats.BATCH_FORMAT_MODULE_TO_COLUMN_TO_BATCHED
    )

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
        # Convert lists of individual items into properly batched data.
        for module_id, module_data in batch.copy().items():
            for col, individual_items in module_data.copy().items():
                # Don't batch info dicts, ever. Often, they have inhomogenous structures
                # across the episode/batch.
                if col != Columns.INFOS and isinstance(individual_items, list):
                    module_data[col] = batch_fn(
                        individual_items,
                        individual_items_already_have_batch_dim="auto",
                    )
        return batch
