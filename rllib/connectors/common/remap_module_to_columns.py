from typing import Any, Dict, List, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2, ConnectorV2BatchFormats
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class RemapModuleToColumns(ConnectorV2):
    """Remaps [col]->[(epsID,)]->[items ..] to [DEFAULT_MODULE_ID]->[col]->[items ..].

    Note: In the single-agent case, this is one of the default env-to-module or
    Learner ConnectorV2 pieces that are added automatically by RLlib into every
    env-to-module/Learner connector pipeline, unless
    `config.add_default_connectors_to_env_to_module_pipeline` or
    `config.add_default_connectors_to_learner_pipeline ` are set to False.

    The default env-to-module connector pipeline is:
    [
        [0 or more user defined ConnectorV2 pieces],
        AddObservationsFromEpisodesToBatch,
        AddTimeDimToBatchAndZeroPad,
        AddStatesFromEpisodesToBatch,
        RemapModuleToColumns,  # only in single-agent setups!
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
        BatchIndividualItems,
        NumpyToTensor,
    ]

    This ConnectorV2:
    - Operates only on the input `batch`, NOT the incoming list of episode objects
    (ignored).
    - `batch` must already be a dict, structured as follows by prior connector pieces of
    the same pipeline:
    [col0] -> {[(eps_id,)]: [list of individual batch items]}
    - Remaps the above structure to a new one mapping DEFAULT_MODULE_ID (single-agent)
    to columns to individual batch items. The output `batch` looks like this:
    [DEFAULT_MODULE_ID] -> [col0] -> [list of individual batch items].

    .. testcode::

        from ray.rllib.connectors.common import RemapModuleToColumns
        from ray.rllib.utils.test_utils import check

        single_agent_batch = {
            "obs": {
                # Note that at this stage, next-obs is not part of the data anymore ..
                ("EPS0",): [0, 1],
                ("EPS1",): [2, 3],
            },
            "actions": {
                # .. so we have as many actions per episode as we have observations.
                ("EPS0",): [4, 5],
                ("EPS1",): [6, 7],
            },
        }

        # Create our (single-agent) connector piece.
        connector = RemapModuleToColumns()

        # Call the connector (and thereby re-map).
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
            {
                "default_policy": {"obs": [0, 1, 2, 3], "actions": [4, 5, 6, 7]},
            },
        )
    """

    # Incoming batches have the format:
    # [column name] -> [(episodeID,)] -> [.. individual items]
    # For more details on the various possible batch formats, see the
    # `ray.rllib.connectors.connector_v2.ConnectorV2BatchFormats` Enum.
    INPUT_BATCH_FORMAT = (
        ConnectorV2BatchFormats.BATCH_FORMAT_COLUMN_TO_EPISODE_TO_INDIVIDUAL_ITEMS
    )
    # Returned batches have the format:
    # [DEFAULT_MODULE_ID] -> [column name] -> [.. individual items]
    # For more details on the various possible batch formats, see the
    # `ray.rllib.connectors.connector_v2.ConnectorV2BatchFormats` Enum.
    OUTPUT_BATCH_FORMAT = (
        ConnectorV2BatchFormats.BATCH_FORMAT_MODULE_TO_COLUMN_TO_INDIVIDUAL_ITEMS
    )

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
        # Remap [col]->(eps_id,)->[data ..] to DEFAULT_MODULE_ID->[col]->[data ..].
        module_data = {DEFAULT_MODULE_ID: {}}
        memorized_map_structure = []

        for column, column_data in batch.copy().items():
            list_to_be_remapped = []
            for (eps_id,), individual_items in column_data.items():
                list_to_be_remapped.extend(individual_items)
                if column == Columns.OBS:
                    memorized_map_structure.append(eps_id)
            module_data[DEFAULT_MODULE_ID][column] = list_to_be_remapped
            if shared_data is not None and column == Columns.OBS:
                shared_data["memorized_map_structure"] = memorized_map_structure

        return module_data
