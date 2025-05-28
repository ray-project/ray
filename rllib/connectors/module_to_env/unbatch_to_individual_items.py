from typing import Any, Dict, List, Optional

import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2, ConnectorV2BatchFormats
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import unbatch as unbatch_fn
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class UnBatchToIndividualItems(ConnectorV2):
    """Unbatches the given `batch` back into the individual-batch-items format.

    Note: This is one of the default module-to-env ConnectorV2 pieces that
    are added automatically by RLlib into every module-to-env connector pipeline,
    unless `config.add_default_connectors_to_module_to_env_pipeline` is set to
    False.

    The default module-to-env connector pipeline is:
    [
        GetActions,
        TensorToNumpy,
        UnBatchToIndividualItems,
        ModuleToAgentUnmapping,  # only in multi-agent setups!
        RemoveSingleTsTimeRankFromBatch,

        [0 or more user defined ConnectorV2 pieces],

        NormalizeAndClipActions,
        ListifyDataForVectorEnv,
    ]
    """

    # Incoming batches have the format:
    # [moduleID] -> [column name] -> [tensors]
    # For more details on the various possible batch formats, see the
    # `ray.rllib.connectors.connector_v2.ConnectorV2BatchFormats` Enum.
    INPUT_BATCH_FORMAT = (
        ConnectorV2BatchFormats.BATCH_FORMAT_MODULE_TO_COLUMN_TO_BATCHED
    )
    # Returned batches have the format:
    # [moduleID] -> [column name] -> [.. individual items]
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
        # Just unbatch.
        return tree.map_structure(lambda s: unbatch_fn(s), batch)
