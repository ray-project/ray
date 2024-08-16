from typing import Any, List, Optional

import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class RemoveSingleTsTimeRankFromBatch(ConnectorV2):
    """
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
        # If single ts time-rank had not been added, early out.
        if shared_data is None or not shared_data.get("_added_single_ts_time_rank"):
            return data

        data = tree.map_structure_with_path(
            lambda p, s: s if Columns.STATE_OUT in p else np.squeeze(s, axis=0),
            data,
        )

        return data
