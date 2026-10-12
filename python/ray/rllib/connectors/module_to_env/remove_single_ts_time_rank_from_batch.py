from typing import Any, Dict, List, Optional

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
        batch: Optional[Dict[str, Any]],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        # If single ts time-rank had not been added, early out.
        if shared_data is None or not shared_data.get("_added_single_ts_time_rank"):
            return batch

        def _remove_single_ts(item, eps_id, aid, mid):
            # Only remove time-rank for modules that are statefule (only for those, a
            # timerank has been added).
            if mid is None or rl_module[mid].is_stateful():
                return tree.map_structure(lambda s: np.squeeze(s, axis=0), item)
            return item

        for column, column_data in batch.copy().items():
            # Skip state_out (doesn't have a time rank).
            if column == Columns.STATE_OUT:
                continue
            self.foreach_batch_item_change_in_place(
                batch,
                column=column,
                func=_remove_single_ts,
            )

        return batch
