from typing import Any, List, Optional

import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType


class RemoveSingleTsTimeRankFromBatch(ConnectorV2):
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
