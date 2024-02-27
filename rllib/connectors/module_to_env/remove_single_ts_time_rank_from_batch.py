from typing import Any, List, Optional

import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.models.base import STATE_OUT
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch, try_import_tf
from ray.rllib.utils.typing import EpisodeType

#torch, _ = try_import_torch()
#_, tf, _ = try_import_tf()


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

        # Note that we are still operating on tensors.
        #framework = rl_module.framework
        #if framework == "torch":
        #    framework = torch
        #elif framework == "tf2":
        #    framework = tf
        #else:
        #    framework = np

        data = tree.map_structure_with_path(
            lambda p, s: s if STATE_OUT in p else np.squeeze(s, axis=0),
            data,
        )

        return data

        #if not is_multi_agent:
        #    data = {DEFAULT_POLICY_ID: data}

        #for module_id, module_data in data.items():
        #    state = module_data.pop(STATE_OUT, None)
        #    # TODO: If we were NOT operating on unbatched data (but we are right now),
        #    # this would have to be changed to axis=1
        #    module_data = tree.map_structure(
        #        lambda s: np.squeeze(s, axis=0), module_data
        #    )
        #    if state:
        #        module_data[STATE_OUT] = state

        #    if not is_multi_agent:
        #        return module_data
        #    data[module_id] = module_data

        #return data
