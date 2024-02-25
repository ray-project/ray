from typing import Any, List, Optional

import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.models.base import STATE_OUT
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import convert_to_tensor
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.spaces.space_utils import batch, unbatch
from ray.rllib.utils.typing import EpisodeType


class GetActions(ConnectorV2):

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

        # Action have already been sampled -> Early out.
        if SampleBatch.ACTIONS in data:
            return data

        # ACTION_DIST_INPUTS field returned by `forward_exploration|inference()` ->
        # Create a new action distribution object.
        action_dist = None
        sa_rl_module = rl_module        
        if SampleBatch.ACTION_DIST_INPUTS in data:
            #for key, dist_inputs in data[SampleBatch.ACTION_DIST_INPUTS].items():
            #    if len(key) == 3:
            #        (env_vector_idx, agent_id, module_id) = key
            #        sa_rl_module = rl_module[module_id]
            #    else:
            #        env_vector_idx = key[0]
            #        agent_id = module_id = None

            if explore:
                action_dist_class = sa_rl_module.get_exploration_action_dist_cls()
            else:
                action_dist_class = sa_rl_module.get_inference_action_dist_cls()
            action_dist = action_dist_class.from_logits(
                data[SampleBatch.ACTION_DIST_INPUTS],
            )
            # TODO (sven): Should this not already be taken care of by RLModule's
            #  `get_...action_dist_cls()` methods?
            if not explore:
                action_dist = action_dist.to_deterministic()

            # Sample actions from the distribution.
            actions = action_dist.sample()

            # For convenience and if possible, compute action logp from distribution
            # and add to output.
            if SampleBatch.ACTION_LOGP not in data:
                data[SampleBatch.ACTION_LOGP] = action_dist.logp(actions)

            data[SampleBatch.ACTIONS] = actions

        #if not is_multi_agent:
        #    return data[DEFAULT_POLICY_ID]
        return data
