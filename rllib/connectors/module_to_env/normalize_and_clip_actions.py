from typing import Any, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import (
    clip_action,
    get_base_struct_from_space,
    unsquash_action,
)
from ray.rllib.utils.typing import EpisodeType


class NormalizeAndClipActions(ConnectorV2):

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        normalize_actions: bool,
        clip_actions: bool,
        **kwargs,
    ):
        """Initializes a DefaultModuleToEnv (connector piece) instance.

        Args:
            normalize_actions: If True, actions coming from the RLModule's distribution
                (or are directly computed by the RLModule w/o sampling) will
                be assumed 0.0 centered with a small stddev (only affecting Box
                components) and thus be unsquashed (and clipped, just in case) to the
                bounds of the env's action space. For example, if the action space of
                the environment is `Box(-2.0, -0.5, (1,))`, the model outputs
                mean and stddev as 0.1 and exp(0.2), and we sample an action of 0.9
                from the resulting distribution, then this 0.9 will be unsquashed into
                the [-2.0 -0.5] interval. If - after unsquashing - the action still
                breaches the action space, it will simply be clipped.
            clip_actions: If True, actions coming from the RLModule's distribution
                (or are directly computed by the RLModule w/o sampling) will be clipped
                such that they fit into the env's action space's bounds.
                For example, if the action space of the environment is
                `Box(-0.5, 0.5, (1,))`, the model outputs
                mean and stddev as 0.1 and exp(0.2), and we sample an action of 0.9
                from the resulting distribution, then this 0.9 will be clipped to 0.5
                to fit into the [-0.5 0.5] interval.
        """
        super().__init__(input_observation_space, input_action_space, **kwargs)

        self._action_space_struct = None
        self.normalize_actions = normalize_actions
        self.clip_actions = clip_actions

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
        """Based on settings, will normalize (unsquash) and/or clip computed actions.

        This is such that the final actions (to be sent to the env) match the
        environment's action space and thus don't lead to an error.
        """
        # TODO: Get rid of this once we merge the PR for `recompute_observation_space`
        #  ...
        if self._action_space_struct is None:
            self._action_space_struct = get_base_struct_from_space(self.action_space)        
        
        def _unsquash_or_clip(action, env_vector_idx, agent_id, module_id):
            if self.normalize_actions:
                if agent_id is not None and module_id is not None:
                    return unsquash_action(
                        action,
                        {
                            k: v
                            for k, v in self._action_space_struct.items()
                            if k in action
                        },
                    )
                else:
                    return unsquash_action(
                        action, self._action_space_struct
                    )
            elif self.clip_actions:
                if agent_id is not None and module_id is not None:
                    return clip_action(
                        action,
                        {
                            k: v
                            for k, v in self._action_space_struct.items()
                            if k in action
                        },
                    )
                else:
                    return clip_action(
                       data[SampleBatch.ACTIONS], self._action_space_struct
                    )
            return action

        # Create a duplicate "actions" entry, just to be sent to the ENV.
        # The original actions (non-normalized, non-clipped) will be the ones
        # stored in the Episode (and then possibly used for learning).
        data[SampleBatch.ACTIONS_FOR_ENV] = data[SampleBatch.ACTIONS]

        # Normalize or clip actions.
        self.foreach_batch_item_change_in_place(
            batch=data,
            column=SampleBatch.ACTIONS_FOR_ENV,
            func=_unsquash_or_clip,
        )

        return data
