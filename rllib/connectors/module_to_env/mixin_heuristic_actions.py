from collections import defaultdict
from typing import Any, Callable, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType, TensorType


class MixinHeuristicActions(ConnectorV2):
    """Connector mixing in heuristic action (logits) into the RLModule computed ones.

    This connector should be used for a) inference only or b) off-policy algorithms,
    which do NOT require the current RLModule to produce the actions sent to the env.

    - This connector should be placed into the module-to-env pipeline.
    - If the mixin weight is > 0.0, the connector will compute heuristic action logits
    using a provided (heuristic) function and mixin these heuristic logits into the
    RLModule computed ones.
    - The connector will rely on the RLlib default connector pieces in the module-to-env
    pipeline to create the action distribution object (whose class is defined by the
    RLModule), sample from this distribution (using the mixin logits), and push the
    sampled action(s) into the final batch sent to the env for stepping.
    """

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        compute_heuristic_actions: Callable[[Any], TensorType],
        mixin_weight: float = 0.5,
    ):
        """Initializes a MixinHeuristicActions instance.

        Args:
            compute_heuristic_actions: A callable taking a current observation and
                returning a tensor with action logits. This may be a one-hot tensor
                if the action choice is unanimous/greedy.
            mixin_weight: The weight from 0.0 to 1.0, with which to mixin the heuristic
                logits into the RLModule computed ones.
        """
        super().__init__(input_observation_space, input_action_space)
        self.compute_heuristic_actions = compute_heuristic_actions
        self.mixin_weight = mixin_weight

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        data: Any,
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        # Early out if no mixin required -> Just use RLModule logits as always.
        if self.mixin_weight == 0.0:
            return data

        action_dist_inputs = data.get(Columns.ACTION_DIST_INPUTS)
        if action_dist_inputs is None:
            raise ValueError(
                "`data` must already have a column named "
                f"{Columns.ACTION_DIST_INPUTS} in it for this connector to work!"
            )

        obs = defaultdict(list)
        for sa_episode in self.single_agent_episode_iterator(episodes):
             eps_id, agent_id, module_id = (
                 sa_episode.id_, sa_episode.agent_id, sa_episode.module_id
             )
             obs[
                 (eps_id,) + (() if agent_id is None else (agent_id, module_id))
             ].append(sa_episode.get_observations(-1))
        obs = dict(obs)

        def _wrapped_compute_heuristic_actions(
            obs_and_logits, eps_id, agent_id, module_id
        ):
            obs, rl_module_logits = obs_and_logits
            # Compute heuristic logits.
            heuristic_logits = self.compute_heuristic_actions(obs)
            # Mixin heuristics with RLModule computed action logits.
            new_logits = heuristic_logits * self.mixin_weight + rl_module_logits * (
                1.0 - self.mixin_weight
            )
            return obs, new_logits

        self.foreach_batch_item_change_in_place(
            batch=data | {Columns.OBS: obs},
            column=[Columns.OBS, Columns.ACTION_DIST_INPUTS],
            func=_wrapped_compute_heuristic_actions,
        )

        return data
