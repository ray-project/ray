import copy
from typing import Any, Dict, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import (
    clip_action,
    get_base_struct_from_space,
    unsquash_action,
)
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class NormalizeAndClipActions(ConnectorV2):
    """Normalizes or clips actions in the input data (coming from the RLModule).

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

    This ConnectorV2:
    - Deep copies the Columns.ACTIONS in the incoming `data` into a new column:
    Columns.ACTIONS_FOR_ENV.
    - Loops through the Columns.ACTIONS in the incoming `data` and normalizes or clips
    these depending on the c'tor settings in `config.normalize_actions` and
    `config.clip_actions`.
    - Only applies to envs with Box action spaces.

    Normalizing is the process of mapping NN-outputs (which are usually small
    numbers, e.g. between -1.0 and 1.0) to the bounds defined by the action-space.
    Normalizing helps the NN to learn faster in environments with large ranges between
    `low` and `high` bounds or skewed action bounds (e.g. Box(-3000.0, 1.0, ...)).

    Clipping clips the actions computed by the NN (and sampled from a distribution)
    between the bounds defined by the action-space. Note that clipping is only performed
    if `normalize_actions` is False.
    """

    @override(ConnectorV2)
    def recompute_output_action_space(
        self,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
    ) -> gym.Space:
        self._action_space_struct = get_base_struct_from_space(input_action_space)
        return input_action_space

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
        self._action_space_struct = None

        super().__init__(input_observation_space, input_action_space, **kwargs)

        self.normalize_actions = normalize_actions
        self.clip_actions = clip_actions

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
        """Based on settings, will normalize (unsquash) and/or clip computed actions.

        This is such that the final actions (to be sent to the env) match the
        environment's action space and thus don't lead to an error.
        """

        def _unsquash_or_clip(action_for_env, env_id, agent_id, module_id):
            if agent_id is not None:
                struct = self._action_space_struct[agent_id]
            else:
                struct = self._action_space_struct

            if self.normalize_actions:
                return unsquash_action(action_for_env, struct)
            else:
                return clip_action(action_for_env, struct)

        # Normalize or clip this new actions_for_env column, leaving the originally
        # computed/sampled actions intact.
        if self.normalize_actions or self.clip_actions:
            # Copy actions into separate column, just to go to the env.
            batch[Columns.ACTIONS_FOR_ENV] = copy.deepcopy(batch[Columns.ACTIONS])
            self.foreach_batch_item_change_in_place(
                batch=batch,
                column=Columns.ACTIONS_FOR_ENV,
                func=_unsquash_or_clip,
            )

        return batch
