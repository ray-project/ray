from typing import Any, List, Optional

import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.models.base import STATE_OUT
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.spaces.space_utils import (
    clip_action,
    get_base_struct_from_space,
    unsquash_action,
)
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class DefaultModuleToEnv(ConnectorV2):
    """Default connector piece added by RLlib to the end of any module-to-env pipeline.

    If necessary, this connector samples actions, given action dist. inputs and a
    dist. class.
    The connector will only sample from the action distribution, if the
    SampleBatch.ACTIONS key cannot be found in `data`. Otherwise, it'll behave
    as pass through (noop). If SampleBatch.ACTIONS is not present, but
    SampleBatch.ACTION_DIST_INPUTS are, the connector will create a new action
    distribution using the RLModule in the connector context and sample from this
    distribution (deterministically, if we are not exploring, stochastically, if we
    are).

    input_type: INPUT_OUTPUT_TYPES.DICT_OF_MODULE_IDS_TO_DATA
        Operates per RLModule as it will have to pull the action distribution from each
        in order to sample actions if necessary. Searches for the ACTIONS and
        ACTION_DIST_INPUTS keys in a module's outputs and - should ACTIONS not be
        found - sample actions from the module's action distribution.
    output_type: INPUT_OUTPUT_TYPES.DICT_OF_MODULE_IDS_TO_DATA (same as input: data in,
        data out, however, data
        out might contain an additional ACTIONS key if it was not previously present
        in the input).
    """

    def __init__(
        self,
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
        super().__init__(**kwargs)

        self._action_space_struct = get_base_struct_from_space(self.action_space)
        self.normalize_actions = normalize_actions
        self.clip_actions = clip_actions

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

        is_multi_agent = isinstance(episodes[0], MultiAgentEpisode)

        # If our RLModule is stateful, remove the T=1 axis from all model outputs
        # (except the state outs, which never have this extra time axis).
        if rl_module.is_stateful():
            data = self._remove_time_rank_from_data(data, rl_module, is_multi_agent)

        data = self._get_actions(data, rl_module, explore, is_multi_agent)

        # Check, whether module-to-agent mapping needs to be performed.
        if is_multi_agent:
            all_module_ids = set(rl_module.keys())
            if all(module_id in all_module_ids for module_id in data.keys()):
                data = self._perform_module_to_agent_unmapping(
                    data, episodes, shared_data
                )

        # Convert everything into numpy.
        data = convert_to_numpy(data)

        # Process actions according to Env's action space bounds, if necessary.
        self._normalize_clip_actions(data, is_multi_agent)

        return data

    def _remove_time_rank_from_data(self, data, rl_module, is_multi_agent):
        if not is_multi_agent:
            data = {DEFAULT_POLICY_ID: data}

        for module_id, module_data in data.items():
            state = data.pop(STATE_OUT, None)
            data = tree.map_structure(lambda s: np.squeeze(s, axis=1), data)
            if state:
                data[STATE_OUT] = state

        if not is_multi_agent:
            return data[DEFAULT_POLICY_ID]
        return data

    def _get_actions(self, data, rl_module, explore, is_multi_agent):
        if not is_multi_agent:
            data = {DEFAULT_POLICY_ID: data}
            sa_rl_module = rl_module

        for module_id, module_data in data.items():
            if is_multi_agent:
                sa_rl_module = rl_module[module_id]

            # ACTION_DIST_INPUTS field returned by `forward_exploration|inference()` ->
            # Create a new action distribution object.
            action_dist = None
            if SampleBatch.ACTION_DIST_INPUTS in module_data:
                if explore:
                    action_dist_class = sa_rl_module.get_exploration_action_dist_cls()
                else:
                    action_dist_class = sa_rl_module.get_inference_action_dist_cls()
                action_dist = action_dist_class.from_logits(
                    module_data[SampleBatch.ACTION_DIST_INPUTS]
                )

                # TODO (sven): Should this not already be taken care of by RLModule's
                #  `get_...action_dist_cls()` methods?
                if not explore:
                    action_dist = action_dist.to_deterministic()

            # If `forward_...()` returned actions, use them here as-is.
            if SampleBatch.ACTIONS in module_data:
                actions = module_data[SampleBatch.ACTIONS]
            # Otherwise, sample actions from the distribution.
            else:
                if action_dist is None:
                    raise KeyError(
                        "Your RLModule's `forward_[exploration|inference]()` methods "
                        f"must return a dict with either the '{SampleBatch.ACTIONS}' "
                        f"key or the '{SampleBatch.ACTION_DIST_INPUTS}' key in it "
                        "(or both)!"
                    )
                actions = module_data[SampleBatch.ACTIONS] = action_dist.sample()

            # For convenience and if possible, compute action logp from distribution
            # and add to output.
            if action_dist is not None and SampleBatch.ACTION_LOGP not in module_data:
                module_data[SampleBatch.ACTION_LOGP] = action_dist.logp(actions)

        if not is_multi_agent:
            return data[DEFAULT_POLICY_ID]
        return data

    def _perform_module_to_agent_unmapping(self, data, episodes, shared_data):
        """Performs flipping of `data` from ModuleID- to AgentID based mapping.

        Before mapping:
        data[module1]: ACTIONS: ...
        data[module2]: ACTIONS: ...

        #
        data[ACTIONS]: [{}] <- list index == episode index from shared_data

        # Flip column (e.g. OBS) with module IDs (no cost/extra memory):
        data[OBS]: "ag1": push into list -> [..., ...]

        # Mapping (no cost/extra memory):
        # ... then perform batching: stack([each module's items in list], axis=0)
        data[OBS]: "module1": [...] <- already batched data

        data[OBS]: "ag1" ... "ag2" ...
        """
        module_to_episode_agents_mapping = shared_data[
            "module_to_episode_agents_mapping"
        ]
        agent_data = {}
        for module_id, module_data in data.items():
            for column, values_batch in module_data.items():
                if column not in agent_data:
                    agent_data[column] = [{}] * len(episodes)
                for i, val in enumerate(values_batch):
                    eps_idx, agent_id = module_to_episode_agents_mapping[module_id][i]
                    agent_data[column][eps_idx][agent_id] = val

        return agent_data

    def _normalize_clip_actions(self, data, is_multi_agent):
        if is_multi_agent:
            if self.normalize_actions:
                data[SampleBatch.ACTIONS] = [
                    unsquash_action(
                        a,
                        {k: v for k, v in self._action_space_struct.items() if k in a},
                    )
                    for a in data[SampleBatch.ACTIONS]
                ]
            elif self.clip_actions:
                data[SampleBatch.ACTIONS] = [
                    clip_action(
                        a,
                        {k: v for k, v in self._action_space_struct.items() if k in a},
                    )
                    for a in data[SampleBatch.ACTIONS]
                ]
        else:
            if self.normalize_actions:
                data[SampleBatch.ACTIONS] = unsquash_action(
                    data[SampleBatch.ACTIONS], self._action_space_struct
                )
            elif self.clip_actions:
                data[SampleBatch.ACTIONS] = clip_action(
                    data[SampleBatch.ACTIONS], self._action_space_struct
                )
