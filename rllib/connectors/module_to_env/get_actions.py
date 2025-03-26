from typing import Any, Dict, List, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class GetActions(ConnectorV2):
    """Connector piece sampling actions from ACTION_DIST_INPUTS from an RLModule.

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

    If necessary, this connector samples actions, given action dist. inputs and a
    dist. class.
    The connector will only sample from the action distribution, if the
    Columns.ACTIONS key cannot be found in `data`. Otherwise, it'll behave
    as pass-through. If Columns.ACTIONS is NOT present in `data`, but
    Columns.ACTION_DIST_INPUTS is, this connector will create a new action
    distribution using the given RLModule and sample from its distribution class
    (deterministically, if we are not exploring, stochastically, if we are).
    """

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
        is_multi_agent = isinstance(episodes[0], MultiAgentEpisode)

        if is_multi_agent:
            for module_id, module_data in batch.copy().items():
                self._get_actions(module_data, rl_module[module_id], explore)
        else:
            self._get_actions(batch, rl_module, explore)

        return batch

    def _get_actions(self, batch, sa_rl_module, explore):
        # Action have already been sampled -> Early out.
        if Columns.ACTIONS in batch:
            return

        # ACTION_DIST_INPUTS field returned by `forward_exploration|inference()` ->
        # Create a new action distribution object.
        if Columns.ACTION_DIST_INPUTS in batch:
            if explore:
                action_dist_class = sa_rl_module.get_exploration_action_dist_cls()
            else:
                action_dist_class = sa_rl_module.get_inference_action_dist_cls()
            action_dist = action_dist_class.from_logits(
                batch[Columns.ACTION_DIST_INPUTS],
            )
            if not explore:
                action_dist = action_dist.to_deterministic()

            # Sample actions from the distribution.
            actions = action_dist.sample()
            batch[Columns.ACTIONS] = actions

            # For convenience and if possible, compute action logp from distribution
            # and add to output.
            if Columns.ACTION_LOGP not in batch:
                batch[Columns.ACTION_LOGP] = action_dist.logp(actions)
