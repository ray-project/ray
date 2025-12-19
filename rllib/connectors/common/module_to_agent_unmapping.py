from collections import defaultdict
from typing import Any, Dict, List, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class ModuleToAgentUnmapping(ConnectorV2):
    """Performs flipping of `data` from ModuleID- to AgentID based mapping.

    Before mapping:
    data[module1] -> [col, e.g. ACTIONS]
    -> [dict mapping episode-identifying tuples to lists of data]
    data[module2] -> ...

    After mapping:
    data[ACTIONS]: [dict mapping episode-identifying tuples to lists of data]

    Note that episode-identifying tuples have the form of: (episode_id,) in the
    single-agent case and (ma_episode_id, agent_id, module_id) in the multi-agent
    case.
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
        # This Connector should only be used in a multi-agent setting.
        assert isinstance(episodes[0], MultiAgentEpisode)

        agent_data = defaultdict(dict)
        for module_id, module_data in batch.items():
            for column, values_dict in module_data.items():
                agent_data[column].update(values_dict)

        return dict(agent_data)
