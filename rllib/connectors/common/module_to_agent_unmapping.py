from collections import defaultdict
from typing import Any, List, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import batch, unbatch
from ray.rllib.utils.typing import EpisodeType


class ModuleToAgentUnmapping(ConnectorV2):
    """TODO"""

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
        # This Connector should only be used in a multi-agent setting.
        assert isinstance(episodes[0], MultiAgentEpisode)

        agent_data = defaultdict(dict)
        for module_id, module_data in data.items():
            for column, values_dict in module_data.items():
                #if column not in agent_data:
                #    agent_data[column] = [{} for _ in range(len(episodes))]
                #individual_items = unbatch(values_batch)
                #assert len(individual_items) == len(memorized_map_structure[module_id])
                agent_data[column].update(values_dict)
                
                #for individual_item, (eps_id, agent_id) in zip(
                #    individual_items, memorized_map_structure[module_id]
                #):
                #for i, val in enumerate():
                    ## TODO (sven): If one agent is terminated, we should NOT perform
                    ##  another forward pass on its (obs) data anymore, which we
                    ##  currently do in case we are using the WriteObservationsToEpisode
                    ##  connector piece, due to the fact that this piece requires even
                    ##  the terminal obs to be processed inside the batch. This if block
                    ##  here is a temporary fix for this issue.
                    #if episodes[eps_idx].agent_episodes[agent_id].is_done:
                    #    continue
                    #key = (eps_id, agent_id, module_id)
                    #if key not in agent_data[column]:
                    #    agent_data[column][key] = []
                    #agent_data[column][key].append(individual_item)

        return dict(agent_data)
