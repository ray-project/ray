from collections import defaultdict
from typing import Any, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import unbatch
from ray.rllib.utils.typing import EpisodeType


class AgentToModuleMapping(ConnectorV2):
    """TODO"""

    @override(ConnectorV2)
    def recompute_observation_space_from_input_spaces(self):
        return self._map_space_if_necessary(self.input_observation_space)

    @override(ConnectorV2)
    def recompute_action_space_from_input_spaces(self):
        return self._map_space_if_necessary(self.input_action_space)

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        modules,
        agent_to_module_mapping_fn,
    ):
        super().__init__(input_observation_space, input_action_space)

        self._modules = modules
        self._agent_to_module_mapping_fn = agent_to_module_mapping_fn

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
        """Performs flipping of `data` from AgentID- to ModuleID based mapping.

        Note that before the mapping, the batch is expected to have the following
        structure:
        [col0]:
            (eps_id0, ag0, mod0): [list of individual batch items]
            (eps_id0, ag1, mod2): [list of individual batch items]
            (eps_id1, ag0, mod1): [list of individual batch items]
        [col1]:
            etc..

        The target structure of the above batch would be:
        [mod0]:
            [col0]: [batched data -> batch_size_B will be the number of all items in the
                input data under col0 that have mod0 as their ModuleID]
            [col1]: [batched data]
        [mod1]:
            [col0]: etc.

        Mapping happens in the following stages:

        1) Under each column name, sort keys first by EpisodeID, then AgentID.
        2) Add ModuleID keys under each column name (no cost/extra memory) and map these
        new keys to empty lists.
        [col0] -> [mod0] -> []: Then push items that belong to mod0 into these lists.
        3) Perform batching on the per-module lists under each column:
        [col0] -> [mod0]: [...] <- now batched data (numpy array or struct of numpy
        arrays).
        4) Flip column names with ModuleIDs (no cost/extra memory):
        [mod0]:
            [col0]: [batched data]
        etc..

        Note that in order to unmap the resulting batch back into an AgentID based one,
        we have to store the env vector index AND AgentID of each module's batch item
        in an additionally returned `memorized_map_structure`.
        """
        # This Connector should only be used in a multi-agent setting.
        assert isinstance(episodes[0], MultiAgentEpisode)

        # Current agent to module mapping function.
        # agent_to_module_mapping_fn = shared_data.get("agent_to_module_mapping_fn")
        # Store in shared data, which module IDs map to which episode/agent, such
        # that the module-to-env pipeline can map the data back to agents.
        memorized_map_structure = defaultdict(list)
        for column, column_data in data.items():
            if column in rl_module:
                continue
            for eps_id, agent_id, module_id in column_data.keys():
                memorized_map_structure[module_id].append((eps_id, agent_id))
            # TODO (sven): We should check that all columns have the same struct.
            break

        shared_data["memorized_map_structure"] = dict(memorized_map_structure)

        # Mapping from ModuleID to column data.
        data_by_module = {}

        # Iterating over each column in the original data:
        for column, agent_data in data.items():
            if column in rl_module:
                if column in data_by_module:
                    data_by_module[column].update(agent_data)
                else:
                    data_by_module[column] = agent_data
                continue
            for (
                eps_id,
                agent_id,
                module_id,
            ), values_batch_or_list in agent_data.items():
                if not isinstance(values_batch_or_list, list):
                    assert False
                    values_batch_or_list = unbatch(values_batch_or_list)
                for value in values_batch_or_list:
                    if module_id not in data_by_module:
                        data_by_module[module_id] = {column: []}
                    elif column not in data_by_module[module_id]:
                        data_by_module[module_id][column] = []

                    # Append the data.
                    data_by_module[module_id][column].append(value)

        return data_by_module

    def _map_space_if_necessary(self, space):
        if space is None:
            return None
        # Analyze input observation space to check, whether the user has already taken
        # care of the agent to module mapping.
        elif set(self._modules) == set(space.spaces.keys()):
            return space

        # We need to take care of agent to module mapping. Figure out the resulting
        # observation space here.
        dummy_eps = MultiAgentEpisode()

        ret_space = {}
        for module_id in self._modules:
            # Need to reverse map spaces (for the different agents) to certain
            # module IDs (using a dummy MultiAgentEpisode).
            one_space = next(iter(space.spaces.values()))
            # If all obs spaces are the same anyway, just use the first
            # single-agent space.
            if all(s == one_space for s in space.spaces.values()):
                ret_space[module_id] = one_space
            # Otherwise, we have to match the policy ID with all possible
            # agent IDs and find the agent ID that matches.
            else:
                match_aid = None
                for aid in space.spaces.keys():
                    # Match: Assign spaces for this agentID to the PolicyID.
                    if self._agent_to_module_mapping_fn(aid, dummy_eps) == module_id:
                        # Make sure, different agents that map to the same
                        # policy don't have different spaces.
                        if (
                            module_id in ret_space
                            and space[aid] != ret_space[module_id]
                        ):
                            raise ValueError(
                                f"Two agents ({aid} and {match_aid}) in your "
                                "environment map to the same ModuleID (as per your "
                                "`agent_to_module_mapping_fn`), however, these agents "
                                "also have different observation spaces as per the env!"
                            )
                        ret_space[module_id] = space[aid]
                        match_aid = aid

        return gym.spaces.Dict(ret_space)
