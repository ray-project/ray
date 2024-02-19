from collections import defaultdict
from typing import Any, List, Optional

import gymnasium as gym
import numpy as np

import tree
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.models.base import STATE_IN, STATE_OUT
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import convert_to_tensor
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class DefaultEnvToModule(ConnectorV2):
    """Default connector piece added by RLlib to the end of any env-to-module pipeline.

    Makes sure that the output data will have at the minimum:
    a) An observation (the most recent one returned by `env.step()`) under the
    SampleBatch.OBS key for each agent and
    b) In case the RLModule is stateful, a STATE_IN key populated with the most recently
    computed STATE_OUT.

    The connector will not add any new data in case other connector pieces in the
    pipeline already take care of populating these fields (obs and state in).
    """

    @property
    @override(ConnectorV2)
    def observation_space(self):
        return self._map_space_if_necessary(self.input_observation_space)

    @property
    @override(ConnectorV2)
    def action_space(self):
        return self._map_space_if_necessary(self.input_action_space)

    def __init__(
        self,
        input_observation_space,
        input_action_space,
        *,
        multi_agent: bool = False,
        modules=None,
        agent_to_module_mapping_fn=None,
    ):
        super().__init__(input_observation_space, input_action_space)

        self._multi_agent = multi_agent
        self._modules = modules
        self._agent_to_module_mapping_fn = agent_to_module_mapping_fn

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        data: Optional[Any] = None,
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        is_multi_agent = isinstance(episodes[0], MultiAgentEpisode)

        # TODO (sven): Prepare for case where user already did the agent to module
        #  mapping and our data arrives here with top-level keys: "module1", "module2",
        #  etc.. (instead of top-level keys OBS, ACTIONS, ...).

        # If observations cannot be found in `input`, add the most recent ones (from all
        # episodes).
        if SampleBatch.OBS not in data:
            data = self._add_most_recent_obs_to_data(data, episodes, rl_module)

        # If our module is stateful AND uses has not provided a STATE_IN yet:
        # - Add the most recent STATE_OUTs to `data`.
        # - Later (after everything has been numpyf'ied): Make all data in `data`
        # have a time rank (T=1).
        added_state = None
        if rl_module.is_stateful() and STATE_IN not in data:
            data = self._add_most_recent_states_to_data(data, episodes, rl_module)
            added_state = data[STATE_IN]

        # Perform AgentID to ModuleID mapping.
        if is_multi_agent:
            # TODO (sven): Prepare for case where user already did the agent to module
            #  mapping (see TODO comment above).
            # Make sure user has not already done the mapping themselves.
            # If all keys under obs are part of the MARLModule, then the mapping has
            # already happened.
            all_module_ids = set(rl_module.keys())
            if not all(
                module_id in all_module_ids
                for module_id in data[SampleBatch.OBS].keys()
            ):
                if shared_data is None:
                    raise RuntimeError(
                        "In multi-agent mode, the DefaultEnvToModule requires the "
                        "`shared_data` argument (a dict) to be passed into the call!"
                    )
                data = self._perform_agent_to_module_mapping(
                    data, episodes, shared_data
                )
                # Convert lists of items into properly stacked (batched) data.
                for module_id, columns in data.items():
                    for column, column_data in columns.items():
                        if isinstance(column_data, list):
                            data[module_id][column] = batch(column_data)

        # Convert lists of items into properly stacked (batched) data.
        else:
            for column, column_data in data.items():
                if isinstance(column_data, list):
                    data[column] = batch(column_data)

        # Only after everything has been batched and there are not more lists
        # underneath the column names:
        # Make all inputs (other than STATE_IN) have an additional T=1 axis.
        if added_state is not None:
            data = tree.map_structure_with_path(
                lambda p, s: np.expand_dims(s, axis=1) if STATE_IN not in p else s,
                data,
            )

        # Convert data to proper tensor formats, depending on framework used by the
        # RLModule.
        # TODO (sven): Support GPU-based EnvRunners + RLModules for sampling. Right
        #  now we assume EnvRunners are always only on the CPU.
        data = convert_to_tensor(data, rl_module.framework)

        return data

    def _add_most_recent_obs_to_data(self, data, episodes, rl_module):
        for sa_episode in self.single_agent_episode_iterator(episodes):
            self.add_batch_item(
                batch=data,
                column=SampleBatch.OBS,
                item_to_add=sa_episode.get_observations(-1),
                single_agent_episode=sa_episode,
            )
        return data

    def _add_most_recent_states_to_data(self, data, episodes, rl_module):
        sa_module = rl_module
        for sa_episode in self.single_agent_episode_iterator(episodes):
            # Multi-agent case: Extract correct single agent RLModule (to get the state
            # for individually).
            if sa_episode.module_id is not None:
                sa_module = rl_module[sa_episode.module_id]

            # Episode just started -> Get initial state from our RLModule.
            if len(sa_episode) == 0:
                state = sa_module.get_initial_state()
            # Episode is already ongoing -> Use most recent STATE_OUT.
            else:
                state = sa_episode.get_extra_model_outputs(key=STATE_OUT, indices=-1)

            self.add_batch_item(
                column=STATE_IN,
                batch=data,
                item_to_add=state,
                single_agent_episode=sa_episode,
            )

        return data

    @staticmethod
    def _perform_agent_to_module_mapping(data, episodes, shared_data):
        """Performs flipping of `data` from agent ID- to module ID based mapping.

        Before mapping (obs):
        data[OBS]: "ag1" ... "ag2" ...

        # Mapping (no cost/extra memory):
        data[OBS]: "module1": push into list -> [..., ...]
        # ... then perform batching: stack([each module's items in list], axis=0)
        data[OBS]: "module1": [...] <- already batched data
        # Flip column (e.g. OBS) with module IDs (no cost/extra memory):
        data[module1]: OBS: ...
        data[module2]: OBS: ...
        """
        # Current agent to module mapping function.
        agent_to_module_mapping_fn = shared_data.get("agent_to_module_mapping_fn")
        # Store in shared data, which module IDs map to which episode/agent, such
        # that the module-to-env pipeline can map the data back to agents.
        module_to_episode_agents_mapping = defaultdict(list)

        for episode_idx, ma_episode in enumerate(episodes):
            for agent_id in ma_episode.get_agents_that_stepped():
                module_id = ma_episode.agent_episodes[agent_id].module_id
                if module_id is None:
                    module_id = agent_to_module_mapping_fn(agent_id, ma_episode)
                    ma_episode.agent_episodes[agent_id].module_id = module_id
                # Store (in the correct order) which episode+agentID belongs to which
                # batch item in a module IDs forward batch.
                module_to_episode_agents_mapping[module_id].append(
                    (episode_idx, agent_id)
                )

        shared_data["module_to_episode_agents_mapping"] = dict(
            module_to_episode_agents_mapping
        )

        # Mapping from ModuleID to column data.
        module_data = {}

        # Iterating over each column in the original data:
        for column, agent_data in data.items():
            for (agent_id, module_id), values_batch_or_list in agent_data.items():
                for i, value in enumerate(values_batch_or_list):
                    if module_id not in module_data:
                        module_data[module_id] = {column: []}
                    elif column not in module_data[module_id]:
                        module_data[module_id][column] = []

                    # Append the data.
                    module_data[module_id][column].append(value)

        return module_data

    def _map_space_if_necessary(self, space):
        if not self._multi_agent:
            return space

        # Analyze input observation space to check, whether the user has already taken
        # care of the agent to module mapping.
        if set(self._modules) == set(space.spaces.keys()):
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
