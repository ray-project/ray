from collections import defaultdict
from typing import Any, List, Optional

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
            self._add_most_recent_obs_to_data(data, episodes, is_multi_agent)

        # If our module is stateful:
        # - Add the most recent STATE_OUTs to `data`.
        # - Make all data in `data` have a time rank (T=1).
        if rl_module.is_stateful():
            self._add_most_recent_states_and_time_rank_to_data(
                data, episodes, rl_module, is_multi_agent
            )

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

        # Convert data to proper tensor formats, depending on framework used by the
        # RLModule.
        # TODO (sven): Support GPU-based EnvRunners + RLModules for sampling. Right
        #  now we assume EnvRunners are always only on the CPU.
        data = convert_to_tensor(data, rl_module.framework)

        return data

    @staticmethod
    def _add_most_recent_obs_to_data(data, episodes, is_multi_agent):
        # Single-agent case:
        # Construct:
        #  {"obs": [batch across all single-agent episodes]}
        if not is_multi_agent:
            observations = []
            for sa_episode in episodes:
                # Get most-recent observations from episode.
                observations.append(sa_episode.get_observations(indices=-1))
            # Batch all collected observations together.
            data[SampleBatch.OBS] = batch(observations)
        # Multi-agent case:
        # Construct:
        #  {"obs: {"ag1": [list of all ag1 obs], "ag2": [list of all ag2 obs]}}
        #  Note that we don't batch yet due to the fact that even under the same
        #  AgentID, data may be split up to different ModuleIDs (an agent may map to
        #  one module in one episode, but to another one in a different episode given
        #  e.g. a stochastic mapping function).
        else:
            observations_per_agent = defaultdict(list)
            for ma_episode in episodes:
                # Collect all most-recent observations from given episodes.
                for agent_id, obs in ma_episode.get_observations(-1).items():
                    if not ma_episode.agent_episodes[agent_id].is_done:
                        observations_per_agent[agent_id].append(obs)
            # Batch all collected observations together (separately per agent).
            data[SampleBatch.OBS] = observations_per_agent

    @staticmethod
    def _add_most_recent_states_and_time_rank_to_data(
        data,
        episodes,
        rl_module,
        is_multi_agent,
    ):
        # Single-agent case:
        # Construct:
        #  {STATE_IN: [batch across all state-outs/initial states of all episodes]}
        if not is_multi_agent:
            # Collect all most recently computed STATE_OUT (or use initial states from
            # RLModule if at beginning of episode).
            states = []
            for sa_episode in episodes:
                # Episode just started -> Get initial state from our RLModule.
                if len(sa_episode) == 0:
                    state = rl_module.get_initial_state()
                # Episode is already ongoing -> Use most recent STATE_OUT.
                else:
                    state = sa_episode.get_extra_model_outputs(
                        key=STATE_OUT, indices=-1
                    )
                states.append(state)

            state_in = batch(states)
        # Multi-agent case:
        # Construct:
        #  {STATE_IN: {"ag1": [list of ag1 states], "ag2": [list of ag2 states]}}
        #  Note that we don't batch yet due to the fact that even under the same
        #  AgentID, data may be split up to different ModuleIDs (an agent may map to
        #  one module in one episode, but to another one in a different episode given
        #  e.g. a stochastic mapping function).
        else:
            state_in = defaultdict(list)
            for ma_episode in episodes:
                # Episode just started -> Get initial states from our RLModule.
                if len(ma_episode) == 0:
                    all_states = rl_module.get_initial_state().items()
                # Episode is already ongoing -> Use most recent STATE_OUTs.
                else:
                    all_states = ma_episode.get_extra_model_outputs(
                        key=STATE_OUT, indices=-1, global_ts=True
                    )
                for agent_id, agent_state in all_states.items():
                    state_in[agent_id].append(agent_state)

        # Make all other inputs have an additional T=1 axis.
        data = tree.map_structure(lambda s: np.expand_dims(s, axis=1), data)

        # Batch states (from list of individual vector sub-env states).
        # Note that state ins should NOT have the extra time dimension.
        data[STATE_IN] = state_in

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
        # Per-agent deque of module IDs to map the final per-agent data to.
        # Using a deque here as we will be popleft'ing items out of it again when we
        # perform the mapping step.
        agent_to_module_mappings = defaultdict(list)
        # Store in shared data, which module IDs map to which episode/agent, such
        # that the module-to-env pipeline can map the data back to agents.
        module_to_episode_agents_mapping = defaultdict(list)

        for episode_idx, ma_episode in enumerate(episodes):
            for agent_id in ma_episode.get_agents_to_act():
                module_id = ma_episode.agent_to_module_map.get(agent_id)
                if module_id is None:
                    module_id = agent_to_module_mapping_fn(agent_id, ma_episode)
                    ma_episode.agent_to_module_map[agent_id] = module_id
                agent_to_module_mappings[agent_id].append(module_id)
                # Store (in the correct order) which episode+agentID belongs to which
                # batch item in a module IDs forward batch.
                module_to_episode_agents_mapping[module_id].append(
                    (episode_idx, agent_id)
                )

        shared_data[
            "module_to_episode_agents_mapping"
        ] = module_to_episode_agents_mapping

        # Mapping from ModuleID to column data.
        module_data = {}

        # Iterating over each column in the original data:
        for column, agent_data in data.items():
            for agent_id, values_batch_or_list in agent_data.items():
                for i, value in enumerate(values_batch_or_list):
                    # Retrieve the correct ModuleID.
                    module_id = agent_to_module_mappings[agent_id][i]
                    #
                    if module_id not in module_data:
                        module_data[module_id] = {column: []}
                    elif column not in module_data[module_id]:
                        module_data[module_id][column] = []

                    # Append the data.
                    module_data[module_id][column].append(value)

        # Convert lists of items into properly stacked (batched) data.
        for module_id, columns in module_data.items():
            for column, values in columns.items():
                module_data[module_id][column] = batch(values)

        return module_data
