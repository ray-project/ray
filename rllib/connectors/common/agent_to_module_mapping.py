from collections import defaultdict
from typing import Any, Dict, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleSpec
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType, ModuleID
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class AgentToModuleMapping(ConnectorV2):
    """ConnectorV2 that performs mapping of data from AgentID based to ModuleID based.

    Note: This is one of the default env-to-module or Learner ConnectorV2 pieces that
    are added automatically by RLlib into every env-to-module/Learner connector
    pipeline, unless `config.add_default_connectors_to_env_to_module_pipeline` or
    `config.add_default_connectors_to_learner_pipeline ` are set to
    False.

    The default env-to-module connector pipeline is:
    [
        [0 or more user defined ConnectorV2 pieces],
        AddObservationsFromEpisodesToBatch,
        AddStatesFromEpisodesToBatch,
        AgentToModuleMapping,  # only in multi-agent setups!
        BatchIndividualItems,
        NumpyToTensor,
    ]
    The default Learner connector pipeline is:
    [
        [0 or more user defined ConnectorV2 pieces],
        AddObservationsFromEpisodesToBatch,
        AddColumnsFromEpisodesToTrainBatch,
        AddStatesFromEpisodesToBatch,
        AgentToModuleMapping,  # only in multi-agent setups!
        BatchIndividualItems,
        NumpyToTensor,
    ]

    This connector piece is only used by RLlib (as a default connector piece) in a
    multi-agent setup.

    Note that before the mapping, `data` is expected to have the following
    structure:
    [col0]:
        (eps_id0, ag0, mod0): [list of individual batch items]
        (eps_id0, ag1, mod2): [list of individual batch items]
        (eps_id1, ag0, mod1): [list of individual batch items]
    [col1]:
        etc..

    The target structure of the above `data` would then be:
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

    .. testcode::

        from ray.rllib.connectors.env_to_module import AgentToModuleMapping
        from ray.rllib.utils.test_utils import check

        batch = {
            "obs": {
                ("MA-EPS0", "agent0", "module0"): [0, 1, 2],
                ("MA-EPS0", "agent1", "module1"): [3, 4, 5],
            },
            "actions": {
                ("MA-EPS1", "agent2", "module0"): [8],
                ("MA-EPS0", "agent1", "module1"): [9],
            },
        }

        # Create our connector piece.
        connector = AgentToModuleMapping(
            rl_module_specs={"module0", "module1"},
            agent_to_module_mapping_fn=(
                lambda agent_id, eps: "module1" if agent_id == "agent1" else "module0"
            ),
        )

        # Call the connector (and thereby flip from AgentID based to ModuleID based
        # structure..
        output_batch = connector(
            rl_module=None,  # This particular connector works without an RLModule.
            batch=batch,
            episodes=[],  # This particular connector works without a list of episodes.
            explore=True,
            shared_data={},
        )

        # `data` should now be mapped from ModuleIDs to module data.
        check(
            output_batch,
            {
                "module0": {
                    "obs": [0, 1, 2],
                    "actions": [8],
                },
                "module1": {
                    "obs": [3, 4, 5],
                    "actions": [9],
                },
            },
        )
    """

    @override(ConnectorV2)
    def recompute_output_observation_space(
        self,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
    ) -> gym.Space:
        return self._map_space_if_necessary(input_observation_space, "obs")

    @override(ConnectorV2)
    def recompute_output_action_space(
        self,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
    ) -> gym.Space:
        return self._map_space_if_necessary(input_action_space, "act")

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        rl_module_specs: Dict[ModuleID, RLModuleSpec],
        agent_to_module_mapping_fn,
    ):
        super().__init__(input_observation_space, input_action_space)

        self._rl_module_specs = rl_module_specs
        self._agent_to_module_mapping_fn = agent_to_module_mapping_fn

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
        # Current agent to module mapping function.
        # agent_to_module_mapping_fn = shared_data.get("agent_to_module_mapping_fn")
        # Store in shared data, which module IDs map to which episode/agent, such
        # that the module-to-env pipeline can map the data back to agents.
        memorized_map_structure = defaultdict(list)
        for column, agent_data in batch.items():
            if rl_module is not None and column in rl_module:
                continue
            for eps_id, agent_id, module_id in agent_data.keys():
                memorized_map_structure[module_id].append((eps_id, agent_id))
            # TODO (sven): We should check that all columns have the same struct.
            break

        shared_data["memorized_map_structure"] = dict(memorized_map_structure)

        # Mapping from ModuleID to column data.
        data_by_module = {}

        # Iterating over each column in the original data:
        for column, agent_data in batch.items():
            if rl_module is not None and column in rl_module:
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
                assert isinstance(values_batch_or_list, list)
                for value in values_batch_or_list:
                    if module_id not in data_by_module:
                        data_by_module[module_id] = {column: []}
                    elif column not in data_by_module[module_id]:
                        data_by_module[module_id][column] = []

                    # Append the data.
                    data_by_module[module_id][column].append(value)

        return data_by_module

    def _map_space_if_necessary(self, space: gym.Space, which: str = "obs"):
        # Analyze input observation space to check, whether the user has already taken
        # care of the agent to module mapping.
        if set(self._rl_module_specs) == set(space.spaces.keys()):
            return space

        # We need to take care of agent to module mapping. Figure out the resulting
        # observation space here.
        dummy_eps = MultiAgentEpisode()

        ret_space = {}
        for module_id in self._rl_module_specs:
            # Easy way out, user has provided space in the RLModule spec dict.
            if (
                isinstance(self._rl_module_specs, dict)
                and module_id in self._rl_module_specs
            ):
                if (
                    which == "obs"
                    and self._rl_module_specs[module_id].observation_space
                ):
                    ret_space[module_id] = self._rl_module_specs[
                        module_id
                    ].observation_space
                    continue
                elif which == "act" and self._rl_module_specs[module_id].action_space:
                    ret_space[module_id] = self._rl_module_specs[module_id].action_space
                    continue

            # Need to reverse map spaces (for the different agents) to certain
            # module IDs (using a dummy MultiAgentEpisode).
            one_space = next(iter(space.spaces.values()))
            # If all obs spaces are the same anyway, just use the first
            # single-agent space.
            if all(s == one_space for s in space.spaces.values()):
                ret_space[module_id] = one_space
            # Otherwise, we have to compare the ModuleID with all possible
            # AgentIDs and find the agent ID that matches.
            else:
                match_aid = None
                one_agent_for_module_found = False
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
                        one_agent_for_module_found = True
                # Still no space found for this module ID -> Error out.
                if not one_agent_for_module_found:
                    raise ValueError(
                        f"Could not find or derive any {which}-space for RLModule "
                        f"{module_id}! This can happen if your `config.rl_module(rl_"
                        f"module_spec=...)` does NOT contain space information for this"
                        " particular single-agent module AND your agent-to-module-"
                        "mapping function is stochastic (such that for some agent A, "
                        "more than one ModuleID might be returned somewhat randomly). "
                        f"Fix this error by providing {which}-space information using "
                        "`config.rl_module(rl_module_spec=MultiRLModuleSpec("
                        f"rl_module_specs={{'{module_id}': RLModuleSpec("
                        "observation_space=..., action_space=...)}}))"
                    )

        return gym.spaces.Dict(ret_space)
