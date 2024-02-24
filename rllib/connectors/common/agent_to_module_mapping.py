from collections import defaultdict
from ray.rllib.utils.spaces.space_utils import batch, unbatch


def perform_agent_to_module_mapping(data):
    """Performs flipping of `data` from AgentID- to ModuleID based mapping.

    Note that before the mapping, the batch is expected to have the following
    structure:
    [col0]:
        (eps_idx0, ag0, mod0): [list of individual batch items]
        (eps_idx0, ag1, mod2): [list of individual batch items]
        (eps_idx1, ag0, mod1): [list of individual batch items]
    [col1]:
        etc..

    The target structure of the above batch would be:
    [mod0]:
        [col0]: [batched data -> batch_size_B will be the number of items in the
            input data under col0 that have mod0 as their ModuleID]
        [col1]: [batched data]
    [mod1]:
        [col0]: etc.

    Mapping happens in the following stages:

    1) Under each column name, sort keys by eps_idx, then AgentID.
    2) Add ModuleID keys under each column name (no cost/extra memory):
    [col0] -> [mod0]: push items that belong to mod0 into list -> [..., ...]
    2) Perform batching on the per-module lists under each column:
    [col0] -> [mod0]: [...] <- already batched data (numpy array or struct of numpy
    arrays).
    3) Flip column names with ModuleIDs (no cost/extra memory):
    [mod0]:
        [col0]: [batched data]
    etc..

    Note that in order to unmap the resulting batch back into an AgentID based one,
    we have to store the episode index AND agent ID of each module's batch item
    in the given `shared_data` dict (under key: "module_to_episode_agents_mapping").

    Args:
        data: The data batch to map.
        shared_data: A dict into which we will insert a new key
            (`module_to_episode_agents_mapping`) with
    """
    # Current agent to module mapping function.
    # agent_to_module_mapping_fn = shared_data.get("agent_to_module_mapping_fn")
    # Store in shared data, which module IDs map to which episode/agent, such
    # that the module-to-env pipeline can map the data back to agents.
    memorized_map_structure = defaultdict(list)
    for column, column_data in data.items():
        for env_vector_idx, agent_id, module_id in column_data.keys():
            memorized_map_structure[module_id].append((env_vector_idx, agent_id))
        # TODO (sven): We should check that all columns have the same struct.
        break

    # for episode_idx, ma_episode in enumerate(episodes):
    #    for agent_id in ma_episode.get_agents_that_stepped():
    #        module_id = ma_episode.agent_episodes[agent_id].module_id
    #        if module_id is None:
    #            raise NotImplementedError
    # module_id = agent_to_module_mapping_fn(agent_id, ma_episode)
    # ma_episode.agent_episodes[agent_id].module_id = module_id
    # Store (in the correct order) which episode+agentID belongs to which
    # batch item in a module IDs forward batch.

    # shared_data["module_to_episode_agents_mapping"] = dict(
    #    module_to_episode_agents_mapping
    # )

    # Mapping from ModuleID to column data.
    data_by_module = {}

    # Iterating over each column in the original data:
    for column, agent_data in data.items():
        for (env_vector_idx, agent_id, module_id), values_batch_or_list in agent_data.items():
            if not isinstance(values_batch_or_list, list):
                values_batch_or_list = unbatch(values_batch_or_list)
            for value in values_batch_or_list:
                if module_id not in data_by_module:
                    data_by_module[module_id] = {column: []}
                elif column not in data_by_module[module_id]:
                    data_by_module[module_id][column] = []

                # Append the data.
                data_by_module[module_id][column].append(value)

    # Batch all (now restructured) data again.
    for module_id, module_data in data_by_module.items():
        for column, l in module_data.copy().items():
            module_data[column] = batch(l)

    return data_by_module, memorized_map_structure


def perform_module_to_agent_unmapping(data, memorized_map_structure):#episodes, shared_data):
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
    #module_to_episode_agents_mapping = shared_data[
    #    "module_to_episode_agents_mapping"
    #]
    agent_data = defaultdict(dict)
    for module_id, module_data in data.items():
        if module_id not in memorized_map_structure:
            raise KeyError(
                f"ModuleID={module_id} not found in `memorized_map_structure`!"
            )

        for column, values_batch in module_data.items():
            #if column not in agent_data:
            #    agent_data[column] = [{} for _ in range(len(episodes))]
            individual_items = unbatch(values_batch)
            assert len(individual_items) == len(memorized_map_structure[module_id])
            for val, (env_vector_idx, agent_id) in zip(
                individual_items, memorized_map_structure[module_id]
            ):
            #for i, val in enumerate():
                ## TODO (sven): If one agent is terminated, we should NOT perform
                ##  another forward pass on its (obs) data anymore, which we
                ##  currently do in case we are using the WriteObservationsToEpisode
                ##  connector piece, due to the fact that this piece requires even
                ##  the terminal obs to be processed inside the batch. This if block
                ##  here is a temporary fix for this issue.
                #if episodes[eps_idx].agent_episodes[agent_id].is_done:
                #    continue
                key = (env_vector_idx, agent_id, module_id)
                assert key not in agent_data[column]
                agent_data[column][key] = val

    return dict(agent_data)
