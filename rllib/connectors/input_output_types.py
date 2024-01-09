from enum import Enum


class INPUT_OUTPUT_TYPES(Enum):
    """Definitions of possible datatypes being processed by individual connectors.

    TODO: Make sure this is valid:
     Each connector will always receive a list of Episodes (MultiAgentEpisodes or
     SingleAgentEpisodes, depending on the setup and EnvRunner used). In addition, the
     output of the previous connector (or an empty dict at the beginnnig) will be
     received.
     An IntoModule connector pipeline should eventually output a dict mapping module IDs
     to SampleBatches

    Typical env-module-env pipeline:
        env.step(List[Data]) -> List[MultiAgentEpisode]

        connector: auto-agent-extraction: List[MultiAgentEpisode] -> dict[AgentID, Data]
        connector: auto-broadcast: Data -> Data (legacy postprocessing and filtering)
            under the hood: dict[AgentID, Data] -> dict[AgentID, Data]
        connector: auto-policy-mapping: dict[AgentID, Data] -> dict[ModuleID, Data]

        module.forward_exploration() -> dict[ModuleID, Data]

        connector: auto-action-sampling: dict[ModuleID, Data] -> dict[ModuleID, Data]
        connector: action-clipping: Data -> Data
            under the hood: dict[ModuleID, Data] -> dict[ModuleID, Data]
        connector: auto-policy-unmapping: dict[ModuleID, Data] -> dict[AgentID, Data]
            (using information stored in connector ctx)
        connector: auto-action-sorting (using information stored in connector ctx):
            dict[AgentID, Data] -> List[Data]

        env.step(List[Data]) ... repeats

    Typical training pipeline:


    Default env-module-env pipeline picked by RLlib if no connector defined by user AND
    module is RNN:
        env.step(List[Data]) -> List[MultiAgentEpisode]

        connector: auto-agent-extraction: List[MultiAgentEpisode] -> dict[AgentID, Data]
        connector: auto-policy-mapping: dict[AgentID, Data] -> dict[ModuleID, Data]
        connector: auto-state-handling: dict[ModuleID, Data] ->
            dict[ModuleID, Data + state] (using information stored in connector ctx)

        module.forward_exploration() -> dict[ModuleID, Data + state]

        connector: auto-state-handling: dict[ModuleID, Data + state] ->
            dict[ModuleID, Data] (state was stored in ctx)
        connector: auto-policy-unmapping: dict[ModuleID, Data] ->
            dict[AgentID, Data] (using information stored in connector ctx)
        connector: auto-action-sorting (using information stored in connector ctx):
            dict[AgentID, Data] -> List[Data]

        env.step(List[Data]) ... repeats
    """

    # Normally, after `env.step()`, we have a list (vector env) of MultiAgentEpisodes
    # as a starting point.
    LIST_OF_MULTI_AGENT_EPISODES = 0
    # In the simplified case, there might be a list of SingleAgentEpisodes, instead.
    LIST_OF_SINGLE_AGENT_EPISODES = 1

    # From each MultiAgentEpisode, we might extract a dict, mapping agent IDs to data.
    LIST_OF_DICTS_MAPPING_AGENT_IDS_TO_DATA = 10
    # Eventually boiling down to simply one dict mapping agent IDs to data.
    #
    DICT_MAPPING_AGENT_IDS_TO_DATA = 11

    # Right after the module's forward pass, we usually have a single dict mapping
    # Module IDs to data (model outputs).
    DICT_MAPPING_MODULE_IDS_TO_DATA = 12

    DATA = 11
