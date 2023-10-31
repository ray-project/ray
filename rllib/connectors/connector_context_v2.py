from dataclasses import dataclass
from typing import Any, Optional

from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.typing import AgentID, EnvType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@dataclass
class ConnectorContextV2:
    """Information needed by pieces of connector pipeline to communicate with each other.

    ConnectorContextV2 will be passed to each connector (pipeline) call.
    Also might contain references to the RLModule used, the Env, as well as whether
    `explore` is True or False (whether forward_exploration or forward_inference was
    used).

    TODO: Describe use cases, e.g.
     - state out need to be fed back as state ins.
     Unless we would like to temporarily store the states in the episode.
     - agent_to_policy_mappings need to be stored as they might be stochastic. Then the
     to_env pipeline can properly map back from module (formerly known as policy) IDs
     to agent IDs.

    Attributes:
        env: The Env object used to reset/step through in the current Env -> Module
            setup.
        rl_module: The RLModule used for forward passes in the current Env -> Module
            setup.
        explore: Whether `explore` is currently on. Per convention, if True, the
            RLModule's `forward_exploration` method should be called, if False, the
            EnvRunner should call `forward_inference` instead.
        agent_id: The (optional) current agent ID that the connector should be
            creating/extracting data for.
        episode_index: The (optional) index within the list of SingleAgentEpisodes or
            MultiAgentEpisodes, which each connector is given in a call, that belongs
            to the given agent_id.
        data: Optional additional context data that needs to be exchanged between
            different Connector pieces and -pipelines.
    """

    env: Optional[EnvType] = None
    rl_module: Optional[RLModule] = None
    explore: Optional[bool] = None
    data: Optional[Any] = None

    # TODO (sven): Do these have to be here??
    agent_id: Optional[AgentID] = None
    episode_index: Optional[int] = None

    def add_data(self, key, value):
        assert key not in self.data
        self.data[key] = value

    def get_data(self, key):
        assert key in self.data
        return self.data[key]

    def override_data(self, key, value):
        assert key in self.data
        self.data[key] = value

    def del_data(self, key):
        assert key in self.data
        del self.data[key]
