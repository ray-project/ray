from typing import Any, Optional

from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.typing import EnvType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
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
    """

    def __init__(
        self,
        rl_module: Optional[RLModule] = None,
        env: Optional[EnvType] = None,
        explore: Optional[bool] = None,
        *,
        data: Optional[Any] = None,
    ):
        """Initializes a ConnectorContextV2 instance.

        Args:
            rl_module: The RLModule used for forward passes in the current Env -> Module
                setup.
            env: The Env object used to reset/step through in the current Env -> Module
                setup.
            explore: Whether `explore` is currently on. Per convention, if True, the
                RLModule's `forward_exploration` method should be called, if False, the
                EnvRunner should call `forward_inference` instead.
            data: Optional additional context data that needs to be exchanged between
                different Connector pieces and -pipelines.
        """

        self.rl_module = rl_module
        self.env = env
        self.explore = explore

        self._data = data or {}

    def add_data(self, key, value):
        assert key not in self._data
        self._data[key] = value

    def get_data(self, key):
        assert key in self._data
        return self._data[key]

    def override_data(self, key, value):
        assert key in self._data
        self._data[key] = value

    def del_data(self, key):
        assert key in self._data
        del self._data[key]

