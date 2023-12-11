import abc
from typing import Any, List, Optional

import gymnasium as gym

from ray.rllib.connectors.input_output_types import INPUT_OUTPUT_TYPES
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class ConnectorV2(abc.ABC):
    """Connector base class.

    A connector performs a transformation step, either on envrionment data before it
    gets to the RLModule, or on RLModule output before it is sent back to the
    environment.

    Connectors may be training-aware, for example, behave slightly differently
    during training and inference.

    All connectors are required to be serializable and implement the `serialize()`
    method.
    """

    # Set these in ALL subclasses.
    input_type = INPUT_OUTPUT_TYPES.DATA
    output_type = INPUT_OUTPUT_TYPES.DATA

    @property
    def observation_space(self):
        """Override this if your connector piece changes the input observation space."""
        return self.input_observation_space

    @property
    def action_space(self):
        """Override this if your connector piece changes the input action space."""
        return self.input_action_space

    def __init__(
        self,
        *,
        input_observation_space: Optional[gym.Space],
        input_action_space: Optional[gym.Space],
        env: Optional[gym.Env] = None,
        #rl_module: Optional["RLModule"] = None,
        **kwargs,
    ):
        """Initializes a ConnectorV2 instance.

        Args:
            env: An optional env object that the connector might need to know about.
                Note that normally, env-to-module and module-to-env connectors get this
                information at construction time, but learner connectors won't (b/c
                Learner objects don't carry an environment object).
            input_observation_space: The (mandatory) input observation space. This
                is the space coming from a previous connector piece in the
                (env-to-module or learner) pipeline or it is directly defined within
                the used gym.Env.
            input_action_space: The (mandatory) input action space. This
                is the space coming from a previous connector piece in the
                (module-to-env) pipeline or it is directly defined within the used
                gym.Env.
            #rl_module: An optional RLModule object that the connector might need to know
            #    about. Note that normally, only module-to-env connectors get this
            #    information at construction time, but env-to-module and learner
            #    connectors won't (b/c they get constructed before the RLModule).
            **kwargs: Forward API-compatibility kwargs.
        """
        self.input_observation_space = input_observation_space
        self.input_action_space = input_action_space
        self.env = env
        #self.rl_module = rl_module

    @abc.abstractmethod
    def __call__(
        self,
        *,
        rl_module: RLModule,
        input_: Any,
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        persistent_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        """Method for transforming input data into output data.

        Args:
            input_: The input data abiding to `self.input_type` to be transformed by
                this connector. Transformations might either be done in-place or a new
                structure may be returned that matches `self.output_type`.
            episodes: The list of SingleAgentEpisode or MultiAgentEpisode objects,
                each corresponding to one slot in the vector env. Note that episodes
                should always be considered read-only and not be altered.
            rl_module: An optional RLModule object that the connector might need to know
                about. Note that normally, only module-to-env connectors get this
                information at construction time, but env-to-module and learner
                connectors won't (b/c they get constructed before the RLModule).
            explore: Whether `explore` is currently on. Per convention, if True, the
                RLModule's `forward_exploration` method should be called, if False, the
                EnvRunner should call `forward_inference` instead.
            persistent_data: Optional additional context data that needs to be exchanged
                between different Connector pieces and -pipelines.
            kwargs: Forward API-compatibility kwargs.

        Returns:
            The transformed connector output abiding to `self.output_type`.
        """

    def __str__(self, indentation: int = 0):
        return " " * indentation + self.__class__.__name__

    # @abc.abstractmethod
    # def serialize(self) -> Tuple[str, Any]:
    #    """Serialize a connector into a JSON serializable Tuple.

    #    `serialize()` is required, so that all Connectors are serializable.

    #    Returns:
    #        A tuple of connector's name and its serialized states.
    #        String should match the name used to register the connector,
    #        while state can be any single data structure that contains the
    #        serialized state of the connector. If a connector is stateless,
    #        state can simply be None.
    #    """

    # @staticmethod
    # @abc.abstractmethod
    # def from_state(ctx: ConnectorContextV2, params: Any) -> "ConnectorV2":
    #    """De-serialize a JSON params back into a Connector.

    #    `from_state()` is required, so that all Connectors are serializable.

    #    Args:
    #        ctx: ConnectorContextV2 for constructing this connector.
    #        params: Serialized states of the connector to be recovered.

    #    Returns:
    #        De-serialized connector.
    #    """


#class EnvToModule(ConnectorV2):
#    """EnvToModule connector base class with a mandatory RLModule arg for __call__."""
#    @abc.abstractmethod
#    def __call__(
#        self,
#        *,
#        input_: Optional[Any] = None,
#        episodes: List[EpisodeType],
#        explore: Optional[bool] = None,
#        persistent_data: Optional[dict] = None,
#        rl_module: RLModule,
#        **kwargs,
#    ) -> Any:
#        """Method for transforming input data from the env into output data."""


#class ModuleToEnv(ConnectorV2):
#    @abc.abstractmethod
#    def __call__(
#        self,
#        *,
#        input_: Optional[Any] = None,
#        episodes: List[EpisodeType],
#        explore: Optional[bool] = None,
#        persistent_data: Optional[dict] = None,
#        rl_module: RLModule,
#        **kwargs,
#    ) -> Any:
#        """Method for transforming input data from the env into output data."""
