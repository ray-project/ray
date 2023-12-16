import abc
from typing import Any, List, Optional

import gymnasium as gym

from ray.rllib.connectors.input_output_types import INPUT_OUTPUT_TYPES
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class ConnectorV2(abc.ABC):
    """Base class defining the API for an individual "connector piece".

    A ConnectorV2 ("connector piece") is usually part of a whole series of connector
    pieces within a so-called connector pipeline, which in itself also abides to this
    very API..
    For example, you might have a connector pipeline consisting of two connector pieces,
    A and B, both instances of subclasses of ConnectorV2 and each one performing a
    particular transformation on their input data. The resulting connector pipeline
    (A->B) itself also abides to this very ConnectorV2 API and could thus be part of yet
    another, higher-level connector pipeline.

    Any ConnectorV2 instance (individual pieces or several connector pieces in a
    pipeline) is a callable and you should override their `__call__()` method.
    When called, they take the outputs of a previous connector piece (or an empty dict
    if there are no previous pieces) as well as all the data collected thus far in the
    ongoing episode(s) (only applies to connectors used in EnvRunners) or retrieved
    from a replay buffer or from an environment sampling step (only applies to
    connectors used in Learner pipelines). From this input data, a ConnectorV2 then
    performs a transformation step.

    There are 3 types of pipelines a ConnectorV2 can belong to:
    1) env-to-module: The connector transforms envrionment data before it gets to the
    RLModule. This type of pipeline is used by an EnvRunner for transforming
    env output data to RLModule readable data (for the next RLModule forward pass).
    2) module-to-env: The connector transforms RLModule outputs before they are sent
    back to the environment (as actions). This type of pipeline is used by an EnvRunner
    to transform RLModule output data to env readable actions (for the next
    `env.step()` call).
    3) learner pipeline: The connector transforms data coming directly from an
    environment sampling step or a replay buffer and will be sent into the RLModule's
    `forward_train()` method afterwards to compute the loss inputs. This type of
    pipeline is used by a Learner to transform raw training data (a batch or a list of
    episodes) to RLModule readable training data (for the next RLModule
    `forward_train()` call).

    Some connectors might be stateful, for example for keeping track of observation
    filtering stats (mean and stddev values). Any Algorithm, which uses connectors is
    responsible for frequenly synchronizing the states of all connectors and connector
    pipelines between the EnvRunners (owning the env-to-module and module-to-env
    pipelines) and the Learners (owning the Learner pipelines).
    """

    # Set these in ALL subclasses.
    # TODO (sven): Irrelevant for single-agent cases. Once multi-agent is supported
    #  by ConnectorV2, we need to elaborate more on the different input/output types.
    #  For single-agent, the types should always be just INPUT_OUTPUT_TYPES.DATA.
    input_type = INPUT_OUTPUT_TYPES.DATA
    output_type = INPUT_OUTPUT_TYPES.DATA

    @property
    def observation_space(self):
        return self._observation_space or self.input_observation_space

    @observation_space.setter
    def observation_space(self, value):
        self._observation_space = value

    @property
    def action_space(self):
        return self._action_space or self.input_action_space

    @action_space.setter
    def action_space(self, value):
        self._action_space = value

    def __init__(
        self,
        *,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
        env: Optional[gym.Env] = None,
        **kwargs,
    ):
        """Initializes a ConnectorV2 instance.

        Args:
            input_observation_space: The (mandatory) input observation space. This
                is the space coming from a previous connector piece in the
                (env-to-module or learner) pipeline or it is directly defined within
                the used gym.Env.
            input_action_space: The (mandatory) input action space. This
                is the space coming from a previous connector piece in the
                (module-to-env) pipeline or it is directly defined within the used
                gym.Env.
            env: An optional env object that the connector might need to know about.
                Note that normally, env-to-module and module-to-env connectors get this
                information at construction time, but learner connectors won't (b/c
                Learner objects don't carry an environment object).
            **kwargs: Forward API-compatibility kwargs.
        """
        self.input_observation_space = input_observation_space
        self.input_action_space = input_action_space
        self.env = env

        self._observation_space = None
        self._action_space = None

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


# class EnvToModule(ConnectorV2):
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


# class ModuleToEnv(ConnectorV2):
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
