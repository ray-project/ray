import abc
from typing import Any, Dict, List, Optional

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

    There are 3 types of pipelines any ConnectorV2 piece can belong to:
    1) EnvToModulePipeline: The connector transforms environment data before it gets to
    the RLModule. This type of pipeline is used by an EnvRunner for transforming
    env output data into RLModule readable data (for the next RLModule forward pass).
    For example, such a pipeline would include observation postprocessors, -filters,
    or any RNN preparation code related to time-sequences and zero-padding.
    2) ModuleToEnvPipeline: This type of pipeline is used by an
    EnvRunner to transform RLModule output data to env readable actions (for the next
    `env.step()` call). For example, in case the RLModule only outputs action
    distribution parameters (but not actual actions), the ModuleToEnvPipeline would
    take care of sampling the actions to be sent back to the end from the
    resulting distribution (made deterministic if exploration is off).
    3) LearnerConnectorPipeline: This connector pipeline type transforms data coming
    from an `EnvRunner.sample()` call or a replay buffer and will then be sent into the
    RLModule's `forward_train()` method in order to compute loss function inputs.
    This type of pipeline is used by a Learner worker to transform raw training data
    (a batch or a list of episodes) to RLModule readable training data (for the next
    RLModule `forward_train()` call).

    Some connectors might be stateful, for example for keeping track of observation
    filtering stats (mean and stddev values). Any Algorithm, which uses connectors is
    responsible for frequently synchronizing the states of all connectors and connector
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
        """Getter for our (output) observation space.

        Logic: Use user provided space (if set via `observation_space` setter)
        otherwise, use the same as the input space, assuming this connector piece
        does not alter the space.
        """
        return self._observation_space or self.input_observation_space

    @observation_space.setter
    def observation_space(self, value):
        """Setter for our (output) observation space."""
        self._observation_space = value

    @property
    def action_space(self):
        """Getter for our (output) action space.

        Logic: Use user provided space (if set via `action_space` setter)
        otherwise, use the same as the input space, assuming this connector piece
        does not alter the space.
        """
        return self._action_space or self.input_action_space

    @action_space.setter
    def action_space(self, value):
        """Setter for our (output) action space."""
        self._action_space = value

    def __init__(
        self,
        *,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
        **kwargs,
    ):
        """Initializes a ConnectorV2 instance.

        Args:
            input_observation_space: The input observation space for this connector
                piece. This is the space coming from a previous connector piece in the
                (env-to-module or learner) pipeline or it is directly defined within
                the used gym.Env.
            input_action_space: The input action space for this connector piece. This
                is the space coming from a previous connector piece in the
                (module-to-env) pipeline or it is directly defined within the used
                gym.Env.
            **kwargs: Forward API-compatibility kwargs.
        """
        self.input_observation_space = input_observation_space
        self.input_action_space = input_action_space

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
            rl_module: An optional RLModule object that the connector might need to know
                about. Note that normally, only module-to-env connectors get this
                information at construction time, but env-to-module and learner
                connectors won't (b/c they get constructed before the RLModule).
            input_: The input data abiding to `self.input_type` to be transformed by
                this connector. Transformations might either be done in-place or a new
                structure may be returned that matches `self.output_type`.
            episodes: The list of SingleAgentEpisode or MultiAgentEpisode objects,
                each corresponding to one slot in the vector env. Note that episodes
                should always be considered read-only and not be altered.
            explore: Whether `explore` is currently on. Per convention, if True, the
                RLModule's `forward_exploration` method should be called, if False, the
                EnvRunner should call `forward_inference` instead.
            persistent_data: Optional additional context data that needs to be exchanged
                between different Connector pieces and -pipelines.
            kwargs: Forward API-compatibility kwargs.

        Returns:
            The transformed connector output abiding to `self.output_type`.
        """

    def get_state(self) -> Dict[str, Any]:
        """Returns the current state of this ConnectorV2 as a state dict.

        Returns:
            A state dict mapping any string keys to their (state-defining) values.
        """
        return {}

    def set_state(self, state: Dict[str, Any]) -> None:
        """Sets the state of this ConnectorV2 to the given value.

        Args:
            state: The state dict to define this ConnectorV2's new state.
        """
        pass

    def reset_state(self) -> None:
        """Resets the state of this ConnectorV2 to some initial value.

        Note that this may NOT be the exact state that this ConnectorV2 was originally
        constructed with.
        """
        pass

    @staticmethod
    def merge_states(states: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Computes a resulting state given a list of other state dicts.

        Algorithms should use this method for synchronizing states between connectors
        running on workers (of the same type, e.g. EnvRunner workers).

        Args:
            states: The list of n other ConnectorV2 states to merge into a single
                resulting state.

        Returns:
            The resulting state dict.
        """
        return {}

    def __str__(self, indentation: int = 0):
        return " " * indentation + self.__class__.__name__
