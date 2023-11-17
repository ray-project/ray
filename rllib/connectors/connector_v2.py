import abc
from typing import Any, Dict, List, Tuple

from ray.rllib.connectors.connector_context_v2 import ConnectorContextV2
from ray.rllib.connectors.input_output_types import INPUT_OUTPUT_TYPES
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class ConnectorV2(abc.ABC):
    """Base class defining the API for an individual "connector piece".

    A ConnectorV2 ("connector piece") is usually part of a series of pieces within
    a "connector pipeline", which in itself also abides to this very API.
    For example, you might have a connector pipeline consisting of two connector pieces,
    A and B, both instances of subclasses of ConnectorV2 and each one performing a
    particular transformation on their input data. The resulting connector pipeline
    (A->B) itself also abides to this very ConnectorV2 API and could thus be part of yet
    another, higher-level connector pipeline.

    Any ConnectorV2 instances (individual pieces or several connector pieces in a
    pipeline) must be callable by overriding their `__call__()` method. When called,
    they take the outputs of a previous connector piece (or an empty dict if there are
    no previous pieces) as well as all the data collected thus far in the ongoing
    episode(s) (only applies to connectors used in EnvRunners) or retrieved from a
    replay buffer or from an environment sampling step (only applies to connectors used
    in Learner pipelines). From this data (previous piece's output and possibly
    episodes), a ConnectorV2 then performs a transformation step.

    There are 3 types of pipelines a ConnectorV2 can belong to:
    1) env-to-module: The connector transforms envrionment data before it gets to the
    RLModule.
    2) module-to-env: The connector transforms RLModule outputs before they are sent
    back to the environment (as actions).
    3) learner pipeline: The connector transforms data coming directly from an
    environment sampling step or a replay buffer and will be sent into the RLModule's
    `forward_train()` method afterwards to compute the loss inputs.

    Some connectors might be stateful, for example for keeping track of observation
    filtering stats (mean and stddev values). States of all connectors and connector
    pipelines are frequently being synchronized between the EnvRunners (owning the
    env-to-module and module-to-env pipelines) and the Learners (owning the Learner
    pipelines).
    """

    # Set these in ALL subclasses.
    # TODO (sven): Irrelevant for single-agent cases. Once multi-agent is supported
    #  by ConnectorV2, we need to elaborate more on the different input/output types.
    #  For single-agent, the types should always be just INPUT_OUTPUT_TYPES.DATA.
    input_type = INPUT_OUTPUT_TYPES.DATA
    output_type = INPUT_OUTPUT_TYPES.DATA

    def __init__(self, *, ctx: ConnectorContextV2, **kwargs):
        """Initializes a ConnectorV2 instance.

        Args:
            ctx: The initial ConnectorContextV2.
            **kwargs: Forward API-compatibility kwargs.
        """
        self.ctx = ctx

    @abc.abstractmethod
    def __call__(
        self,
        *,
        input_: Any,
        episodes: List[EpisodeType],
        ctx: ConnectorContextV2,
        **kwargs,
    ) -> Any:
        """Method for transforming input data into output data.

        Args:
            input_: The input data abiding to `self.input_type` to be transformed by
                this connector. Transformations might either be done in-place or a new
                structure may be returned. The returned data must match
                `self.output_type`.
            episodes: The list of SingleAgentEpisode or MultiAgentEpisode objects,
                each corresponding to one slot in a gym.vector.Env.
            ctx: The ConnectorContextV2, containing the current Env, RLModule, and other
                context-relevant information. It can also be used to pass along
                information between connector pieces (even across different pipelines).
            **kwargs: Forward API-compatibility kwargs.

        Returns:
            The transformed connector output abiding to `self.output_type`.
        """

    def __str__(self, indentation: int = 0):
        return " " * indentation + self.__class__.__name__

    def get_state(self) -> Dict[str, Any]:
        """Returns the current state of this ConnectorV2.

        Used for checkpointing (connectors may be stateful) as well as synchronization
        between connectors that are run on the (distributed) EnvRunners vs those that
        run on the (distributed) Learners.

        Returns:
            A dict mapping str keys to state information.
        """
        return {}

    def set_state(self, state: Dict[str, Any]) -> None:
        """Sets the state of this connector to the provided one.

        Args:
            state: The new state to set this connector to.
        """
        pass
