import abc
from typing import Any, List, Tuple

from ray.rllib.connectors.connector_context_v2 import ConnectorContextV2
from ray.rllib.connectors.input_output_types import INPUT_OUTPUT_TYPES
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

    All connectors are required to be serializable and implement the `serialize()` method.
    """

    # Set these in ALL subclasses.
    input_type = INPUT_OUTPUT_TYPES.DATA
    output_type = INPUT_OUTPUT_TYPES.DATA

    def __init__(self, *, ctx: ConnectorContextV2, **kwargs):
        """Initializes a ConnectorV2 instance.

        Args:
            ctx: The current ConnectorContextV2.
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
                structure may be returned that matches `self.output_type`.
            episodes: The list of SingleAgentEpisode or MultiAgentEpisode objects,
                each corresponding to one slot in the vector env. Note that episodes
                should always be considered read-only and not be altered.
            ctx: The ConnectorContext that might be used to pass along other important
                information in between connector pieces (even across pipelines).
            kwargs: Forward API-compatibility kwargs.

        Returns:
            The transformed connector output abiding to `self.output_type`.
        """

    def __str__(self, indentation: int = 0):
        return " " * indentation + self.__class__.__name__

    #@abc.abstractmethod
    #def serialize(self) -> Tuple[str, Any]:
    #    """Serialize a connector into a JSON serializable Tuple.

    #    `serialize()` is required, so that all Connectors are serializable.

    #    Returns:
    #        A tuple of connector's name and its serialized states.
    #        String should match the name used to register the connector,
    #        while state can be any single data structure that contains the
    #        serialized state of the connector. If a connector is stateless,
    #        state can simply be None.
    #    """

    #@staticmethod
    #@abc.abstractmethod
    #def from_state(ctx: ConnectorContextV2, params: Any) -> "ConnectorV2":
    #    """De-serialize a JSON params back into a Connector.

    #    `from_state()` is required, so that all Connectors are serializable.

    #    Args:
    #        ctx: ConnectorContextV2 for constructing this connector.
    #        params: Serialized states of the connector to be recovered.

    #    Returns:
    #        De-serialized connector.
    #    """
