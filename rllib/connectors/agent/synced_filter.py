from ray.rllib.connectors.connector import (
    AgentConnector,
    ConnectorContext,
)
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class SyncedFilterAgentConnector(AgentConnector):
    """ An agent connector that filters with synchronized parameters."""

    def __init__(self, ctx: ConnectorContext, *args, **kwargs):
        super().__init__(ctx)
        if args or kwargs:
            raise ValueError("SyncedFilterAgentConnector does not take any additional arguments, but got args=`{}` and kwargs={}.".format(args, kwargs))

    def apply_changes(self, other: "Filter", *args, **kwargs) -> None:
        """Updates self with state from other filter."""
        raise NotImplementedError

    def copy(self) -> "Filter":
        """Creates a new object with same state as self.

        Returns:
            A copy of self.
        """
        raise NotImplementedError

    def sync(self, other: "Filter") -> None:
        """Copies all state from other filter to self."""
        raise NotImplementedError

    def reset_buffer(self) -> None:
        """Creates copy of current state and resets accumulated state"""
        raise NotImplementedError
