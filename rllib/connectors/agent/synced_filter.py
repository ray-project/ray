from ray.rllib.connectors.connector import (
    AgentConnector,
    ConnectorContext,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import AgentConnectorDataType
from ray.util.annotations import PublicAPI
from ray.rllib.utils.filter import Filter


@PublicAPI(stability="alpha")
class SyncedFilterAgentConnector(AgentConnector):
    """An agent connector that filters with synchronized parameters."""

    def __init__(self, ctx: ConnectorContext, *args, **kwargs):
        super().__init__(ctx)
        if args or kwargs:
            raise ValueError(
                "SyncedFilterAgentConnector does not take any additional arguments, "
                "but got args=`{}` and kwargs={}.".format(args, kwargs)
            )

    def apply_changes(self, other: "Filter", *args, **kwargs) -> None:
        """Updates self with state from other filter."""
        # inline this as soon as we deprecate ordinary filter with non-connector
        # env_runner
        return self.filter.apply_changes(other, *args, **kwargs)

    def copy(self) -> "Filter":
        """Creates a new object with same state as self.

        This is a legacy Filter method that we need to keep around for now

        Returns:
            A copy of self.
        """
        # inline this as soon as we deprecate ordinary filter with non-connector
        # env_runner
        return self.filter.copy()

    def sync(self, other: "AgentConnector") -> None:
        """Copies all state from other filter to self."""
        # inline this as soon as we deprecate ordinary filter with non-connector
        # env_runner
        return self.filter.sync(other)

    def reset_state(self) -> None:
        """Creates copy of current state and resets accumulated state"""
        raise NotImplementedError

    def as_serializable(self) -> "Filter":
        # inline this as soon as we deprecate ordinary filter with non-connector
        # env_runner
        return self.filter.as_serializable()
