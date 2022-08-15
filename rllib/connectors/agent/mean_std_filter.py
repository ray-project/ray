from typing import Any, List

from ray.rllib.connectors.agent.synced_filter import SyncedFilterAgentConnector
from ray.rllib.connectors.connector import (
    ConnectorContext,
    register_connector,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.filter import MeanStdFilter, ConcurrentMeanStdFilter
from ray.rllib.utils.typing import AgentConnectorDataType
from ray.util.annotations import PublicAPI
from ray.rllib.utils.filter import Filter
from ray.rllib.connectors.connector import AgentConnector


@PublicAPI(stability="alpha")
class MeanStdObservationFilterAgentConnector(SyncedFilterAgentConnector):
    def __init__(
        self, ctx: ConnectorContext, shape, demean=True, destd=True, clip=10.0
    ):
        SyncedFilterAgentConnector(ctx)
        # We simply use the old MeanStdFilter until non-connector env_runner is fully
        # deprecated to avoid duplicate code
        self.filter = MeanStdFilter(shape, demean=True, destd=True, clip=10.0)

    def transform(self, ac_data: AgentConnectorDataType) -> AgentConnectorDataType:
        d = ac_data.data
        assert (
            type(d) == dict
        ), "Single agent data must be of type Dict[str, TensorStructType]"
        if SampleBatch.OBS in d:
            d[SampleBatch.OBS] = self.filter(d[SampleBatch.OBS])
        if SampleBatch.NEXT_OBS in d:
            d[SampleBatch.NEXT_OBS] = self.filter(d[SampleBatch.NEXT_OBS])

        return ac_data

    def to_state_dict(self):
        return MeanStdObservationFilterAgentConnector.__name__, {
            "sign": self.sign,
            "limit": self.limit,
        }

    @staticmethod
    def from_state_dict(ctx: ConnectorContext, params: List[Any]):
        return MeanStdObservationFilterAgentConnector(ctx, **params)

    def reset_state(self) -> None:
        """Creates copy of current state and resets accumulated state"""
        if not self._is_training:
            raise ValueError(
                "State of {} can only be changed when trainin.".format(self.__name__)
            )
        self.filter.reset_buffer()

    def apply_changes(self, other: "Filter", *args, **kwargs) -> None:
        """Updates self with state from other filter."""
        # inline this as soon as we deprecate ordinary filter with non-connector
        # env_runner
        if not self._is_training:
            raise ValueError(
                "Changes can only be applied to {} when trainin.".format(self.__name__)
            )
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
        if not self._is_training:
            raise ValueError(
                "{} can only be synced when trainin.".format(self.__name__)
            )
        return self.filter.sync(other)


@PublicAPI(stability="alpha")
class ConcurrentMeanStdObservationFilterAgentConnector(
    MeanStdObservationFilterAgentConnector
):
    def __init__(
        self, ctx: ConnectorContext, shape, demean=True, destd=True, clip=10.0
    ):
        SyncedFilterAgentConnector(ctx)
        # We simply use the old MeanStdFilter until non-connector env_runner is fully
        # deprecated to avoid duplicate code
        self.filter = ConcurrentMeanStdFilter(shape, demean=True, destd=True, clip=10.0)


register_connector(
    MeanStdObservationFilterAgentConnector.__name__,
    MeanStdObservationFilterAgentConnector,
)
register_connector(
    ConcurrentMeanStdObservationFilterAgentConnector.__name__,
    ConcurrentMeanStdObservationFilterAgentConnector,
)
