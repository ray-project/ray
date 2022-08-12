from typing import Any, List

from ray.rllib.connectors.connector import (
    ConnectorContext,
    register_connector,
)
from ray.rllib.connectors.agent.synced_filter import SyncedFilterAgentConnector
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import AgentConnectorDataType
from ray.util.annotations import PublicAPI
from ray.rllib.utils.filter import MeanStdFilter


@PublicAPI(stability="alpha")
class MeanStdObservationFilterAgentConnector(SyncedFilterAgentConnector):
    def __init__(self, ctx: ConnectorContext, shape, demean=True, destd=True,
                 clip=10.0):
        super().__init__(ctx)
        self.filter = MeanStdFilter(shape, demean=True, destd=True, clip=10.0)

    def transform(self, ac_data: AgentConnectorDataType) -> AgentConnectorDataType:
        d = ac_data.data
        assert (
            type(d) == dict
        ), "Single agent data must be of type Dict[str, TensorStructType]"

        return self.filter(ac_data[SampleBatch.OBS])

    def to_state_dict(self):
        return MeanStdObservationFilterAgentConnector.__name__, {
            "sign": self.sign,
            "limit": self.limit,
        }

    @staticmethod
    def from_state_dict(ctx: ConnectorContext, params: List[Any]):
        return ClipRewardAgentConnector(ctx, **params)



register_connector(ClipRewardAgentConnector.__name__, ClipRewardAgentConnector)
