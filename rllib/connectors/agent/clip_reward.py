from typing import Any, List

import numpy as np

from ray.rllib.connectors.connector import (
    AgentConnector,
    ConnectorContext,
    register_connector,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import AgentConnectorDataType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class ClipRewardAgentConnector(AgentConnector):
    def __init__(self, ctx: ConnectorContext, sign=False, limit=None):
        super().__init__(ctx)
        assert (
            not sign or not limit
        ), "should not enable both sign and limit reward clipping."
        self.sign = sign
        self.limit = limit

    def transform(self, ac_data: AgentConnectorDataType) -> AgentConnectorDataType:
        d = ac_data.data
        assert (
            type(d) == dict
        ), "Single agent data must be of type Dict[str, TensorStructType]"

        assert SampleBatch.REWARDS in d, "input data does not have reward column."
        if self.sign:
            d[SampleBatch.REWARDS] = np.sign(d[SampleBatch.REWARDS])
        elif self.limit:
            d[SampleBatch.REWARDS] = np.clip(
                d[SampleBatch.REWARDS],
                a_min=-self.limit,
                a_max=self.limit,
            )
        return ac_data

    def to_config(self):
        return ClipRewardAgentConnector.__name__, {
            "sign": self.sign,
            "limit": self.limit,
        }

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        return ClipRewardAgentConnector(ctx, **params)


register_connector(ClipRewardAgentConnector.__name__, ClipRewardAgentConnector)
