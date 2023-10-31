from typing import Any

import numpy as np

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.connector_context_v2 import ConnectorContextV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import AgentConnectorDataType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class ClipRewards(ConnectorV2):
    def __init__(self, ctx: ConnectorContextV2, sign=False, limit=None):
        super().__init__(ctx)
        assert (
            not sign or not limit
        ), "Should not enable both sign and limit reward clipping!"
        self.sign = sign
        self.limit = limit

    def __call__(
        self,
        input_: Any,
        episodes: List[EpisodeType],
        ctx: ConnectorContextV2,
    ) -> Any:
        # Nothing to clip. May happen for initial obs.
        if SampleBatch.REWARDS not in d:
            return ac_data

        if self.sign:
            d[SampleBatch.REWARDS] = np.sign(d[SampleBatch.REWARDS])
        elif self.limit:
            d[SampleBatch.REWARDS] = np.clip(
                d[SampleBatch.REWARDS],
                a_min=-self.limit,
                a_max=self.limit,
            )
        return ac_data

    @override(AgentConnector)
    def serialize(self):
        return ClipRewards.__name__, {
            "sign": self.sign,
            "limit": self.limit,
        }

    @staticmethod
    def from_state(ctx: ConnectorContext, params: Any):
        return ClipRewardAgentConnector(ctx, **params)
