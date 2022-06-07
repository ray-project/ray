from typing import Any, List

from ray.rllib.connectors.connector import (
    ConnectorContext,
    AgentConnector,
    register_connector,
)
from ray.rllib.models.preprocessors import get_preprocessor
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.typing import AgentConnectorDataType


# Bridging between current obs preprocessors and connector.
# We should not introduce any new preprocessors.
# TODO(jungong) : migrate and implement preprocessor library in Connector framework.
@DeveloperAPI
class ObsPreprocessorConnector(AgentConnector):
    """A connector that wraps around existing RLlib observation preprocessors.

    This includes:
    - OneHotPreprocessor for Discrete and Multi-Discrete spaces.
    - GenericPixelPreprocessor and AtariRamPreprocessor for Atari spaces.
    - TupleFlatteningPreprocessor and DictFlatteningPreprocessor for flattening
      arbitrary nested input observations.
    - RepeatedValuesPreprocessor for padding observations from RLlib Repeated
      observation space.
    """

    def __init__(self, ctx: ConnectorContext):
        super().__init__(ctx)

        self._preprocessor = get_preprocessor(ctx.observation_space)(
            ctx.observation_space, ctx.config.get("model", {})
        )

    def __call__(self, ac_data: AgentConnectorDataType) -> List[AgentConnectorDataType]:
        d = ac_data.data
        assert (
            type(d) == dict
        ), "Single agent data must be of type Dict[str, TensorStructType]"

        if SampleBatch.OBS in d:
            d[SampleBatch.OBS] = self._preprocessor.transform(d[SampleBatch.OBS])
        if SampleBatch.NEXT_OBS in d:
            d[SampleBatch.NEXT_OBS] = self._preprocessor.transform(
                d[SampleBatch.NEXT_OBS]
            )

        return [ac_data]

    def to_config(self):
        return ObsPreprocessorConnector.__name__, {}

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        return ObsPreprocessorConnector(ctx, **params)


register_connector(ObsPreprocessorConnector.__name__, ObsPreprocessorConnector)
