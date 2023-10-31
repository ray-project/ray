from typing import Any

from ray.rllib.connectors.connector import (
    AgentConnector,
    ConnectorContext,
)
from ray.rllib.connectors.registry import register_connector
from ray.rllib.models.preprocessors import get_preprocessor, NoPreprocessor
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import AgentConnectorDataType
from ray.util.annotations import PublicAPI


# Bridging between current obs preprocessors and connector.
# We should not introduce any new preprocessors.
# TODO(jungong) : migrate and implement preprocessor library in Connector framework.
@PublicAPI(stability="alpha")
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

        if hasattr(ctx.observation_space, "original_space"):
            # ctx.observation_space is the space this Policy deals with.
            # We need to preprocess data from the original observation space here.
            obs_space = ctx.observation_space.original_space
        else:
            obs_space = ctx.observation_space

        self._preprocessor = get_preprocessor(obs_space)(
            obs_space, ctx.config.get("model", {})
        )

    def is_identity(self):
        """Returns whether this preprocessor connector is a no-op preprocessor."""
        return isinstance(self._preprocessor, NoPreprocessor)

    def transform(self, ac_data: AgentConnectorDataType) -> AgentConnectorDataType:
        d = ac_data.data
        assert type(d) == dict, (
            "Single agent data must be of type Dict[str, TensorStructType] but is of "
            "type {}".format(type(d))
        )

        if SampleBatch.OBS in d:
            d[SampleBatch.OBS] = self._preprocessor.transform(d[SampleBatch.OBS])
        if SampleBatch.NEXT_OBS in d:
            d[SampleBatch.NEXT_OBS] = self._preprocessor.transform(
                d[SampleBatch.NEXT_OBS]
            )

        return ac_data

    def to_state(self):
        return ObsPreprocessorConnector.__name__, None

    @staticmethod
    def from_state(ctx: ConnectorContext, params: Any):
        return ObsPreprocessorConnector(ctx)


register_connector(ObsPreprocessorConnector.__name__, ObsPreprocessorConnector)
