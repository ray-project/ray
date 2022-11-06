import logging
from typing import Any

from ray.rllib.connectors.connector import (
    AgentConnector,
    ConnectorContext,
    register_connector,
)
from ray.rllib.models.preprocessors import get_preprocessor
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import AgentConnectorDataType
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


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

        self.obs_space = obs_space

        self._preprocessor = get_preprocessor(self.obs_space)(
            self.obs_space, ctx.config.get("model", {})
        )

        # This is only for the special case where we auto-wrap environments with
        # atari wrappers.
        if hasattr(self.obs_space, "atari_wrapped_space"):
            # self.obs_space is what the policy deals with. When auto-wrapping with
            # deepmind preprocessing, we save the atari-wrapped space to
            # the `atari_wrapped_space` attribute. This is different from the
            # `original_space` attribute that specifies how to unflatten the tensor
            # into a ragged tensor.
            atari_wrapped_space = self.obs_space.atari_wrapped_space
            self._dummy_atari_preprocessor = get_preprocessor(atari_wrapped_space)(
                atari_wrapped_space, ctx.config.get("model", {})
            )
        else:
            self._dummy_atari_preprocessor = None

    def transform(self, ac_data: AgentConnectorDataType) -> AgentConnectorDataType:
        d = ac_data.data
        assert type(d) == dict, (
            "Single agent data must be of type Dict[str, TensorStructType] but is of "
            "type {}".format(type(d))
        )

        def _transformation_helper(data):
            try:
                return self._preprocessor.transform(data)
            except ValueError as e:
                if self._dummy_atari_preprocessor:
                    try:
                        self._dummy_atari_preprocessor.check_shape(data)
                    except ValueError:
                        # Observation is neither in observation space nor wrapped space
                        raise e
                    # Observation is not in observation original space but in wrapped
                    # space
                    raise ValueError(
                        "ObsPreprocessor connector input is in the space "
                        "of your atari environment, but this connector "
                        "was set up for an atari-wrapped environment. "
                        "You can wrap your environment with "
                        "ray/rllib/env/wrappers/atari_wrappers.py "
                        "during policy inference to mitigate this."
                    )

                else:
                    raise e

        if SampleBatch.OBS in d:
            d[SampleBatch.OBS] = _transformation_helper(d[SampleBatch.OBS])
        if SampleBatch.NEXT_OBS in d:
            d[SampleBatch.NEXT_OBS] = _transformation_helper(d[SampleBatch.NEXT_OBS])

        return ac_data

    def to_state(self):
        return ObsPreprocessorConnector.__name__, None

    @staticmethod
    def from_state(ctx: ConnectorContext, params: Any):
        return ObsPreprocessorConnector(ctx)


register_connector(ObsPreprocessorConnector.__name__, ObsPreprocessorConnector)
