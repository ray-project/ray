from typing import List

from ray.rllib.utils.nested_dict import NestedDict
from rllib.models.configs.pi import DistributionMetadata, PiConfig
from rllib.models.distributions import Distribution
from rllib.models.specs.specs_dict import ModelSpec
from rllib.models.torch.model import TorchModel
import torch

from rllib.models.torch.torch_distributions import TorchMultiDistribution


class TorchPi(TorchModel):
    """Maps logits to a MultiTorchDistribution"""

    def __init__(
        self,
        input_spec: ModelSpec,
        dist_configs: List[DistributionMetadata],
        config: PiConfig,
    ):
        self._input_spec = input_spec
        self.dist_configs = dist_configs

        # Size of a single input to produce all the needed distributions
        flat_input_size = sum([d.input_shape_flat for d in self.dist_configs])
        self._input_spec = input_spec
        self.to_logits = torch.nn.Linear(flat_input_size, flat_input_size)
        self.output_spec = ModelSpec({config.output_key: Distribution})

    def _forward(self, inputs: NestedDict) -> NestedDict:
        [logits] = inputs.values()
        # Each chunk represents the logits for a single distribution
        chunks = logits.tensor_split(
            [s for s in self.dist_configs._input_shape_flat], dim=-1
        )
        multidist = []
        for logit_chunk, dist_meta in zip(chunks, self.dist_configs):
            # E.g. [batch, time, agent, action_type, action] batch shapes would be:
            # batch, time, agent
            # feature_shapes would be: action_type, action
            feature_shape_len = len(dist_meta.input_shape)
            batch_shape = logit_chunk.shape[:-feature_shape_len]
            # Reshape for normal, beta, etc. distributions that required N-dimensions
            reshaped_logit_chunk = logit_chunk.reshape(
                *batch_shape, *dist_meta.input_shape
            )
            # Produce the distribution
            multidist.append(dist_meta.distribution_class(reshaped_logit_chunk))

        # Combine all subdistributions into a single distribution for ease
        # of sample()'ing, etc
        inputs[self._config.output_key] = TorchMultiDistribution(multidist)
        return inputs

    @property
    def input_spec(self) -> ModelSpec:
        return self._input_spec

    @property
    def output_spec(self) -> ModelSpec:
        return self._output_spec
