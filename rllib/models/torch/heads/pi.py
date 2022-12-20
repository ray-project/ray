from typing import TYPE_CHECKING, List

from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.models.distributions import Distribution
from ray.rllib.models.specs.specs_dict import ModelSpec
from ray.rllib.models.torch.model import TorchModel
import torch

from ray.rllib.models.torch.torch_distributions import TorchMultiDistribution

if TYPE_CHECKING:
    from ray.rllib.models.configs.pi import DistributionMetadata, PiConfig


class TorchPi(TorchModel):
    """Maps state to a MultiTorchDistribution: pi(s) -> d"""

    def __init__(
        self,
        input_spec: ModelSpec,
        dist_configs: List["DistributionMetadata"],
        config: "PiConfig",
    ):
        super().__init__(config)
        self._input_spec = input_spec
        self.dist_configs = dist_configs

        # Size of a single input to produce all the needed distributions
        flat_input_size = sum([d.input_shape_flat for d in self.dist_configs])
        self._input_spec = input_spec
        # Map input shape to the correct shape for action distribution
        self.to_dist_inputs = torch.nn.Linear(flat_input_size, flat_input_size)
        self._output_spec = ModelSpec({config.output_key: Distribution})

    def _forward(self, inputs: NestedDict) -> NestedDict:
        [dist_inputs] = inputs.values()
        # Each chunk represents the dist_inputs for a single distribution
        chunks = dist_inputs.tensor_split(
            [s.input_shape_flat for s in self.dist_configs], dim=-1
        )
        # Build a list of distributions, e.g.
        # [Discrete(2), Box(3), Multibinary(2)] -> [
        #   (torch.tensor(2), Categorical),
        #   (torch.tensor(3,2).flatten(), Gaussian),
        #   (torch.tensor(2), Categorical)
        # ]
        multidist = []
        for dist_input, dist_cfg in zip(chunks, self.dist_configs):
            multidist.append(dist_cfg.build(dist_input))

            """

            # E.g. [batch, time, agent, action_type, action] batch shapes would be:
            # batch, time, agent
            # feature_shapes would be: action_type, action
            feature_shape_len = len(dist_cfg.input_shape)
            batch_shape = logit_chunk.shape[:-feature_shape_len]
            # Reshape for normal, beta, etc. distributions that required N-dimensions
            reshaped_logit_chunk = logit_chunk.reshape(
                *batch_shape, *dist_cfg.input_shape
            )
            # Produce the distribution
            multidist.append(dist_cfg.distribution_class.from_dist_inputs(reshaped_logit_chunk))
            """

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
