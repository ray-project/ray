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
        flat_dist_input_size = sum([d.input_shape_flat for d in self.dist_configs])
        self._input_spec = input_spec
        # Map input shape to the correct shape for action distribution
        # TODO: make this work for N input vectors
        pi_input_size = list(input_spec.values())[0].shape[-1]
        self.to_dist_inputs = torch.nn.Linear(pi_input_size, flat_dist_input_size)
        self._output_spec = ModelSpec({config.output_key: Distribution})

    def _forward(self, inputs: NestedDict) -> NestedDict:
        [pi_inputs] = inputs.values()
        # Map to correct shape
        dist_inputs = self.to_dist_inputs(pi_inputs)
        # Each chunk represents the dist_inputs for a single distribution
        chunks = dist_inputs.split(
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
