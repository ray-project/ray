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
        # Size of the inputs if we concatenate them over the last dim
        prev_module_size = sum(v.shape[-1] for v in input_spec.values())
        # Map input shape to the correct shape for action distribution
        self.to_dist_inputs = torch.nn.Linear(prev_module_size, flat_dist_input_size)
        self._output_spec = ModelSpec({config.output_key: Distribution})

    def _forward(self, inputs: NestedDict) -> NestedDict:
        # Ensure all inputs have matching dims before concat
        # so we can emit an informative error message
        first_key, first_tensor = list(inputs.items())[0]
        for k, tensor in inputs.items():
            assert tensor.shape[:-1] == first_tensor.shape[:-1], (
                "Inputs have mismatching dimensions, all dims but the last should "
                f"be equal: {first_key}: {first_tensor.shape} != {k}: {tensor.shape}"
            )
        # Concatenate all input along the feature dim
        pi_inputs = torch.cat(list(inputs.values()), dim=-1)
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
