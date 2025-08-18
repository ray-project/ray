from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.core.distribution.torch.torch_distribution import (  # noqa
    TorchDistribution,
    TorchCategorical,
    TorchDiagGaussian,
    TorchSquashedGaussian,
    TorchDeterministic,
    TorchMultiCategorical,
    TorchMultiDistribution,
)

deprecation_warning(
    old="ray.rllib.models.torch.torch_distributions.TorchDistribution",
    new="ray.rllib.core.distribution.torch.torch_distribution.TorchDistribution",
    error=False,
)
