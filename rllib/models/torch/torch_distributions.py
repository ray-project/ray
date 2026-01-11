from ray._common.deprecation import deprecation_warning
from ray.rllib.core.distribution.torch.torch_distribution import (  # noqa
    TorchCategorical,
    TorchDeterministic,
    TorchDiagGaussian,
    TorchDistribution,
    TorchMultiCategorical,
    TorchMultiDistribution,
    TorchSquashedGaussian,
)

deprecation_warning(
    old="ray.rllib.models.torch.torch_distributions.TorchDistribution",
    new="ray.rllib.core.distribution.torch.torch_distribution.TorchDistribution",
    error=False,
)
