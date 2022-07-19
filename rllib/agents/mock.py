from ray.rllib.algorithms.mock import (  # noqa
    _MockTrainer,
    _ParameterTuningTrainer,
    _SigmoidFakeData,
)

from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning(
    old="ray.rllib.agents.callbacks",
    new="ray.rllib.algorithms.callbacks",
    error=False,
)
