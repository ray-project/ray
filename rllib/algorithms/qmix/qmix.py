from ray.rllib.algorithms.simple_q import SimpleQ, SimpleQConfig
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


@Deprecated(
    old="rllib/algorithms/qmix/",
    new="rllib_contrib/qmix/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class QMixConfig(SimpleQConfig):
    pass


@Deprecated(
    old="rllib/algorithms/qmix/",
    new="rllib_contrib/qmix/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class QMix(SimpleQ):
    pass
