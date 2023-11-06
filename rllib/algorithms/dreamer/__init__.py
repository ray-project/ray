import logging

from ray.rllib.algorithms.dreamer.dreamer import Dreamer, DreamerConfig
from ray.util import log_once


if log_once("dreamer_contrib"):
    logging.getLogger(__name__).warning(
        "`Dreamer(V1)` has been removed from RLlib and will no longer be "
        "maintained by the RLlib team. Use `DreamerV3` instead, which is part of the "
        "RLlib library and will continue to be maintained and supported in the future. "
        "See https://github.com/ray-project/ray/tree/master/rllib/algorithms/dreamerv3 "
        "for more information on our DreamerV3 implementation."
    )


__all__ = [
    "Dreamer",
    "DreamerConfig",
]
