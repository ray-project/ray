"""TQC (Truncated Quantile Critics) Algorithm.

Paper: https://arxiv.org/abs/2005.04269
"""

from ray.rllib.algorithms.tqc.tqc import TQC, TQCConfig
from ray.rllib.algorithms.tqc.tqc_catalog import TQCCatalog

__all__ = [
    "TQC",
    "TQCConfig",
    "TQCCatalog",
]
