import warnings

from ray.train import *  # noqa: F401, F403
from ray.train import TrainingCallback

SGDCallback = TrainingCallback

warnings.warn("ray.util.sgd.v2 has been moved to ray.train.")
