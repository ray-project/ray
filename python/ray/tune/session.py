from ray.tune._structure_refactor import warn_structure_refactor
from ray.tune.trainable.session import *  # noqa: F401, F403

warn_structure_refactor(__name__, "ray.tune.trainable.session")
