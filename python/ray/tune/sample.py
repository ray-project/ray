from ray.tune._structure_refactor import warn_structure_refactor
from ray.tune.search.sample import *  # noqa: F401, F403

# Needed for FLAML
from ray.tune.search.sample import _BackwardsCompatibleNumpyRng  # noqa: F401, F403

warn_structure_refactor(__name__, "ray.tune.search.sample")
