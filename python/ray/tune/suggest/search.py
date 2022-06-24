from ray.tune._structure_refactor import warn_structure_refactor
from ray.tune.search.search_algorithm import *  # noqa: F401, F403

warn_structure_refactor(__name__, "ray.tune.search.search_algorithm")
