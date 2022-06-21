from ray.tune._structure_refactor import warn_structure_refactor
from ray.tune.server.web_server import *  # noqa: F401, F403

warn_structure_refactor(__file__, "ray.tune.server.web_server")
