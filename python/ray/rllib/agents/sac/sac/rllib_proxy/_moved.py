""" Classes and functions that have been moved between to different places between
ray 0.8.1 and ray 0.6.6
"""
from ray.rllib.agents.sac.sac.dev_utils import using_ray_8

if using_ray_8():
    from ray.tune.resources import Resources
else:
    from ray.tune.trial import Resources


__all__ = [
    "Resources",
]
