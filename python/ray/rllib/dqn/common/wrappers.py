from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models import ModelCatalog
from ray.rllib.dqn.common.atari_wrappers import wrap_deepmind


def wrap_dqn(registry, env, options):
    """Apply a common set of wrappers for DQN."""

    is_atari = hasattr(env.unwrapped, "ale")

    if is_atari:
        return wrap_deepmind(env)

    return ModelCatalog.get_preprocessor_as_wrapper(registry, env, options)
