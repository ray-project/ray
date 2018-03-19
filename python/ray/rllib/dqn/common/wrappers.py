from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models import ModelCatalog
from ray.rllib.utils.atari_wrappers import wrap_deepmind


def wrap_dqn(registry, env, options, random_starts):
    """Apply a common set of wrappers for DQN."""

    is_atari = hasattr(env.unwrapped, "ale")

    # Override atari default to use the deepmind wrappers.
    # TODO(ekl) this logic should be pushed to the catalog.
    if is_atari and "custom_preprocessor" not in options:
        return wrap_deepmind(env, random_starts=random_starts)

    return ModelCatalog.get_preprocessor_as_wrapper(registry, env, options)
