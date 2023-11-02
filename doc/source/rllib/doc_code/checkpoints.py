# flake8: noqa

# __rllib-convert-pickle-to-msgpack-checkpoint-begin__
import tempfile

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.simple_q import SimpleQConfig
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.checkpoints import (
    convert_to_msgpack_checkpoint,
    convert_to_msgpack_policy_checkpoint,
)


# Base config used for both pickle-based checkpoint and msgpack-based one.
config = SimpleQConfig().environment("CartPole-v1")
# Build algorithm object.
algo1 = config.build()

# Create standard (pickle-based) checkpoint.
with tempfile.TemporaryDirectory() as pickle_cp_dir:
    # Note: `save()` always creates a pickle based checkpoint.
    algo1.save(checkpoint_dir=pickle_cp_dir)

    # But we can convert this pickle checkpoint to a msgpack one using an RLlib utility
    # function.
    with tempfile.TemporaryDirectory() as msgpack_cp_dir:
        convert_to_msgpack_checkpoint(pickle_cp_dir, msgpack_cp_dir)

        # Try recreating a new algorithm object from the msgpack checkpoint.
        # Note: `Algorithm.from_checkpoint` now works with both pickle AND msgpack
        # type checkpoints. However, when recovering from a msgpack-based checkpoint,
        # you will have to provide the original config object (or dict) via the `config`
        # arg, b/c msgpack cannot store any non-serializable information such as custom
        # classes, gym Spaces, or lambdas.
        algo2 = Algorithm.from_checkpoint(msgpack_cp_dir, config=config)

# algo1 and algo2 are now identical.

# __rllib-convert-pickle-to-msgpack-checkpoint-end__


# __rllib-convert-pickle-to-msgpack-policy-checkpoint-begin__
config = SimpleQConfig().environment("CartPole-v1")

# Build algorithm/policy objects.
algo1 = config.build()
pol1 = algo1.get_policy()

# Create standard (pickle-based) checkpoint.
with tempfile.TemporaryDirectory() as pickle_cp_dir:
    pol1.export_checkpoint(pickle_cp_dir)
    # Now convert pickle checkpoint to msgpack using the provided
    # utility function.
    with tempfile.TemporaryDirectory() as msgpack_cp_dir:
        convert_to_msgpack_policy_checkpoint(pickle_cp_dir, msgpack_cp_dir)
        # Try recreating a new policy object from the msgpack checkpoint.
        # Note: `Policy.from_checkpoint` now works with both pickle AND msgpack
        # type checkpoints. However, when recovering from a msgpack-based checkpoint,
        # you will have to provide the original config object (or dict) via the `config`
        # arg, b/c msgpack cannot store any non-serializable information such as custom
        # classes, gym Spaces, or lambdas.
        pol2 = Policy.from_checkpoint(msgpack_cp_dir, config=config)

# pol1 and pol2 are now identical.

# __rllib-convert-pickle-to-msgpack-policy-checkpoint-end__
