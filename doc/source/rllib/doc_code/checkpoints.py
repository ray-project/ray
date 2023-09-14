# flake8: noqa

# __rllib-convert-pickle-to-msgpack-checkpoint-begin__
import tempfile

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.simple_q import SimpleQConfig
from ray.rllib.utils.checkpoints import convert_to_msgpack_checkpoint


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
        # type checkpoints.
        algo2 = Algorithm.from_checkpoint(msgpack_cp_dir)

# algo1 and algo2 are now identical.

# __rllib-convert-pickle-to-msgpack-checkpoint-end__
