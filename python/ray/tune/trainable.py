from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Trainable(object):
    """Interface for trainable models, functions, etc.

    Implementing this interface is required to use Ray.tune's full
    functionality, though you can also get away with supplying just a
    `my_train(config, reporter)` function and calling:

    ``register_trainable("my_func", train)``

    to register it for use with tune. The function will be automatically
    converted to this interface (sans checkpoint functionality)."""

    def train(self):
        """Runs one logical iteration of training.

        Returns:
            A TrainingResult that describes training progress.
        """

        raise NotImplementedError

    def save(self):
        """Saves the current model state to a checkpoint.

        Returns:
            Checkpoint path that may be passed to restore().
        """

        raise NotImplementedError

    def restore(self, checkpoint_path):
        """Restores training state from a given model checkpoint.

        These checkpoints are returned from calls to save().
        """

        raise NotImplementedError

    def stop(self):
        """Releases all resources used by this class."""

        pass


def wrap_function(train_func):
    from ray.tune.script_runner import ScriptRunner

    class WrappedFunc(ScriptRunner):
        def _trainable_func(self):
            return train_func

    return WrappedFunc
