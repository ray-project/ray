from typing import Callable, Type, Union

import inspect
import logging
import os

from ray.tune.function_runner import wrap_function
from ray.tune.registry import get_trainable_cls
from ray.tune.trainable import Trainable, TrainableUtil
from ray.tune.syncer import get_cloud_sync_client

logger = logging.getLogger(__name__)


class DurableTrainable(Trainable):
    """Abstract class for a remote-storage backed fault-tolerant Trainable.

    Supports checkpointing to and restoring from remote storage. To use this
    class, implement the same private methods as ray.tune.Trainable.

    .. warning:: This class is currently **experimental** and may
        be subject to change.

    Run this with Tune as follows. Setting `sync_to_driver=False` disables
    syncing to the driver to avoid keeping redundant checkpoints around, as
    well as preventing the driver from syncing up the same checkpoint.

    See ``tune/trainable.py``.

    Attributes:
        remote_checkpoint_dir (str):  Upload directory (S3 or GS path).
        storage_client: Tune-internal interface for interacting with external
            storage.

    >>> tune.run(MyDurableTrainable, sync_to_driver=False)
    """

    def __init__(self, remote_checkpoint_dir, *args, **kwargs):
        """Initializes a DurableTrainable.

        Args:
            remote_checkpoint_dir (str): Upload directory (S3 or GS path).
        """
        super(DurableTrainable, self).__init__(*args, **kwargs)
        self.remote_checkpoint_dir = remote_checkpoint_dir
        self.storage_client = self._create_storage_client()

    def save(self, checkpoint_dir=None):
        """Saves the current model state to a checkpoint, persisted remotely.

        The storage client must provide durability for
        restoration to work. That is, once ``storage.client.wait()``
        returns after a checkpoint `sync up`, the checkpoint is considered
        committed and can be used to restore the trainable.

        Args:
            checkpoint_dir (Optional[str]): Optional dir to place the
                checkpoint. Must be ``logdir`` or a sub-directory.

        Returns:
            Checkpoint path or prefix that may be passed to restore().
        """
        if checkpoint_dir:
            if checkpoint_dir.startswith(os.path.abspath(self.logdir)):
                raise ValueError("`checkpoint_dir` must be `self.logdir`, or "
                                 "a sub-directory.")
        checkpoint_path = super(DurableTrainable, self).save(checkpoint_dir)
        self.storage_client.sync_up(self.logdir, self.remote_checkpoint_dir)
        self.storage_client.wait()
        return checkpoint_path

    def restore(self, checkpoint_path):
        """Restores training state from a given checkpoint persisted remotely.

        These checkpoints are returned from calls to save().

        Args:
            checkpoint_path (str): Local path to checkpoint.
        """
        self.storage_client.sync_down(self.remote_checkpoint_dir, self.logdir)
        self.storage_client.wait()
        super(DurableTrainable, self).restore(checkpoint_path)

    def delete_checkpoint(self, checkpoint_path):
        """Deletes checkpoint from both local and remote storage.

        Args:
            checkpoint_path (str): Local path to checkpoint.
        """
        try:
            local_dirpath = TrainableUtil.find_checkpoint_dir(checkpoint_path)
        except FileNotFoundError:
            logger.warning(
                "Trial %s: checkpoint path not found during "
                "garbage collection. See issue #6697.", self.trial_id)
        else:
            self.storage_client.delete(self._storage_path(local_dirpath))
        super(DurableTrainable, self).delete_checkpoint(checkpoint_path)

    def _create_storage_client(self):
        """Returns a storage client."""
        return get_cloud_sync_client(self.remote_checkpoint_dir)

    def _storage_path(self, local_path):
        rel_local_path = os.path.relpath(local_path, self.logdir)
        return os.path.join(self.remote_checkpoint_dir, rel_local_path)


def durable(trainable: Union[str, Type[Trainable], Callable]):
    """Convert trainable into a durable trainable.

    Durable trainables are used to upload trial results and checkpoints
    to cloud storage, like e.g. AWS S3.

    This function can be used to convert your trainable, i.e. your trainable
    classes, functions, or string identifiers, to a durable trainable.

    To make durable trainables work, you should pass a valid
    :class:`SyncConfig <ray.tune.SyncConfig>` object to `tune.run()`.

    Example:

    .. code-block:: python

        from ray import tune

        analysis = tune.run(
            tune.durable("PPO"),
            config={"env": "CartPole-v0"},
            checkpoint_freq=1,
            sync_config=tune.SyncConfig(
                sync_to_driver=False,
                upload_dir="s3://your-s3-bucket/durable-ppo/",
            ))

    You can also convert your trainable functions:

    .. code-block:: python

        tune.run(
            tune.durable(your_training_fn),
            # ...
        )

    And your class functions:

    .. code-block:: python

        tune.run(
            tune.durable(YourTrainableClass),
            # ...
        )


    Args:
        trainable (str|Type[Trainable]|Callable): Trainable. Can be a
            string identifier, a trainable class, or a trainable function.

    Returns:
        A durable trainable class wrapped around your trainable.

    """
    if isinstance(trainable, str):
        trainable_cls = get_trainable_cls(trainable)
    else:
        trainable_cls = trainable

    if not inspect.isclass(trainable_cls):
        # Function API
        return wrap_function(trainable_cls, durable=True)

    if not issubclass(trainable_cls, Trainable):
        raise ValueError(
            "You can only use `durable()` with valid trainables. The class "
            "you passed does not inherit from `Trainable`. Please make sure "
            f"it does. Got: {type(trainable_cls)}")

    # else: Class API
    class _WrappedDurableTrainable(DurableTrainable, trainable_cls):
        _name = trainable_cls.__name__ if hasattr(trainable_cls, "__name__") \
            else "durable_trainable"

    return _WrappedDurableTrainable
