import logging
import os

from ray.tune.trainable import Trainable, TrainableUtil
from ray.tune.syncer import get_cloud_sync_client

logger = logging.getLogger(__name__)


class DurableTrainable(Trainable):
    """Abstract class for a remote-storage backed fault-tolerant Trainable.

    Supports checkpointing to and restoring from remote storage. To use this
    class, implement the same private methods as ray.tune.Trainable (`_save`,
    `_train`, `_restore`, `reset_config`, `_setup`, `_stop`).

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
            if checkpoint_dir.starts_with(os.path.abspath(self.logdir)):
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
