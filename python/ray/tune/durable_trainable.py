from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

from ray.tune.trainable import Trainable
from ray.tune.syncer import get_cloud_sync_client


class DurableTrainable(Trainable):
    """A fault-tolerant Trainable.

    Supports checkpointing to and restoring from remote storage.

    The storage client must provide durability for restoration to work. That
    is, once ``storage.client.wait()`` returns after a checkpoint `sync up`,
    the checkpoint is considered committed and can be used to restore the
    trainable.
    """

    def __init__(self, upload_dir, *args, **kwargs):
        super(DurableTrainable, self).__init__(*args, **kwargs)
        self.upload_dir = upload_dir
        self.storage_client = get_cloud_sync_client(self.upload_dir)

    def save(self, checkpoint_dir=None):
        """Saves checkpoint to remote storage."""
        checkpoint_path = super(DurableTrainable, self).save(checkpoint_dir)
        local_dirpath = os.path.join(os.path.dirname(checkpoint_path), "")
        storage_dirpath = self._storage_path(local_dirpath)
        self.storage_client.sync_up(local_dirpath, storage_dirpath)
        self.storage_client.wait()
        return checkpoint_path

    def restore(self, checkpoint_path):
        """Restores checkpoint from remote storage."""
        local_dirpath = os.path.join(os.path.dirname(checkpoint_path), "")
        storage_dirpath = self._storage_path(local_dirpath)
        if not os.path.exists(local_dirpath):
            os.makedirs(local_dirpath)
        self.storage_client.sync_down(storage_dirpath, local_dirpath)
        self.storage_client.wait()
        super(DurableTrainable, self).restore(checkpoint_path)

    def delete_checkpoint(self, checkpoint_path):
        """Deletes checkpoint from both local and remote storage."""
        super(DurableTrainable, self).delete_checkpoint(checkpoint_path)
        local_dirpath = os.path.join(os.path.dirname(checkpoint_path), "")
        self.storage_client.delete(self._storage_path(local_dirpath))

    def _storage_path(self, local_path):
        logdir_parent = os.path.dirname(self.logdir)
        rel_local_path = os.path.relpath(local_path, logdir_parent)
        return os.path.join(self.upload_dir, rel_local_path)
