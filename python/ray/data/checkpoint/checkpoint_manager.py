import logging

from ray.data.checkpoint import CheckpointConfig
from ray.data.datasource.path_util import _unwrap_protocol

logger = logging.getLogger(__name__)


class CheckpointManager:
    """A checkpoint manager.
    If `CheckpointConfig.delete_checkpoint_on_success` is True, this manager will
    delete the checkpoint after job successfully finishes.
    """

    def __init__(self, config: CheckpointConfig):
        self.checkpoint_path_unwrapped = _unwrap_protocol(config.checkpoint_path)
        self.filesystem = config.filesystem

    def delete_checkpoint(self) -> None:
        self.filesystem.delete_dir(self.checkpoint_path_unwrapped)
