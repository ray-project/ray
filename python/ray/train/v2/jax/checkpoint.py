import logging
from typing import Dict, Optional, Tuple

import jax
import orbax.checkpoint as ocp
from orbax.checkpoint import args as ocp_args, type_handlers

from ray.train import Checkpoint, CheckpointConfig
from ray.train._internal.checkpoint_manager import _CheckpointManager
from ray.train._internal.session import _TrainingResult
from ray.train._internal.storage import StorageContext, _exists_at_fs_path

logger = logging.getLogger(__name__)
_DEFAULT_CHUNK_BYTE_SIZE = 1 * 1024 * 1024 * 1024  # 1GB


class JaxCheckpointManager(_CheckpointManager):
    """A CheckpointManager that wraps orbax.CheckpointManager and handles communication with Ray.

    This class manages the lifecycle of checkpoints using both Orbax for storage
    and Ray Train's _CheckpointManager for decision making (keeping top K checkpoints
    based on metrics).

    Args:
        storage_context: The storage context for the checkpoint.
        checkpoint_config: The Ray Train CheckpointConfig to control checkpoint
            keeping/deletion.
        enable_async_checkpointing: Whether to enable async checkpointing.
    """

    def __init__(
        self,
        storage_context: StorageContext,
        checkpoint_config: Optional[CheckpointConfig] = None,
        enable_async_checkpointing: bool = True,
    ):
        super().__init__(checkpoint_config=checkpoint_config)
        self._storage_context = storage_context

        checkpoint_config = checkpoint_config or CheckpointConfig()

        options = ocp.CheckpointManagerOptions(
            max_to_keep=checkpoint_config.num_to_keep,
            best_mode=checkpoint_config.checkpoint_score_order,
            enable_async_checkpointing=enable_async_checkpointing,
        )
        if checkpoint_config.checkpoint_score_attribute:
            options.best_fn = lambda metrics: metrics[
                checkpoint_config.checkpoint_score_attribute
            ]

        # Ensure the directory exists
        # StorageContext.experiment_fs_path already includes the experiment_dir_name
        if not _exists_at_fs_path(
            self._storage_context.storage_filesystem,
            self._storage_context.experiment_fs_path,
        ):
            self._storage_context.storage_filesystem.create_dir(
                self._storage_context.experiment_fs_path
            )

        # Use PyTreeCheckpointHandler for standard PyTree saving
        item_handlers = {
            "items": ocp.PyTreeCheckpointHandler(use_ocdbt=True, use_zarr3=True)
        }

        self._orbax_manager = ocp.CheckpointManager(
            directory=self._storage_context.experiment_fs_path,
            item_handlers=item_handlers,
            options=options,
        )

    def wait_until_finished(self):
        self._orbax_manager.wait_until_finished()

    def save(
        self,
        step: int,
        train_state: dict,
        metrics: Optional[Dict] = None,
        force: bool = False,
        chunk_byte_size: int = _DEFAULT_CHUNK_BYTE_SIZE,
    ) -> str:
        """Saves a checkpoint using Orbax and registers it with Ray's CheckpointManager.

        Args:
            step: The training step (used as checkpoint ID).
            train_state: The train state to save.
            metrics: Metrics associated with this checkpoint, used for top-k keeping.
            force: Whether to force save even if not scheduled (Orbax logic).
            chunk_byte_size: The chunk size for the save operation, default is 1GB.

        Returns:
            The path to the saved checkpoint, or an empty string if the save operation was not performed.
        """

        # Prepare save arguments
        # ocdbt_target_data_file_size controls the chunk size (1GB here)
        save_args = ocp_args.PyTreeSave(
            item=train_state,
            save_args=jax.tree.map(
                lambda _: ocp.SaveArgs(chunk_byte_size=chunk_byte_size), train_state
            ),
        )
        success = self._orbax_manager.save(
            step,
            args=ocp_args.Composite(items=save_args),
            force=force,
            metrics=metrics if metrics else {},
        )

        if success or success is None:
            # Register with Ray
            checkpoint_path = f"{self._storage_context.experiment_fs_path}/{step}"

            # Helper to create Checkpoint matching the storage context filesystem
            checkpoint = Checkpoint(
                filesystem=self._storage_context.storage_filesystem,
                path=checkpoint_path,
            )

            self.register_checkpoint(
                _TrainingResult(checkpoint=checkpoint, metrics=metrics or {})
            )

            logger.info(f"Saved and registered checkpoint for step {step}")
            return checkpoint_path
        return ""

    def restore(
        self,
        target: dict,
        step: Optional[int] = None,
    ) -> Tuple[dict, int]:
        """
        Restores a distributed model checkpoint given the target abstract state with sharding.
        This method should be called by all hosts.
        Args:
            target: A dict containing PyTrees with the desired structure and encodings (sharding).
            step: The step to restore. If None, restores the latest checkpoint.
        Returns:
            A tuple of (dict containing the restored PyTrees, step).
        """
        # 1. Create RestoreArgs with Sharding info
        # This maps every leaf in the tree to an ArrayRestoreArgs object containing its sharding
        # This is CRITICAL for restoring into a distributed mesh correctly
        def map_to_restore_args(leaf):
            if hasattr(leaf, "sharding"):
                return type_handlers.ArrayRestoreArgs(sharding=leaf.sharding)
            return type_handlers.RestoreArgs()

        restore_args_structure = jax.tree.map(map_to_restore_args, target)

        # 2. Create the PyTreeRestore object
        # 'item' is the abstract structure (target)
        # 'restore_args' contains the distributed reading instructions
        checkpoint_args = ocp_args.PyTreeRestore(
            item=target, restore_args=restore_args_structure
        )

        # 3. Restore
        # This returns a dictionary because of the item options
        restored = self._orbax_manager.restore(
            step, args=ocp_args.Composite(items=checkpoint_args)
        )
        # Extract the actual state
        restored_step = step or self._orbax_manager.latest_step()
        logger.info(f"Restored checkpoint for step {restored_step}")
        return restored["items"], restored_step

    def latest_step(self) -> Optional[int]:
        """Returns the latest step saved.

        Returns None if no steps have been saved.

        Returns:
            A step (int) or None if no steps are present.
        """
        return self._orbax_manager.latest_step()

    def best_step(self) -> Optional[int]:
        """Returns the best step saved, as defined by `checkpoint_config.checkpoint_score_attribute`
        and `checkpoint_config.checkpoint_score_order`.

        Returns the latest step if best is not tracked, or None if no steps have been saved.

        Returns:
            A step (int) or None if no steps are present.
        """
        return self._orbax_manager.best_step()

    @property
    def orbax_manager(self) -> ocp.CheckpointManager:
        return self._orbax_manager
