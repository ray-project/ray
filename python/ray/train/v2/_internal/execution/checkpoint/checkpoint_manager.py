import logging
from typing import Any, Dict, List, Optional

from ray.air.config import CheckpointConfig
from ray.train._checkpoint import Checkpoint
from ray.train._internal.checkpoint_manager import (
    _CheckpointManager,
    _insert_into_sorted_list,
)
from ray.train._internal.session import _TrainingResult
from ray.train.v2._internal.exceptions import CheckpointManagerInitializationError
from ray.train.v2._internal.execution.callback import ReportCallback
from ray.train.v2._internal.execution.context import StorageContext
from ray.train.v2._internal.execution.storage import _delete_fs_path, _exists_at_fs_path

try:
    from pydantic import BaseModel
    from pydantic_core import from_json
except (ImportError, ModuleNotFoundError) as exc:
    raise ImportError(
        "`ray.train.v2` requires the pydantic package, which is missing. "
        "Run the following command to fix this: `pip install pydantic`"
    ) from exc


logger = logging.getLogger(__name__)


class _TrainingResultState(BaseModel):
    # Increment version if the schema changes
    version: int = 0
    checkpoint_dir_name: str
    metrics: dict


class _CheckpointManagerState(BaseModel):
    # Increment version if the schema changes
    version: int = 0
    checkpoint_results: List[_TrainingResultState]
    latest_checkpoint_result: Optional[_TrainingResultState]


def _get_training_result_from_state(
    state: _TrainingResultState,
    storage_context: StorageContext,
) -> _TrainingResult:
    """Get a TrainingResult object from a Pydantic state object."""
    return _TrainingResult(
        checkpoint=Checkpoint(
            path=storage_context.build_checkpoint_path_from_name(
                state.checkpoint_dir_name
            ),
            filesystem=storage_context.storage_filesystem,
        ),
        metrics=state.metrics,
    )


def _get_state_from_training_result(
    training_result: _TrainingResult,
    storage_context: StorageContext,
) -> _TrainingResultState:
    """Get a Pydantic state object from a TrainingResult object."""
    return _TrainingResultState(
        checkpoint_dir_name=storage_context.extract_checkpoint_dir_name_from_path(
            training_result.checkpoint.path
        ),
        metrics=training_result.metrics,
    )


class CheckpointManager(_CheckpointManager, ReportCallback):
    def __init__(
        self,
        checkpoint_config: CheckpointConfig,
        storage_context: StorageContext,
    ):
        self._storage_context = storage_context
        self._checkpoint_config = checkpoint_config
        super().__init__(checkpoint_config)
        # If the snapshot is found, the checkpoint manager will restore its state.
        self._maybe_load_state_from_storage()

    def register_checkpoint(self, checkpoint_result: _TrainingResult):
        """Register new checkpoint and add to bookkeeping.

        This method will register a new checkpoint and add it to the internal
        bookkeeping logic. This means the checkpoint manager will decide if
        this checkpoint should be kept, and if older or worse performing
        checkpoints should be deleted.

        Args:
            checkpoint: Tracked checkpoint object to add to bookkeeping.
        """
        self._latest_checkpoint_result = checkpoint_result

        if self._checkpoint_config.checkpoint_score_attribute is not None:
            # If we're ordering by a score, insert the checkpoint
            # so that the list remains sorted.
            _insert_into_sorted_list(
                self._checkpoint_results,
                checkpoint_result,
                key=self._get_checkpoint_score,
            )
        else:
            # If no metric is provided, just append (ordering by time of registration).
            self._checkpoint_results.append(checkpoint_result)

        results_to_delete = {}
        if self._checkpoint_config.num_to_keep is not None:
            # Delete the bottom (N - K) checkpoints
            worst_results = set(
                self._checkpoint_results[: -self._checkpoint_config.num_to_keep]
            )
            # Except for the latest checkpoint.
            results_to_delete = worst_results - {self._latest_checkpoint_result}

            # Update internal state before actually deleting them.
            self._checkpoint_results = [
                checkpoint_result
                for checkpoint_result in self._checkpoint_results
                if checkpoint_result not in results_to_delete
            ]

        # Save the checkpoint manager state to storage.
        # Note: We save the state before deleting the old checkpoints.
        # If deletion happens first and the process crashes, our snapshot
        # may point to some stale checkpoints that are already deleted.
        # TODO: Make this writing operation non-blocking.
        self._write_state_to_storage()

        # Delete the old checkpoints.
        for checkpoint_result in results_to_delete:
            checkpoint = checkpoint_result.checkpoint
            logger.debug("Deleting checkpoint: ", checkpoint)
            _delete_fs_path(fs=checkpoint.filesystem, fs_path=checkpoint.path)

    # --------------------------
    # CheckpointManager state
    # --------------------------

    def _save_state(self) -> str:
        """Save the checkpoint manager state to a JSON str."""

        checkpoint_results = [
            _get_state_from_training_result(checkpoint_result, self._storage_context)
            for checkpoint_result in self._checkpoint_results
        ]

        latest_checkpoint_result = (
            _get_state_from_training_result(
                self._latest_checkpoint_result, self._storage_context
            )
            if self._latest_checkpoint_result is not None
            else None
        )

        manager_snapshot = _CheckpointManagerState(
            checkpoint_results=checkpoint_results,
            latest_checkpoint_result=latest_checkpoint_result,
        )
        return manager_snapshot.model_dump_json()

    def _load_state(self, json_state: str):
        """Load the checkpoint manager state from a JSON str."""
        try:
            manager_snapshot = _CheckpointManagerState.model_validate(
                from_json(json_state)
            )
        except Exception as e:
            raise CheckpointManagerInitializationError(repr(e)) from e
        self._assert_checkpoints_exist()

        self._checkpoint_results = [
            _get_training_result_from_state(
                training_result_state, self._storage_context
            )
            for training_result_state in manager_snapshot.checkpoint_results
        ]

        self._latest_checkpoint_result = (
            _get_training_result_from_state(
                manager_snapshot.latest_checkpoint_result, self._storage_context
            )
            if manager_snapshot.latest_checkpoint_result is not None
            else None
        )

    def _maybe_load_state_from_storage(self):
        """Load the checkpoint manager state from storage.
        If no snapshot is found, start with a clean state.
        """
        if not _exists_at_fs_path(
            fs=self._storage_context.storage_filesystem,
            fs_path=self._storage_context.checkpoint_manager_snapshot_path,
        ):
            logger.debug(
                "No checkpoint manager snapshot found. "
                "No checkpoint will be available via `ray.train.get_checkpoint`, "
                "so training will start from scratch."
            )
            return
        with self._storage_context.storage_filesystem.open_input_stream(
            self._storage_context.checkpoint_manager_snapshot_path
        ) as f:
            logger.info(
                "A run snapshot was found in storage folder at: "
                f"'{self._storage_context.experiment_fs_path}'\n"
                "This snapshot contains a list of checkpoints reported via "
                "`ray.train.report` and will be loaded. "
                "This allows the latest checkpoint found in the snapshot to be "
                "accessible within your training function via "
                "`ray.train.get_checkpoint`.\n"
                "If you meant to start a brand new training job without any "
                "information about previous checkpoints found in this directory, "
                "please configure a new, unique `RunConfig(name)` or delete the "
                f"existing folder at '{self._storage_context.experiment_fs_path}'."
            )
            json_state = f.read().decode("utf-8")
        self._load_state(json_state)

    def _write_state_to_storage(self):
        """Write the checkpoint manager state to storage."""
        checkpoint_manager_snapshot = self._save_state()
        with self._storage_context.storage_filesystem.open_output_stream(
            self._storage_context.checkpoint_manager_snapshot_path
        ) as f:
            f.write(checkpoint_manager_snapshot.encode("utf-8"))

    def _assert_checkpoints_exist(self):
        """Validate the checkpoint manager state.

        This method will validate the checkpoint manager state by checking if
        the checkpoints specified in manager snapshot is compatible with the
        checkpoint folders of the experiment storage filesystem.

        Raises:
            CheckpointManagerInitializationError: If the checkpoint manager snapshot
                is not consistent with the stored checkpoints.
        """
        for checkpoint_result in self._checkpoint_results:
            checkpoint = checkpoint_result.checkpoint
            assert checkpoint is not None
            if not _exists_at_fs_path(
                fs=checkpoint.filesystem, fs_path=checkpoint.path
            ):
                raise CheckpointManagerInitializationError(
                    message=(
                        "The run snapshot contains a reference to a checkpoint "
                        f"that does not exist anymore ({checkpoint}). You are "
                        "running in a corrupted run directory `experiment_fs_path`."
                        "Please configure a new, unique `RunConfig(name)` "
                        "or delete the existing folder at "
                        f"`{self._storage_context.experiment_fs_path}`."
                    )
                )

    # --------------------------
    # ReportCallback
    # --------------------------

    def after_report(
        self, metrics: List[Dict[str, Any]], checkpoint: Optional[Checkpoint]
    ):
        if not checkpoint:
            return

        rank_0_metrics = metrics[0]
        self.register_checkpoint(
            _TrainingResult(checkpoint=checkpoint, metrics=rank_0_metrics)
        )
