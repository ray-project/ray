import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import ray
from ray._common.pydantic_compat import BaseModel
from ray.air.config import CheckpointConfig
from ray.train._checkpoint import Checkpoint
from ray.train._internal.checkpoint_manager import (
    _CheckpointManager,
    _insert_into_sorted_list,
)
from ray.train._internal.session import _TrainingResult
from ray.train.v2._internal.exceptions import CheckpointManagerInitializationError
from ray.train.v2._internal.execution.callback import (
    ReportCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.context import StorageContext
from ray.train.v2._internal.execution.storage import _exists_at_fs_path, delete_fs_path
from ray.train.v2._internal.execution.training_report import _TrainingReport
from ray.train.v2._internal.execution.worker_group import Worker
from ray.train.v2.api.report_config import CheckpointConsistencyMode
from ray.train.v2.api.reported_checkpoint import ReportedCheckpoint
from ray.train.v2.api.validation_config import ValidationTaskConfig

logger = logging.getLogger(__name__)


class _TrainingResultState(BaseModel):
    # Increment version if the schema changes
    version: int = 0
    checkpoint_dir_name: str
    metrics: dict


class _CheckpointManagerState(BaseModel):
    ray_version: str = ray.__version__
    checkpoint_results: List[_TrainingResultState]
    checkpoint_report_indices: List[int]
    latest_checkpoint_result: Optional[_TrainingResultState]
    pending_training_results: List[_TrainingResultState]
    pending_validation_specs: List[Union[bool, ValidationTaskConfig]]
    current_report_index: int


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


class CheckpointManager(_CheckpointManager, ReportCallback, WorkerGroupCallback):
    def __init__(
        self,
        checkpoint_config: CheckpointConfig,
        storage_context: StorageContext,
    ):
        self._storage_context = storage_context
        self._checkpoint_config = checkpoint_config

        # This tracks the number of report calls that have been processed
        # for the current worker group.
        self._current_report_index = 0

        # Map from pending checkpoint to validation.
        self._pending_training_results: Dict[
            Checkpoint, Tuple[_TrainingResult, Union[bool, ValidationTaskConfig]]
        ] = {}

        # Map from checkpoint to report index. Used to order checkpoints.
        self._checkpoint_to_report_index = {}

        self._condition = asyncio.Condition()
        super().__init__(checkpoint_config)
        # If the snapshot is found, the checkpoint manager will restore its state.
        # TODO(xgui): CheckpointManager is used to save or restore the checkpoint manager state.
        # We should sanity check if we should see old state in the storage folder.
        self._maybe_load_state_from_storage()

    def register_checkpoint(
        self,
        training_report: _TrainingReport,
    ):
        """Register new checkpoint and add to bookkeeping.

        This method will register a new checkpoint and add it to the internal
        bookkeeping logic. This means the checkpoint manager will decide if
        this checkpoint should be kept, and if older or worse performing
        checkpoints should be deleted.

        Args:
            training_report: Training report to register.
        """
        checkpoint_result = _TrainingResult(
            checkpoint=training_report.checkpoint,
            metrics=training_report.metrics,
        )
        self._latest_checkpoint_result = checkpoint_result
        self._checkpoint_to_report_index[
            checkpoint_result.checkpoint
        ] = self._current_report_index

        if self._checkpoint_config.checkpoint_score_attribute is not None:
            # If we're ordering by a score, insert the checkpoint
            # so that the list remains sorted.
            _insert_into_sorted_list(
                self._checkpoint_results,
                checkpoint_result,
                key=self._get_checkpoint_score,
                checkpoint_to_report_index=self._checkpoint_to_report_index,
            )
        else:
            # If no metric is provided, just append (ordering by time of registration).
            self._checkpoint_results.append(checkpoint_result)

        if training_report.validation:
            self._pending_training_results[checkpoint_result.checkpoint] = (
                checkpoint_result,
                training_report.validation,
            )

        self._current_report_index += 1

        self._save_state_and_delete_old_checkpoints()

        self._notify()

    def update_checkpoints_with_metrics(
        self, checkpoint_to_metrics: Dict[Checkpoint, Dict[str, Any]]
    ):
        """Update the checkpoints with the metrics."""
        for checkpoint, metrics in checkpoint_to_metrics.items():
            if checkpoint not in self._pending_training_results:
                logger.warning(
                    f"Checkpoint {checkpoint} not found in pending training results. "
                )
                continue
            checkpoint_result, _ = self._pending_training_results[checkpoint]
            checkpoint_result.metrics.update(metrics)
            if checkpoint_result not in self._checkpoint_results:
                raise ValueError(
                    f"Checkpoint {checkpoint} was in pending training results but not "
                    "checkpoint results. "
                )
            self._checkpoint_results.remove(checkpoint_result)
            _insert_into_sorted_list(
                self._checkpoint_results,
                checkpoint_result,
                key=self._get_checkpoint_score,
                checkpoint_to_report_index=self._checkpoint_to_report_index,
            )
            self._pending_training_results.pop(checkpoint)
        self._save_state_and_delete_old_checkpoints()
        self._notify()

    def get_pending_training_results(
        self,
    ) -> Dict[Checkpoint, Tuple[_TrainingResult, Union[bool, ValidationTaskConfig]]]:
        """Get the pending training results which includes their validation specs."""
        return self._pending_training_results

    def _notify(self):
        """Notify condition so all listeners know state has changed."""

        async def async_notify():
            async with self._condition:
                self._condition.notify_all()

        asyncio.create_task(async_notify())

    def _save_state_and_delete_old_checkpoints(self):
        """Delete the old checkpoints."""
        # Get checkpoints to delete
        results_to_delete = set()
        if self._checkpoint_config.num_to_keep is not None:
            # Delete the bottom (N - K) checkpoints
            worst_results = set(
                self._checkpoint_results[: -self._checkpoint_config.num_to_keep]
            )
            # Except for the latest checkpoint and pending checkpoints
            results_to_delete = worst_results - {self._latest_checkpoint_result}
            results_to_delete = results_to_delete - {
                v for v, _ in self._pending_training_results.values()
            }

            # Update internal state before actually deleting them.
            self._checkpoint_results = [
                checkpoint_result
                for checkpoint_result in self._checkpoint_results
                if checkpoint_result not in results_to_delete
            ]
            for checkpoint_result in results_to_delete:
                del self._checkpoint_to_report_index[checkpoint_result.checkpoint]

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
            delete_fs_path(fs=checkpoint.filesystem, fs_path=checkpoint.path)

    # --------------------------
    # CheckpointManager state
    # --------------------------

    def _save_state(self) -> str:
        """Save the checkpoint manager state to a JSON str."""

        checkpoint_results = [
            _get_state_from_training_result(checkpoint_result, self._storage_context)
            for checkpoint_result in self._checkpoint_results
        ]

        checkpoint_report_indices = [
            self._checkpoint_to_report_index[checkpoint_result.checkpoint]
            for checkpoint_result in self._checkpoint_results
        ]

        latest_checkpoint_result = (
            _get_state_from_training_result(
                self._latest_checkpoint_result, self._storage_context
            )
            if self._latest_checkpoint_result is not None
            else None
        )

        pending_training_results = [
            _get_state_from_training_result(v, self._storage_context)
            for v, _ in self._pending_training_results.values()
        ]
        pending_validation_specs = [
            v for _, v in self._pending_training_results.values()
        ]

        manager_snapshot = _CheckpointManagerState(
            checkpoint_results=checkpoint_results,
            checkpoint_report_indices=checkpoint_report_indices,
            latest_checkpoint_result=latest_checkpoint_result,
            pending_training_results=pending_training_results,
            pending_validation_specs=pending_validation_specs,
            current_report_index=self._current_report_index,
        )
        return manager_snapshot.json()

    def _load_state(self, json_state: str):
        """Load the checkpoint manager state from a JSON str."""
        json_dict = None
        try:
            json_dict = json.loads(json_state)
            manager_snapshot = _CheckpointManagerState.parse_obj(json_dict)
        except Exception as e:
            if not json_dict:
                error = e
            elif "ray_version" not in json_dict:
                error = (
                    "You are loading a checkpoint manager snapshot saved with an unknown Ray version "
                    f"but you are running Ray version {ray.__version__}. Please use the same Ray version "
                    "the checkpoint manager snapshot was saved with."
                )
            elif json_dict["ray_version"] != ray.__version__:
                error = (
                    f"You are loading a checkpoint manager snapshot saved with Ray version "
                    f"{json_dict['ray_version']} but you are running Ray version "
                    f"{ray.__version__}. Please use the same Ray version the checkpoint "
                    "manager snapshot was saved with."
                )
            else:
                error = e
            raise CheckpointManagerInitializationError(error) from e

        # Do this so we are using the same checkpoint and trainingresult objects.
        # TODO: consider asserting that every checkpoint has a unique dir name
        checkpoint_dir_name_to_checkpoint_result = {}

        for training_result_state in manager_snapshot.checkpoint_results:
            training_result = _get_training_result_from_state(
                training_result_state, self._storage_context
            )
            checkpoint_dir_name_to_checkpoint_result[
                training_result_state.checkpoint_dir_name
            ] = training_result
            self._checkpoint_results.append(training_result)
        self._assert_checkpoints_exist()

        assert len(self._checkpoint_results) == len(
            manager_snapshot.checkpoint_report_indices
        )
        self._checkpoint_to_report_index = {
            checkpoint_result.checkpoint: report_index
            for checkpoint_result, report_index in zip(
                self._checkpoint_results, manager_snapshot.checkpoint_report_indices
            )
        }

        self._latest_checkpoint_result = (
            checkpoint_dir_name_to_checkpoint_result[
                manager_snapshot.latest_checkpoint_result.checkpoint_dir_name
            ]
            if manager_snapshot.latest_checkpoint_result is not None
            else None
        )

        assert len(manager_snapshot.pending_training_results) == len(
            manager_snapshot.pending_validation_specs
        )
        for training_result_state, validation_spec in zip(
            manager_snapshot.pending_training_results,
            manager_snapshot.pending_validation_specs,
        ):
            training_result = checkpoint_dir_name_to_checkpoint_result[
                training_result_state.checkpoint_dir_name
            ]
            self._pending_training_results[training_result.checkpoint] = (
                training_result,
                validation_spec,
            )

        self._current_report_index = manager_snapshot.current_report_index

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
        self,
        training_report: _TrainingReport,
        metrics: List[Dict[str, Any]],
    ):
        if not training_report.checkpoint:
            self._current_report_index += 1
            self._notify()
            return

        self.register_checkpoint(training_report)

    # --------------------------
    # WorkerGroupCallback
    # --------------------------

    def before_init_train_context(self, workers: List[Worker]) -> Dict[str, List[Any]]:
        latest_checkpoint = (
            self.latest_checkpoint_result.checkpoint
            if self.latest_checkpoint_result
            else None
        )
        train_context_args = {
            "checkpoint": [latest_checkpoint] * len(workers),
            "current_report_index": [self._current_report_index] * len(workers),
        }
        return train_context_args

    # --------------------------------
    # Get all reported checkpoints API
    # --------------------------------

    async def get_all_reported_checkpoints(
        self,
        current_report_index: int,
        consistency_mode: CheckpointConsistencyMode = CheckpointConsistencyMode.VALIDATED,
    ) -> List[ReportedCheckpoint]:
        """Get all the reported checkpoints so far.

        Args:
            current_report_index: The current report index.
            consistency_mode: Read semantics for checkpoint retrieval. Defaults to VALIDATED.

        Returns:
            A list of ReportedCheckpoint objects that represent the checkpoints and
            corresponding metrics reported by the workers.
        """
        if consistency_mode == CheckpointConsistencyMode.COMMITTED:
            async with self._condition:
                await self._condition.wait_for(
                    lambda: self._current_report_index == current_report_index
                )
        elif consistency_mode == CheckpointConsistencyMode.VALIDATED:
            async with self._condition:
                await self._condition.wait_for(
                    lambda: self._current_report_index == current_report_index
                    and not self._pending_training_results
                )
        else:
            raise ValueError(
                f"Unexpected CheckpointConsistencyMode: {consistency_mode}"
            )
        # TODO: might be nice for CheckpointManager to manage ReportedCheckpoint
        # instead of _TrainingResult but that is a large refactor.
        return [
            ReportedCheckpoint(
                checkpoint=tr.checkpoint,
                metrics=tr.metrics,
            )
            for tr in self._checkpoint_results
        ]
