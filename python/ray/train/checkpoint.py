import logging
from pathlib import Path
from typing import List, Optional, Dict, Union, Callable

from ray import cloudpickle
from ray.train.constants import TUNE_CHECKPOINT_FILE_NAME, TUNE_CHECKPOINT_ID
from ray.train.constants import TUNE_INSTALLED, TRAIN_CHECKPOINT_SUBDIR
from ray.train.session import TrainingResult
from ray.train.utils import construct_path
from ray.util.ml_utils.checkpoint_manager import (
    CheckpointManager as CommonCheckpointManager,
    _TrackedCheckpoint,
    CheckpointStrategy,
)

if TUNE_INSTALLED:
    from ray import tune
else:
    tune = None

logger = logging.getLogger(__name__)


def load_checkpoint_from_path(checkpoint_to_load: Union[str, Path]) -> Dict:
    """Utility function to load a checkpoint Dict from a path."""
    checkpoint_path = Path(checkpoint_to_load).expanduser()
    if not checkpoint_path.exists():
        raise ValueError(f"Checkpoint path {checkpoint_path} does not exist.")
    with checkpoint_path.open("rb") as f:
        return cloudpickle.load(f)


class _NotYetPersistedCheckpoint(_TrackedCheckpoint):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._committed = False

    def commit(self, path: Optional[Path] = None):
        if self._committed:
            return

        assert path

        # Get or create checkpoint dir.
        path.parent.mkdir(parents=True, exist_ok=True)
        # Write checkpoint to disk.
        with path.open("wb") as f:
            cloudpickle.dump(self, f)
            logger.debug(f"Checkpoint successfully written to: {path}")

        self._committed = True

    def delete(self):
        if not self._committed:
            return
        return super().delete()

    @classmethod
    def from_tracked_checkpoint(cls, checkpoint: _TrackedCheckpoint):
        new_checkpoint = cls(
            **{**checkpoint.__dict__, "storage_mode": _TrackedCheckpoint.PERSISTENT}
        )
        return new_checkpoint


class CheckpointManager(CommonCheckpointManager):
    """Manages checkpoint processing, writing, and loading.


    - A ``checkpoints`` directory is created in the ``run_dir`` and contains
    all the checkpoint files.

    The full default path will be:

    ~/ray_results/train_<datestring>/run_<run_id>/checkpoints/
    checkpoint_<checkpoint_id>

    Attributes:
        latest_checkpoint_dir (Optional[Path]): Path to the file directory for
            the checkpoints from the latest run. Configured through
            ``start_training``.
        latest_checkpoint_filename (Optional[str]): Filename for the latest
            checkpoint.
        next_checkpoint_path (Optional[Path]): Path to the next checkpoint to
            persist from the latest run.
        best_checkpoint_path (Optional[Path]): Path to the best persisted
            checkpoint from the latest run.
        latest_checkpoint_id (Optional[int]): The id of the most recently
            saved checkpoint.
        latest_checkpoint (Optional[Dict]): The latest saved checkpoint. This
            checkpoint may not be saved to disk.
    """

    def __init__(self, run_dir: Path, checkpoint_strategy: CheckpointStrategy):
        self.run_dir = run_dir

        super().__init__(checkpoint_strategy=checkpoint_strategy)

    def _load_checkpoint(
        self, checkpoint_to_load: Optional[Union[Dict, str, Path]]
    ) -> Optional[Dict]:
        """Load the checkpoint dictionary from the input dict or path."""
        if checkpoint_to_load is None:
            return None
        if isinstance(checkpoint_to_load, Dict):
            return checkpoint_to_load
        else:
            # Load checkpoint from path.
            return load_checkpoint_from_path(checkpoint_to_load)

    def _process_checkpoint(
        self,
        checkpoint_results: List[TrainingResult],
        decode_checkpoint_fn: Callable,
    ) -> None:
        """Perform all processing for a checkpoint."""

        # Get checkpoint from first worker.
        checkpoint_data = checkpoint_results[0].data

        # Decode checkpoint.
        checkpoint_data = decode_checkpoint_fn(checkpoint_data)

        score_attr = self._checkpoint_strategy.checkpoint_score_attribute
        if (
            self._checkpoint_strategy.num_to_keep != 0
            and score_attr not in checkpoint_data
        ):
            raise ValueError(
                f"Unable to persist checkpoint for "
                f"checkpoint_score_attribute: "
                f"{score_attr}. "
                f"Include this attribute in the call to "
                f"train.save_checkpoint."
            )

        tracked_checkpoint = _TrackedCheckpoint(
            checkpoint_dir_or_data=checkpoint_data,
            checkpoint_id=self._latest_checkpoint_id,
            storage_mode=_TrackedCheckpoint.MEMORY,
            result={score_attr: checkpoint_data.get(score_attr, 0.0)},
        )

        # Always update the latest memory checkpoint
        self._replace_latest_memory_checkpoint(tracked_checkpoint)

        # Only process further if we consider keeping this checkpoint on disk
        if self._checkpoint_strategy.num_to_keep != 0:
            not_yet_persisted_checkpoint = (
                _NotYetPersistedCheckpoint.from_tracked_checkpoint(tracked_checkpoint)
            )
            self._decide_what_to_do_with_checkpoint(not_yet_persisted_checkpoint)

    def _get_next_checkpoint_path(self) -> Optional[Path]:
        """Path to the next checkpoint to persist."""
        checkpoint_file = construct_checkpoint_file_name(self._latest_checkpoint_id + 1)
        return self.latest_checkpoint_dir.joinpath(checkpoint_file)

    # Train-specific attributes
    @property
    def latest_checkpoint_dir(self) -> Optional[Path]:
        """Path to the latest checkpoint directory."""
        checkpoint_dir = Path(TRAIN_CHECKPOINT_SUBDIR)
        return construct_path(checkpoint_dir, self.run_dir)

    @property
    def latest_checkpoint_file_name(self) -> Optional[str]:
        """Filename to use for the latest checkpoint."""
        if self._latest_checkpoint_id > 0:
            return construct_checkpoint_file_name(self._latest_checkpoint_id)
        else:
            return None

    @property
    def next_checkpoint_path(self) -> Optional[Path]:
        """Path to the next checkpoint to persist."""
        checkpoint_file = construct_checkpoint_file_name(self._latest_checkpoint_id + 1)
        return self.latest_checkpoint_dir.joinpath(checkpoint_file)

    @property
    def best_checkpoint_path(self) -> Optional[Path]:
        """Path to the best persisted checkpoint."""
        if self._best_persisted_checkpoint:
            return Path(self._best_persisted_checkpoint.checkpoint_dir_or_data)
        else:
            return None

    @property
    def latest_checkpoint_id(self) -> Optional[int]:
        """The checkpoint id of most recently saved checkpoint.

        If no checkpoint has been saved yet, then return None.
        """
        checkpoint_id = self._latest_checkpoint_id
        if checkpoint_id == 0:
            return None
        else:
            return checkpoint_id


class TuneCheckpointManager(CheckpointManager):
    def _load_checkpoint(
        self, checkpoint_to_load: Optional[Union[Dict, str, Path]]
    ) -> Optional[Dict]:
        loaded_checkpoint = super()._load_checkpoint(checkpoint_to_load)
        if loaded_checkpoint is not None:
            # If the Tune trial is restarted, a new Trainer is instantiated.
            # However, we want the checkpoint_id to continue incrementing
            # from the previous run.
            self._latest_checkpoint_id = loaded_checkpoint[TUNE_CHECKPOINT_ID]
        return loaded_checkpoint

    def add_tune_checkpoint_id(self, checkpoint: Dict):
        # Store the checkpoint_id in the file so that the Tune trial can be
        # resumed after failure or cancellation.
        checkpoint[TUNE_CHECKPOINT_ID] = self._latest_checkpoint_id

    def _decide_what_to_do_with_checkpoint(
        self, checkpoint: _NotYetPersistedCheckpoint
    ):
        self.add_tune_checkpoint_id(checkpoint.checkpoint_dir_or_data)
        # If inside a Tune Trainable, then checkpoint with Tune.
        with tune.checkpoint_dir(step=self._latest_checkpoint_id) as checkpoint_dir:
            path = Path(checkpoint_dir)
            # Use a standard file name so that we know which file to load
            # the checkpoint from.
            file_path = path.joinpath(TUNE_CHECKPOINT_FILE_NAME)
            with file_path.open("wb") as f:
                cloudpickle.dump(checkpoint, f)
        checkpoint._committed = True


def construct_checkpoint_file_name(checkpoint_id: int) -> str:
    return f"checkpoint_{checkpoint_id:06d}"
