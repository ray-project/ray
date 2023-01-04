import logging
from pathlib import Path
from typing import Callable, Dict, List, Optional, Type, Union

from ray.air import Checkpoint, CheckpointConfig
from ray.air._internal.checkpoint_manager import CheckpointStorage
from ray.air._internal.checkpoint_manager import (
    _CheckpointManager as CommonCheckpointManager,
)
from ray.air._internal.checkpoint_manager import _TrackedCheckpoint
from ray.train._internal.session import TrainingResult
from ray.train._internal.utils import construct_path
from ray.train.constants import (
    TIMESTAMP,
    TRAIN_CHECKPOINT_SUBDIR,
    TUNE_CHECKPOINT_ID,
    TUNE_INSTALLED,
    CHECKPOINT_METADATA_KEY,
)

if TUNE_INSTALLED:
    from ray import tune
else:
    tune = None

logger = logging.getLogger(__name__)


def load_checkpoint_from_path(checkpoint_to_load: Union[str, Path]) -> Checkpoint:
    """Utility function to load a checkpoint from a path."""
    checkpoint_path = Path(checkpoint_to_load).expanduser()
    if not checkpoint_path.exists():
        raise ValueError(f"Checkpoint path {checkpoint_path} does not exist.")
    checkpoint = Checkpoint.from_directory(str(checkpoint_path))
    return checkpoint


class CheckpointManager(CommonCheckpointManager):
    """Manages checkpoint processing, writing, and loading.


    - A ``checkpoints`` directory is created in the ``run_dir`` and contains
    all the checkpoint files.

    The full default path will be:

    ~/ray_results/train_<datestring>/run_<run_id>/checkpoints/
    checkpoint_<checkpoint_id>

    Attributes:
        latest_checkpoint_dir: Path to the file directory for
            the checkpoints from the latest run. Configured through
            ``start_training``.
        latest_checkpoint_filename: Filename for the latest
            checkpoint.
        next_checkpoint_path: Path to the next checkpoint to
            persist from the latest run.
        best_checkpoint_path: Path to the best persisted
            checkpoint from the latest run.
        latest_checkpoint_id: The id of the most recently
            saved checkpoint.
        latest_checkpoint: The latest saved checkpoint. This
            checkpoint may not be saved to disk.
    """

    _persist_memory_checkpoints = True

    def __init__(
        self,
        run_dir: Optional[Path] = None,
        checkpoint_strategy: Optional[CheckpointConfig] = None,
    ):
        self.run_dir = run_dir

        super().__init__(checkpoint_strategy=checkpoint_strategy)

        self._validate_checkpoint_strategy()

    def _validate_checkpoint_strategy(self):
        if self._checkpoint_strategy.checkpoint_score_attribute is None:
            self._checkpoint_strategy.checkpoint_score_attribute = TIMESTAMP

    def _load_checkpoint(
        self, checkpoint_to_load: Optional[Union[Dict, str, Path, Checkpoint]]
    ) -> Optional[Checkpoint]:
        """Load the checkpoint dictionary from the input dict or path."""
        if checkpoint_to_load is None:
            return None
        if isinstance(checkpoint_to_load, Dict):
            return Checkpoint.from_dict(checkpoint_to_load)
        if isinstance(checkpoint_to_load, Checkpoint):
            return checkpoint_to_load
        else:
            # Load checkpoint from path.
            return load_checkpoint_from_path(checkpoint_to_load)

    def _process_checkpoint(
        self,
        checkpoint_results: List[TrainingResult],
        decode_checkpoint_fn: Callable,
    ) -> None:
        """Ray Train entrypoint. Perform all processing for a checkpoint."""
        # Get checkpoint from first worker.
        checkpoint_result = checkpoint_results[0]

        checkpoint_data = checkpoint_result.data
        checkpoint_metadata = checkpoint_result.metadata or {}

        if isinstance(checkpoint_data, str):
            checkpoint_class: Type[Checkpoint] = checkpoint_metadata[
                CHECKPOINT_METADATA_KEY
            ].checkpoint_type
            checkpoint_data = checkpoint_class.from_directory(checkpoint_data)
            checkpoint_data._metadata = checkpoint_metadata[CHECKPOINT_METADATA_KEY]
        else:
            # TODO(ml-team): Remove once we remove Backend.decode_data
            checkpoint_data = decode_checkpoint_fn(checkpoint_data)

        score_attr = self._checkpoint_strategy.checkpoint_score_attribute
        if (
            self._checkpoint_strategy.num_to_keep != 0
            and score_attr not in checkpoint_metadata
        ):
            raise ValueError(
                f"Unable to persist checkpoint for "
                f"checkpoint_score_attribute: "
                f"{score_attr}. "
                f"Include this attribute in the call to "
                f"`session.report()`."
            )

        tracked_checkpoint = _TrackedCheckpoint(
            dir_or_data=checkpoint_data,
            checkpoint_id=self._latest_checkpoint_id,
            storage_mode=CheckpointStorage.MEMORY,
            metrics={score_attr: checkpoint_metadata.get(score_attr, 0.0)},
        )
        self.register_checkpoint(checkpoint=tracked_checkpoint)

    def _get_next_checkpoint_path(self) -> Optional[Path]:
        """Path to the next checkpoint to persist."""
        checkpoint_path = _construct_checkpoint_path_name(
            self._latest_checkpoint_id + 1
        )
        return self.latest_checkpoint_dir.joinpath(checkpoint_path)

    def on_start_training(
        self,
        checkpoint_strategy: Optional[CheckpointConfig],
        run_dir: Path,
        latest_checkpoint_id: Optional[int] = 0,
    ):
        checkpoint_strategy = checkpoint_strategy or CheckpointConfig()
        self._checkpoint_strategy = checkpoint_strategy

        self._validate_checkpoint_strategy()

        self.run_dir = run_dir
        self._latest_checkpoint_id = latest_checkpoint_id or 0

    # Train-specific attributes
    @property
    def latest_checkpoint(self):
        if not self._latest_memory_checkpoint:
            return None
        return self._latest_memory_checkpoint.dir_or_data

    @property
    def latest_checkpoint_dir(self) -> Optional[Path]:
        """Path to the latest checkpoint directory."""
        checkpoint_dir = Path(TRAIN_CHECKPOINT_SUBDIR)
        return construct_path(checkpoint_dir, self.run_dir)

    @property
    def latest_checkpoint_file_name(self) -> Optional[str]:
        """Filename to use for the latest checkpoint."""
        if self._latest_checkpoint_id > 0:
            return _construct_checkpoint_path_name(self._latest_checkpoint_id)
        else:
            return None

    @property
    def next_checkpoint_path(self) -> Optional[Path]:
        """Path to the next checkpoint to persist."""
        checkpoint_file = _construct_checkpoint_path_name(
            self._latest_checkpoint_id + 1
        )
        return self.latest_checkpoint_dir.joinpath(checkpoint_file)

    @property
    def best_checkpoint_path(self) -> Optional[Path]:
        """Path to the best persisted checkpoint."""
        if self._best_persisted_checkpoint:
            return Path(self._best_persisted_checkpoint.dir_or_data)
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
        self, checkpoint_to_load: Optional[Union[Dict, str, Path, Checkpoint]]
    ) -> Optional[Union[Dict, Checkpoint]]:
        loaded_checkpoint = super()._load_checkpoint(checkpoint_to_load)
        assert not loaded_checkpoint or isinstance(loaded_checkpoint, Checkpoint)
        # `latest_checkpoint_id` will be the id assigned to the next checkpoint,
        # which should be one more than the loaded checkpoint's id
        # If no checkpoint is loaded, initialize this to 0
        self._latest_checkpoint_id = (
            getattr(loaded_checkpoint, TUNE_CHECKPOINT_ID, -1) + 1
        )
        return loaded_checkpoint

    def add_tune_checkpoint_id(self, checkpoint: Checkpoint):
        # Store the checkpoint_id in the file so that the Tune trial can be
        # resumed after failure or cancellation.
        setattr(checkpoint, TUNE_CHECKPOINT_ID, self._latest_checkpoint_id)

    def _process_persistent_checkpoint(self, checkpoint: _TrackedCheckpoint):
        self.add_tune_checkpoint_id(checkpoint.dir_or_data)
        # If inside a Tune Trainable, then checkpoint with Tune.
        with tune.checkpoint_dir(step=self._latest_checkpoint_id) as checkpoint_dir:
            path = Path(checkpoint_dir)
            checkpoint.commit(path)

        return super()._process_persistent_checkpoint(checkpoint)

    @property
    def latest_checkpoint_dir(self) -> Optional[Path]:
        raise NotImplementedError

    @property
    def next_checkpoint_path(self) -> Optional[Path]:
        return None

    def _get_next_checkpoint_path(self) -> Optional[Path]:
        return None


def _construct_checkpoint_path_name(checkpoint_id: int) -> str:
    return f"checkpoint_{checkpoint_id:06d}"
