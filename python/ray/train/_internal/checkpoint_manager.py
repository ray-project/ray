import logging
import numbers
from typing import Any, Callable, List, Optional, Tuple

from ray._private.dict import flatten_dict
from ray.air.config import MAX
from ray.air._internal.util import is_nan
from ray.train import CheckpointConfig
from ray.train._internal.storage import _delete_fs_path
from ray.train._internal.session import _TrainingResult


logger = logging.getLogger(__name__)


def _insert_into_sorted_list(list: List[Any], item: Any, key: Callable[[Any], Any]):
    """Insert an item into a sorted list with a custom key function.

    Examples:

        >>> list = []
        >>> _insert_into_sorted_list(list, {"a": 1, "b": 0}, lambda x: x["a"])
        >>> list
        [{'a': 1, 'b': 0}]
        >>> _insert_into_sorted_list(list, {"a": 3, "b": 1}, lambda x: x["a"])
        >>> list
        [{'a': 1, 'b': 0}, {'a': 3, 'b': 1}]
        >>> _insert_into_sorted_list(list, {"a": 4, "b": 2}, lambda x: x["a"])
        >>> list
        [{'a': 1, 'b': 0}, {'a': 3, 'b': 1}, {'a': 4, 'b': 2}]
        >>> _insert_into_sorted_list(list, {"a": 1, "b": 3}, lambda x: x["a"])
        >>> list
        [{'a': 1, 'b': 0}, {'a': 1, 'b': 3}, {'a': 3, 'b': 1}, {'a': 4, 'b': 2}]
    """
    i = 0
    while i < len(list):
        # Insert to the right of all duplicates.
        if key(list[i]) > key(item):
            break
        i += 1
    list.insert(i, item)


class _CheckpointManager:
    """Checkpoint manager that handles checkpoint book-keeping for a trial.

    The main purpose of this abstraction is to keep the top K checkpoints based on
    recency/a user-provided metric.

    NOTE: This class interacts with `_TrainingResult` objects, which are
    (checkpoint, metrics) pairs. This is to order checkpoints by metrics.

    Args:
        checkpoint_config: Defines how many and which checkpoints to keep.
    """

    def __init__(self, checkpoint_config: Optional[CheckpointConfig]):
        self._checkpoint_config = checkpoint_config or CheckpointConfig()

        # List of checkpoints ordered by ascending score.
        self._checkpoint_results: List[_TrainingResult] = []

        # The latest registered checkpoint.
        # This should never be immediately deleted upon registration,
        # even if it's not in the top K checkpoints, based on score.
        self._latest_checkpoint_result: Optional[_TrainingResult] = None

        if (
            self._checkpoint_config.num_to_keep is not None
            and self._checkpoint_config.num_to_keep <= 0
        ):
            raise ValueError(
                f"`num_to_keep` must >= 1, got: "
                f"{self._checkpoint_config.num_to_keep}"
            )

    @property
    def checkpoint_config(self):
        return self._checkpoint_config

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

            for checkpoint_result in results_to_delete:
                checkpoint = checkpoint_result.checkpoint
                logger.debug("Deleting checkpoint: ", checkpoint)
                _delete_fs_path(fs=checkpoint.filesystem, fs_path=checkpoint.path)

    def _get_checkpoint_score(
        self, checkpoint: _TrainingResult
    ) -> Tuple[bool, numbers.Number]:
        """Get the score for a checkpoint, according to checkpoint config.

        If `mode="min"`, the metric is negated so that the lowest score is
        treated as the best.

        Returns:
            Tuple: A tuple of (not_is_nan: bool, score: numbers.Number).
                This score orders: nan values < float("-inf") < valid numeric metrics
        """
        checkpoint_score_attribute = self._checkpoint_config.checkpoint_score_attribute
        if checkpoint_score_attribute:
            flat_metrics = flatten_dict(checkpoint.metrics)
            try:
                checkpoint_result = flat_metrics[checkpoint_score_attribute]
            except KeyError:
                valid_keys = list(flat_metrics.keys())
                logger.error(
                    f"Result dict has no key: {checkpoint_score_attribute}. "
                    f"checkpoint_score_attr must be set to a key in the "
                    f"result dict. Valid keys are: {valid_keys}"
                )
                checkpoint_result = float("-inf")
        else:
            checkpoint_result = float("-inf")

        checkpoint_score_order = self._checkpoint_config.checkpoint_score_order
        order_factor = 1.0 if checkpoint_score_order == MAX else -1.0

        checkpoint_score = order_factor * checkpoint_result

        if not isinstance(checkpoint_score, numbers.Number):
            raise ValueError(
                f"Unable to persist checkpoint for "
                f"checkpoint_score_attribute: "
                f"{checkpoint_score_attribute} with value "
                f"{checkpoint_score}. "
                f"This attribute must be numerical."
            )

        return (
            (not is_nan(checkpoint_score), checkpoint_score)
            if not is_nan(checkpoint_score)
            else (False, float("-inf"))
        )

    @property
    def best_checkpoint_result(self) -> Optional[_TrainingResult]:
        return self._checkpoint_results[-1] if self._checkpoint_results else None

    @property
    def latest_checkpoint_result(self) -> Optional[_TrainingResult]:
        return self._latest_checkpoint_result

    @property
    def best_checkpoint_results(self) -> List[_TrainingResult]:
        if self._checkpoint_config.num_to_keep is None:
            return self._checkpoint_results
        return self._checkpoint_results[-self._checkpoint_config.num_to_keep :]
