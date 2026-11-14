import contextlib
import dataclasses
import threading
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.operators.map_transformer import MapTransformer
    from ray.data._internal.progress.base_progress import BaseProgressBar


_thread_local = threading.local()


@dataclass(frozen=True)
class RerunPolicy:
    """The errors on which a map task runs again after it raises.

    A rerun produces the task's blocks from the start, so a task emits the rows
    it buffered before an error the job skips only when no rerun follows. The
    default reruns on any error: a task emits partial output only when its
    operator states how Ray retries it.
    """

    # Whether Ray reruns the task on any error, and otherwise the exception
    # types it reruns on, matched against the error and its causes.
    ray_reruns_any_error: bool = True
    ray_retried_types: Tuple[Type[BaseException], ...] = ()

    # How many more attempts ``_map_task`` makes, and the ``retried_map_errors``
    # patterns it makes them on. ``None`` matches any error, as for
    # ``iterate_with_retry``.
    map_attempts_left: int = 0
    map_retried_patterns: Optional[Tuple[str, ...]] = ()

    @classmethod
    def from_retry_exceptions(
        cls, retry_exceptions: Union[bool, Sequence[Type[BaseException]]]
    ) -> "RerunPolicy":
        """The policy of a task Ray reruns on ``retry_exceptions``.

        ``retry_exceptions`` takes the form of the remote arg: ``True`` for any
        error, ``False`` for none, or a list of exception types.
        """
        if isinstance(retry_exceptions, bool):
            return cls(ray_reruns_any_error=retry_exceptions)
        return cls(
            ray_reruns_any_error=False, ray_retried_types=tuple(retry_exceptions)
        )

    def with_map_retries(
        self, attempts_left: int, patterns: Optional[Sequence[str]]
    ) -> "RerunPolicy":
        """This policy for an attempt of ``_map_task`` with ``attempts_left``."""
        return dataclasses.replace(
            self,
            map_attempts_left=attempts_left,
            map_retried_patterns=None if patterns is None else tuple(patterns),
        )

    def may_rerun(self, error: Exception) -> bool:
        """Whether Ray or ``_map_task`` runs the task again after ``error``."""
        return self._ray_reruns_on(error) or self._map_task_reruns_on(error)

    def _ray_reruns_on(self, error: Exception) -> bool:
        if self.ray_reruns_any_error:
            return True
        # A transform may wrap the error, so its causes count too.
        cause: Optional[BaseException] = error
        while cause is not None:
            if isinstance(cause, self.ray_retried_types):
                return True
            cause = cause.__cause__
        return False

    def _map_task_reruns_on(self, error: Exception) -> bool:
        # Matched as ``_map_task`` matches it: the message with its causes.
        from ray._common.retry import format_exception
        from ray.data._internal.util import is_retryable_error

        if self.map_attempts_left <= 0:
            return False
        error_str = format_exception(error, include_cause=True)
        return is_retryable_error(error_str, self.map_retried_patterns)


@dataclass
class TaskContext:
    """This describes the information of a task running block transform."""

    # The index of task. Each task has a unique task index within the same
    # operator.
    task_idx: int

    # Name of the operator that this task belongs to.
    op_name: str

    # The dictionary of sub progress bar to update. The key is name of sub progress
    # bar. Note this is only used on driver side.
    # TODO(chengsu): clean it up from TaskContext with new optimizer framework.
    sub_progress_bar_dict: Optional[Dict[str, "BaseProgressBar"]] = None

    # NOTE(hchen): `upstream_map_transformer` and `upstream_map_ray_remote_args`
    # are only used for `RandomShuffle`. DO NOT use them for other operators.
    # Ideally, they should be handled by the optimizer, and should be transparent
    # to the specific operators.
    # But for `RandomShuffle`, the AllToAllOperator doesn't do the shuffle itself.
    # It uses `ExchangeTaskScheduler` to launch new tasks to do the shuffle.
    # That's why we need to pass them to `ExchangeTaskScheduler`.
    # TODO(hchen): Use a physical operator to do the shuffle directly.

    # The underlying function called in a MapOperator; this is used when fusing
    # an AllToAllOperator with an upstream MapOperator.
    upstream_map_transformer: Optional["MapTransformer"] = None

    # The Ray remote arguments of the fused upstream MapOperator.
    # This should be set if upstream_map_transformer is set.
    upstream_map_ray_remote_args: Optional[Dict[str, Any]] = None

    # Override of the target max-block-size for the task
    target_max_block_size_override: Optional[int] = None

    # The errors on which this task runs again. A map operator sets Ray's part
    # and ``_map_task`` its own part for each attempt.
    rerun_policy: RerunPolicy = field(default_factory=RerunPolicy)

    # Additional keyword arguments passed to the task.
    kwargs: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def get_current(cls) -> Optional["TaskContext"]:
        """Get the TaskContext for the current thread.
        Returns None if no TaskContext has been set.
        """

        return getattr(_thread_local, "task_context", None)

    @classmethod
    def set_current(cls, context: "TaskContext") -> None:
        """Set the TaskContext for the current thread.

        Args:
            context: The TaskContext instance to set for this thread
        """

        _thread_local.task_context = context

    @classmethod
    def reset_current(cls):
        """Clear the current thread's TaskContext."""

        if hasattr(_thread_local, "task_context"):
            delattr(_thread_local, "task_context")

    @classmethod
    @contextlib.contextmanager
    def current(cls, context: "TaskContext") -> Iterator["TaskContext"]:
        """Sets this TaskContext as current for the scope
        of the context block and resets it on exit.
        """
        cls.set_current(context)
        try:
            yield context
        finally:
            cls.reset_current()
