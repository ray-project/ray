import contextlib
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Generator, Optional

if TYPE_CHECKING:
    from ray.data._internal.execution.operators.map_transformer import MapTransformer
    from ray.data._internal.progress.base_progress import BaseProgressBar


_thread_local = threading.local()


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

    # Additional keyword arguments passed to the task.
    kwargs: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def get_current(cls) -> Optional["TaskContext"]:
        """Get the TaskContext for the current thread.
        Returns None if no TaskContext has been set.
        """

        return getattr(_thread_local, "task_context", None)

    @classmethod
    def set_current(cls, context):
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


@contextlib.contextmanager
def create_temporary_task_context(
    task_ctx: TaskContext,
) -> Generator[TaskContext, None, None]:
    """
    Create a temporary TaskContext for the current thread.
    The TaskContext is created and set for the current thread, and is reset after the context is exited.

    Args:
        task_ctx: the TaskContext instance to be registered for the current thread.

    Yields:
        TaskContext: The created TaskContext instance.

    Examples:
        >>> with create_temporary_task_context(TaskContext(100, "test")):
        ...     ctx = TaskContext.get_current()
        ...     print(ctx.task_idx)
        100

        >>> ctx = TaskContext(0, "first")
        >>> TaskContext.set_current(ctx)  # register the first context
        >>> with create_temporary_task_context(TaskContext(1, "second")):
        ...     ctx = TaskContext.get_current()
        ...     print(ctx.op_name)
        second
        >>> ctx = TaskContext.get_current()
        >>> print(ctx.op_name)
        first
    """
    previous_ctx = TaskContext.get_current()
    TaskContext.set_current(task_ctx)
    try:
        yield task_ctx
    finally:
        if previous_ctx is not None:
            TaskContext.set_current(previous_ctx)
        else:
            TaskContext.reset_current()
