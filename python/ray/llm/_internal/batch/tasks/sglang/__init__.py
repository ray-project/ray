"""SGLang batch tasks, keyed by task type."""

from typing import Dict, Type

from ray.llm._internal.batch.constants import SGLangTaskType
from ray.llm._internal.batch.tasks.sglang.base import _SGLangTask
from ray.llm._internal.batch.tasks.sglang.generate import _SGLangGenerateTask

_TASKS: Dict[str, Type[_SGLangTask]] = {
    SGLangTaskType.GENERATE: _SGLangGenerateTask,
}


def get_sglang_task_cls(task_type: str) -> Type[_SGLangTask]:
    """Return the task class for `task_type`, or raise if it's unsupported."""
    task_cls = _TASKS.get(task_type)
    if task_cls is None:
        raise ValueError(f"Unsupported task type: {task_type}")
    return task_cls
