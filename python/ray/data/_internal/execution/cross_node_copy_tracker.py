import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from ray.data._internal.execution.interfaces.common import NodeIdStr

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle

logger = logging.getLogger(__name__)


@dataclass
class _TaskCopyInfo:
    input_blocks: List[Tuple[Optional[NodeIdStr], int]]
    task_node: Optional[NodeIdStr] = None
    cross_node_bytes: int = 0


class CrossNodeCopyTracker:
    """Estimates live cross-node object store copies incurred by task execution.

    When a downstream task runs on a different node than its input blocks,
    Ray Core copies the input data to the task's node. These copies persist
    until the task finishes and unpins its inputs.

    Usage from the executor:
      1. Call ``on_task_submitted`` when a task is dispatched with its input.
      2. Call ``on_task_output`` on every task output (only the first per task
         is used to learn the execution node).
      3. Call ``on_task_finished`` when the task completes to release accounting.
    """

    def __init__(self):
        self._active_tasks: Dict[Tuple[int, int], _TaskCopyInfo] = {}
        self._bytes_per_node: Dict[NodeIdStr, int] = defaultdict(int)
        self._total_bytes: int = 0
        self._peak_bytes: int = 0
        self._cumulative_tasks_with_copies: int = 0
        self._cumulative_tasks_total: int = 0

    def on_task_submitted(
        self,
        op: "PhysicalOperator",
        task_index: int,
        input_bundle: "RefBundle",
    ) -> None:
        key = (id(op), task_index)
        input_blocks: List[Tuple[Optional[NodeIdStr], int]] = []
        for entry in input_bundle.blocks:
            node_id = (
                entry.metadata.exec_stats.node_id if entry.metadata.exec_stats else None
            )
            input_blocks.append((node_id, entry.metadata.size_bytes))
        self._active_tasks[key] = _TaskCopyInfo(input_blocks=input_blocks)
        self._cumulative_tasks_total += 1

    def on_task_output(
        self,
        op: "PhysicalOperator",
        task_index: int,
        output_bundle: "RefBundle",
    ) -> None:
        key = (id(op), task_index)
        info = self._active_tasks.get(key)
        if info is None or info.task_node is not None:
            return

        task_node: Optional[NodeIdStr] = None
        for entry in output_bundle.blocks:
            if entry.metadata.exec_stats and entry.metadata.exec_stats.node_id:
                task_node = entry.metadata.exec_stats.node_id
                break
        if task_node is None:
            return

        info.task_node = task_node
        cross_node_bytes = 0
        for source_node, size_bytes in info.input_blocks:
            if source_node is not None and source_node != task_node:
                cross_node_bytes += size_bytes

        info.cross_node_bytes = cross_node_bytes
        if cross_node_bytes > 0:
            self._bytes_per_node[task_node] += cross_node_bytes
            self._total_bytes += cross_node_bytes
            self._peak_bytes = max(self._peak_bytes, self._total_bytes)
            self._cumulative_tasks_with_copies += 1

    def on_task_finished(
        self,
        op: "PhysicalOperator",
        task_index: int,
    ) -> None:
        key = (id(op), task_index)
        info = self._active_tasks.pop(key, None)
        if info is None:
            return
        if info.cross_node_bytes > 0 and info.task_node is not None:
            self._bytes_per_node[info.task_node] -= info.cross_node_bytes
            self._total_bytes -= info.cross_node_bytes
            if self._bytes_per_node[info.task_node] <= 0:
                del self._bytes_per_node[info.task_node]

    @property
    def total_bytes(self) -> int:
        return self._total_bytes

    @property
    def bytes_per_node(self) -> Dict[NodeIdStr, int]:
        return dict(self._bytes_per_node)

    @property
    def peak_bytes(self) -> int:
        return self._peak_bytes

    def summary(self) -> str:
        from ray.data._internal.execution.util import memory_string

        if self._total_bytes == 0 and self._peak_bytes == 0:
            return "Cross-node copies: 0 B"
        parts = [f"Cross-node copies: {memory_string(self._total_bytes)} active"]
        parts.append(f"{memory_string(self._peak_bytes)} peak")
        parts.append(
            f"{self._cumulative_tasks_with_copies}/{self._cumulative_tasks_total}"
            " tasks incurred copies"
        )
        if self._bytes_per_node:
            node_parts = []
            for node_id, nbytes in sorted(
                self._bytes_per_node.items(), key=lambda x: -x[1]
            ):
                short_id = node_id[:8] if len(node_id) > 8 else node_id
                node_parts.append(f"{short_id}={memory_string(nbytes)}")
            parts.append(f"by node: [{', '.join(node_parts)}]")
        return ", ".join(parts)
