import logging
import typing

from ray.util.debug import log_once

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.progress.base_progress import BaseExecutionProgressManager
    from ray.data.context import DataContext

logger = logging.getLogger(__name__)


def truncate_operator_name(name: str, max_name_length: int) -> str:
    from ray.data.context import DataContext

    ctx = DataContext.get_current()
    if not ctx.enable_progress_bar_name_truncation or len(name) <= max_name_length:
        return name

    op_names = name.split("->")
    if len(op_names) == 1:
        return op_names[0]

    # Include as many operators as possible without approximately
    # exceeding `MAX_NAME_LENGTH`. Always include the first and
    # last operator names so it is easy to identify the DAG.
    truncated_op_names = [op_names[0]]
    for op_name in op_names[1:-1]:
        if (
            len("->".join(truncated_op_names))
            + len("->")
            + len(op_name)
            + len("->")
            + len(op_names[-1])
        ) > max_name_length:
            truncated_op_names.append("...")
            if log_once("ray_data_truncate_operator_name"):
                logger.warning(
                    f"Truncating long operator name to {max_name_length} "
                    "characters. To disable this behavior, set "
                    "`ray.data.DataContext.get_current()."
                    "DEFAULT_ENABLE_PROGRESS_BAR_NAME_TRUNCATION = False`."
                )
            break
        truncated_op_names.append(op_name)
    truncated_op_names.append(op_names[-1])
    return "->".join(truncated_op_names)


def get_progress_manager(
    ctx: "DataContext", dataset_id: str, topology: "Topology"
) -> "BaseExecutionProgressManager":
    """Obtain the appropriate progress manager for the current
    DataContext"""
    from ray.data._internal.progress.base_progress import NoopExecutionProgressManager

    show_op_progress = ctx.enable_operator_progress_bars
    if not show_op_progress:
        if log_once("ray_data_progress_manager_global"):
            logger.warning(
                "Progress bars for operators disabled. To enable, "
                "set `ray.data.DataContext.get_current()."
                "enable_operator_progress_bars = True`."
            )
    if not ctx.enable_progress_bars:
        if log_once("ray_data_progress_manager_disabled"):
            logger.warning(
                "Progress bars disabled. To enable, set "
                "`ray.data.DataContext.get_current()."
                "enable_progress_bars = True`."
            )
        return NoopExecutionProgressManager(dataset_id, topology, show_op_progress)

    rich_enabled = ctx.enable_rich_progress_bars
    use_ray_tqdm = ctx.use_ray_tqdm

    if not rich_enabled or use_ray_tqdm:
        from ray.data._internal.progress.tqdm_progress import (
            TqdmExecutionProgressManager,
        )

        if log_once("ray_data_rich_progress_disabled"):
            logger.info(
                "[dataset]: A new progress UI is available. To enable, "
                "set `ray.data.DataContext.get_current()."
                "enable_rich_progress_bars = True` and `ray.data."
                "DataContext.get_current().use_ray_tqdm = False`."
            )
        return TqdmExecutionProgressManager(dataset_id, topology, show_op_progress)
    else:
        try:
            from ray.data._internal.progress.rich_progress import (
                RichExecutionProgressManager,
            )

            return RichExecutionProgressManager(dataset_id, topology, show_op_progress)
        except ImportError:
            print("[dataset]: Run `pip install rich` to enable progress reporting.")
            return NoopExecutionProgressManager(dataset_id, topology, show_op_progress)
