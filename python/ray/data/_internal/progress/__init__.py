import logging
import typing

from ray.util.debug import log_once

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.progress.base_progress import BaseExecutionProgressManager
    from ray.data.context import DataContext

logger = logging.getLogger(__name__)


def get_progress_manager(
    ctx: "DataContext", dataset_id: str, topology: "Topology", verbose_progress: bool
) -> "BaseExecutionProgressManager":
    """Obtain the appropriate progress manager for the given DataContext."""
    show_op_progress = ctx.enable_operator_progress_bars

    if not ctx.enable_progress_bars:
        from ray.data._internal.progress.base_progress import (
            NoopExecutionProgressManager,
        )

        if log_once("ray_data_progress_manager_disabled"):
            logger.warning(
                "Progress bars disabled. To enable, set "
                "`ray.data.DataContext.get_current()."
                "enable_progress_bars = True`."
            )
        return NoopExecutionProgressManager(
            dataset_id, topology, show_op_progress, verbose_progress
        )

    if not show_op_progress:
        if log_once("ray_data_progress_manager_global"):
            logger.warning(
                "Progress bars for operators disabled. To enable, "
                "set `ray.data.DataContext.get_current()."
                "enable_operator_progress_bars = True`."
            )

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
        return TqdmExecutionProgressManager(
            dataset_id, topology, show_op_progress, verbose_progress
        )
    else:
        try:
            from ray.data._internal.progress.rich_progress import (
                RichExecutionProgressManager,
            )

            return RichExecutionProgressManager(
                dataset_id, topology, show_op_progress, verbose_progress
            )
        except ImportError:
            from ray.data._internal.progress.base_progress import (
                NoopExecutionProgressManager,
            )

            print("[dataset]: Run `pip install rich` to enable progress reporting.")
            return NoopExecutionProgressManager(
                dataset_id, topology, show_op_progress, verbose_progress
            )
