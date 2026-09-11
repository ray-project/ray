import logging
import sys
import typing

import ray
from ray.util.debug import log_once

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology
    from ray.data._internal.progress.base_progress import BaseExecutionProgressManager
    from ray.data.context import DataContext

logger = logging.getLogger(__name__)


# TODO(async-progress A): add a helper that takes a built progress manager and
# returns either it or an async-wrapped version of it, based on the DataContext
# flag added in context.py. Keeping this in one place avoids duplicating the
# decision at each construction site below.
#   - Import the wrapper lazily inside the function, matching how the manager
#     classes are imported below.
#   - IMPORTANT: only the tqdm and rich managers may be wrapped. The Noop and
#     Logging managers do no blocking terminal I/O, so wrapping them would add a
#     background thread for nothing.
#   - Optional refinement to consider once the renderer exists: when the
#     terminal is already wedged, a newly started dataset could be given a Noop
#     manager instead, so the user sees no bar rather than a frozen one.
#     Wrapping is non-blocking either way, so this is UX, not correctness.


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
    worker = ray._private.worker
    in_ray_worker = worker.global_worker.mode == worker.WORKER_MODE

    if not sys.stdout.isatty() and not (use_ray_tqdm and in_ray_worker):
        from ray.data._internal.progress.logging_progress import (
            LoggingExecutionProgressManager,
        )

        if log_once("ray_data_logging_progress_activated"):
            logger.info(
                "Progress will be logged because stdout is a non-interactive terminal."
            )
        return LoggingExecutionProgressManager(
            dataset_id, topology, show_op_progress, verbose_progress
        )

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
        # TODO(async-progress B): assign this to a local instead of returning it
        # directly, then return it through the _maybe_wrap_async helper so the
        # tqdm manager renders on the background thread when the flag is on.
        return TqdmExecutionProgressManager(
            dataset_id, topology, show_op_progress, verbose_progress
        )
    else:
        try:
            from ray.data._internal.progress.rich_progress import (
                RichExecutionProgressManager,
            )

            # TODO(async-progress C): same as the tqdm branch — assign to a local
            # and return it through _maybe_wrap_async. Note the ImportError
            # fallback below returns a Noop manager, which must NOT be wrapped.
            return RichExecutionProgressManager(
                dataset_id, topology, show_op_progress, verbose_progress
            )
        except ImportError:
            from ray.data._internal.progress.base_progress import (
                NoopExecutionProgressManager,
            )

            logger.warning(
                "[dataset]: Run `pip install rich` to enable progress reporting."
            )
            return NoopExecutionProgressManager(
                dataset_id, topology, show_op_progress, verbose_progress
            )
