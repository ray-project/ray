from typing import Any, Dict, Optional

from ray.train.v2._internal.execution.train_fn_utils import get_train_fn_utils
from ray.train.v2._internal.util import requires_train_worker
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@requires_train_worker()
def report(metrics: Dict[str, Any], step: Optional[int] = None) -> None:
    """Report health signals from inside the training function.

    Unlike ``ray.train.report``, this is not a barrier and does not need to be
    called the same number of times on every worker. Metrics are merged into
    what this worker reported before, last write wins, and are sent to the
    controller on its next poll.

    Example:

        .. testcode::
            :skipif: True

            import ray.train.health as health

            def train_func(config):
                for step, batch in enumerate(loader):
                    ...
                    health.report({"step_time_s": dt, "grad_norm": gn}, step=step)

    Args:
        metrics: Health signals for this worker.
        step: The training step these signals belong to.
    """
    if not isinstance(metrics, dict):
        raise TypeError(f"metrics must be a dict, got {type(metrics).__name__}.")
    get_train_fn_utils().report_health(metrics, step=step)
