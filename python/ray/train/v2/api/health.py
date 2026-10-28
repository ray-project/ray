from typing import Dict, Optional

from ray.train.v2._internal.util import requires_train_worker
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@requires_train_worker()
def report(metrics: Dict[str, float], step: Optional[int] = None) -> None:
    """Report health metrics from the training function.

    Unlike ``ray.train.report``, this is not a barrier: each worker can call it
    any number of times. Each call replaces the worker's previous report, and
    the controller reads the latest one on its next poll of the worker.

    Example:

        .. testcode::
            :skipif: True

            import ray.train.health as health

            def train_func(config):
                for step, batch in enumerate(loader):
                    ...
                    health.report({"grad_norm": grad_norm}, step=step)

    Args:
        metrics: Numeric metrics by name.
        step: The training step the metrics belong to, if any.
    """
    # Avoid a circular import: Ray Train internals import ray.train.health.
    from ray.train.v2._internal.execution.train_fn_utils import get_train_fn_utils

    get_train_fn_utils().report_health(dict(metrics), step)
