import datetime
import math
import time
from typing import Union

from ray import logger
from ray.tune.stopper.stopper import Stopper
from ray.util.annotations import PublicAPI


@PublicAPI
class TimeoutStopper(Stopper):
    """Stops all trials after a certain timeout.

    This stopper is automatically created when the `time_budget_s`
    argument is passed to `tune.RunConfig()`.

    Args:
        timeout: Either a non-negative finite number specifying the timeout
            in seconds, or a non-negative `datetime.timedelta` object.
    """

    def __init__(self, timeout: Union[int, float, datetime.timedelta]):
        from datetime import timedelta

        if isinstance(timeout, timedelta):
            self._timeout_seconds = timeout.total_seconds()
        elif isinstance(timeout, (int, float)) and not isinstance(timeout, bool):
            self._timeout_seconds = timeout
        else:
            raise ValueError(
                "`timeout` parameter has to be either a number or a "
                "`datetime.timedelta` object. Found: {}".format(type(timeout))
            )

        if not math.isfinite(self._timeout_seconds):
            raise ValueError(
                f"`timeout` must be a finite number of seconds, got {timeout}."
            )
        if self._timeout_seconds < 0:
            raise ValueError(
                f"`timeout` must be non-negative, got {self._timeout_seconds} seconds."
            )

        self._budget = self._timeout_seconds

        # To account for setup overhead, set the last check time only after
        # the first call to `stop_all()`.
        self._last_check = None

    def __call__(self, trial_id, result):
        return False

    def stop_all(self):
        now = time.time()

        if self._last_check is not None:
            taken = max(0.0, now - self._last_check)
            self._budget -= taken

        self._last_check = now

        if self._budget <= 0:
            logger.info(
                f"Reached timeout of {self._timeout_seconds} seconds. "
                f"Stopping all trials."
            )
            return True

        return False

    def __setstate__(self, state: dict):
        state["_last_check"] = None
        self.__dict__.update(state)
