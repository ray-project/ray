import datetime
from typing import Union
import time

from ray import logger
from ray.util.annotations import PublicAPI
from ray.tune.stopper.stopper import Stopper


@PublicAPI
class TimeoutStopper(Stopper):
    """Stops all trials after a certain timeout.

    This stopper is automatically created when the `time_budget_s`
    argument is passed to `air.RunConfig()`.

    Args:
        timeout: Either a number specifying the timeout in seconds, or
            a `datetime.timedelta` object.
    """

    def __init__(self, timeout: Union[int, float, datetime.timedelta]):
        from datetime import timedelta

        if isinstance(timeout, timedelta):
            self._timeout_seconds = timeout.total_seconds()
        elif isinstance(timeout, (int, float)):
            self._timeout_seconds = timeout
        else:
            raise ValueError(
                "`timeout` parameter has to be either a number or a "
                "`datetime.timedelta` object. Found: {}".format(type(timeout))
            )

        self._budget = self._timeout_seconds

        # To account for setup overhead, set the last check time only after
        # the first call to `stop_all()`.
        self._last_check = None

    def __call__(self, trial_id, result):
        return False

    def stop_all(self):
        now = time.time()

        if self._last_check:
            taken = now - self._last_check
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
