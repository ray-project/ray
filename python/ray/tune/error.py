from ray.util.annotations import PublicAPI


@PublicAPI
class TuneError(Exception):
    """General error class raised by ray.tune."""

    pass


class _AbortTrialExecution(TuneError):
    """Error that indicates a trial should not be retried."""

    pass


class _SubCategoryTuneError(TuneError):
    """The more specific TuneError that happens for a certain Tune
    subroutine. For example starting/stopping a trial.
    """

    def __init__(self, traceback_str: str):
        self.traceback_str = traceback_str

    def __str__(self):
        return self.traceback_str


class _TuneStopTrialError(_SubCategoryTuneError):
    """Error that happens when stopping a tune trial."""

    pass


class _TuneStartTrialError(_SubCategoryTuneError):
    """Error that happens when starting a tune trial."""

    pass


class _TuneNoNextExecutorEventError(_SubCategoryTuneError):
    """Error that happens when waiting to get the next event to
    handle from RayTrialExecutor.

    Note: RayTaskError will be raised by itself and will not be using
    this category. This category is for everything else."""

    pass
