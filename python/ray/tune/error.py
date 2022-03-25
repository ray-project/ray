class TuneError(Exception):
    """General error class raised by ray.tune."""

    pass


class AbortTrialExecution(TuneError):
    """Error that indicates a trial should not be retried."""

    pass
