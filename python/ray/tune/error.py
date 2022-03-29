import traceback


class TuneError(Exception):
    """General error class raised by ray.tune."""

    pass


class AbortTrialExecution(TuneError):
    """Error that indicates a trial should not be retried."""

    pass


def get_tb_from_exception(e: Exception) -> str:
    """Utility function to get traceback from Exception."""
    return "".join(
        traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
    )
