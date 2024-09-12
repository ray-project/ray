_DEPRECATION_MESSAGE = (
    "The `ray.tune.integration.keras` module is deprecated in favor of "
    "`ray.train.tensorflow.keras.ReportCheckpointCallback`."
)


class TuneReportCallback:
    """Deprecated.
    Use :class:`ray.train.tensorflow.keras.ReportCheckpointCallback` instead."""

    def __new__(cls, *args, **kwargs):
        raise DeprecationWarning(_DEPRECATION_MESSAGE)


class _TuneCheckpointCallback:
    """Deprecated.
    Use :class:`ray.train.tensorflow.keras.ReportCheckpointCallback` instead."""

    def __new__(cls, *args, **kwargs):
        raise DeprecationWarning(_DEPRECATION_MESSAGE)


class TuneReportCheckpointCallback:
    """Deprecated.
    Use :class:`ray.train.tensorflow.keras.ReportCheckpointCallback` instead."""

    def __new__(cls, *args, **kwargs):
        raise DeprecationWarning(_DEPRECATION_MESSAGE)
