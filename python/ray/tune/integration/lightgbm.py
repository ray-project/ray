from ray.util.annotations import Deprecated

from ray.train.lightgbm import RayTrainReportCallback as TuneReportCheckpointCallback

# If a user imports from this package, then the class module should line up.
TuneReportCheckpointCallback.__module__ = "ray.tune.integration.lightgbm"


@Deprecated
class TuneReportCallback:
    def __new__(cls: type, *args, **kwargs):
        # TODO(justinvyu): [code_removal] Remove in 2.11.
        raise DeprecationWarning(
            "`TuneReportCallback` is deprecated. "
            "Use `ray.tune.integration.lightgbm.TuneReportCheckpointCallback` instead."
        )
