from ray.train.xgboost import (  # noqa: F401
    RayTrainReportCallback as TuneReportCheckpointCallback,
)
from ray.util.annotations import Deprecated


@Deprecated
class TuneReportCallback:
    def __new__(cls: type, *args, **kwargs):
        # TODO(justinvyu): [code_removal] Remove in 2.11.
        raise DeprecationWarning(
            "`TuneReportCallback` is deprecated. "
            "Use `ray.tune.integration.xgboost.TuneReportCheckpointCallback` instead."
        )
