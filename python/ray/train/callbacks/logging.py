from ray.train.callbacks.callback import TrainingCallback, _deprecation_msg
from ray.util.annotations import Deprecated


@Deprecated(message=_deprecation_msg)
class JsonLoggerCallback(TrainingCallback):
    pass


@Deprecated(message=_deprecation_msg)
class MLflowLoggerCallback(TrainingCallback):
    pass


@Deprecated(message=_deprecation_msg)
class TBXLoggerCallback(TrainingCallback):
    pass
