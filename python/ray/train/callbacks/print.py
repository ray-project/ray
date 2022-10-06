from ray.train.callbacks.callback import TrainingCallback, _deprecation_msg
from ray.util.annotations import Deprecated


@Deprecated(message=_deprecation_msg)
class PrintCallback(TrainingCallback):
    pass
