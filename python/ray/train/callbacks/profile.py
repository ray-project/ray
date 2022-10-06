from ray.train.callbacks.callback import TrainingCallback, _deprecation_msg
from ray.util.annotations import Deprecated


@Deprecated(message=_deprecation_msg)
class TorchTensorboardProfilerCallback(TrainingCallback):
    pass
