import logging
from typing import Type

from ray.air import Checkpoint
from ray.train.predictor import Predictor
from ray.util.annotations import Deprecated

logger = logging.getLogger(__name__)

BATCH_PREDICTION_DEPRECATION_MSG = (
    "`BatchPredictor` is deprecated from Ray 2.6. Use "
    "`Dataset.map_batches` instead for offline batch inference. "
    "For a migration guide, see "
    "https://github.com/ray-project/ray/issues/37489. "
    "To learn more about batch inference with Ray Data, see "
    "http://batchinference.io."
)


@Deprecated(message=BATCH_PREDICTION_DEPRECATION_MSG)
class BatchPredictor:
    """Batch predictor class.

    Takes a predictor class and a checkpoint and provides an interface to run
    batch scoring on Datasets.

    This batch predictor wraps around a predictor class and executes it
    in a distributed way when calling ``predict()``.
    """

    def __init__(
        self, checkpoint: Checkpoint, predictor_cls: Type[Predictor], **predictor_kwargs
    ):
        raise DeprecationWarning(
            BATCH_PREDICTION_DEPRECATION_MSG,
        )
