import abc
from typing import Any, Dict, Optional, Type, Union, TYPE_CHECKING

import ray
from ray.ml.checkpoint import Checkpoint

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd

DataBatchType = Union["pd.DataFrame", "np.ndarray"]


class PredictorNotSerializableException(RuntimeError):
    """Error raised when trying to serialize a Predictor instance."""

    pass


class Predictor(abc.ABC):
    """Predictors load models from checkpoints to perform inference."""

    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint, **kwargs) -> "Predictor":
        """Create a specific predictor from a checkpoint.

        Args:
            checkpoint: Checkpoint to load predictor data from.
            kwargs: Arguments specific to predictor implementations.

        Returns:
            Predictor: Predictor object.
        """
        raise NotImplementedError

    def predict(self, data: DataBatchType, **kwargs) -> DataBatchType:
        """Perform inference on a batch of data.

        Args:
            data: A batch of input data. Either a pandas Dataframe or numpy
                array.
            kwargs: Arguments specific to predictor implementations.

        Returns:
            DataBatchType: Prediction result.
        """
        raise NotImplementedError

    def __reduce__(self):
        raise PredictorNotSerializableException(
            "Predictor instances are not serializable. Instead, you may want "
            "to serialize a checkpoint and initialize the Predictor with "
            "Predictor.from_checkpoint."
        )


class BatchPredictor(Predictor):
    """Batch predictor class.

    Takes a predictor class and a checkpoint and provides an interface to run
    batch scoring on Ray datasets.

    This batch predictor wraps around a predictor class and executes it
    in a distributed way when calling ``predict()``.

    Attributes:
        checkpoint: Checkpoint loaded by the distributed predictor objects.
        predictor_cls: Predictor class reference. When scoring, each scoring worker
            will create an instance of this class and call ``predict(batch)`` on it.
        **predictor_kwargs: Keyword arguments passed to the predictor on
            initialization.

    """

    def __init__(
        self, checkpoint: Checkpoint, predictor_cls: Type[Predictor], **predictor_kwargs
    ):
        # Store as object ref so we only serialize it once for all map workers
        self.checkpoint_ref = checkpoint.to_object_ref()
        self.predictor_cls = predictor_cls
        self.predictor_kwargs = predictor_kwargs

    @classmethod
    def from_checkpoint(
        cls, checkpoint: Checkpoint, predictor_cls: Type[Predictor], **kwargs
    ) -> "BatchPredictor":
        return BatchPredictor(
            checkpoint=checkpoint, predictor_cls=predictor_cls, **kwargs
        )

    def predict(
        self,
        data: ray.data.Dataset,
        *,
        batch_size: int = 4096,
        max_scoring_actors: Optional[int] = None,
        num_gpus: int = 0,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        **predict_kwargs
    ) -> ray.data.Dataset:
        """Run batch scoring on dataset.

        Args:
            data: Ray dataset to run batch prediction on.
            batch_size: Split dataset into batches of this size for prediction.
            max_scoring_actors: If set, specify the maximum number of scoring actors.
            num_gpus: Number of GPUs to allocate per scoring worker.
            ray_remote_args: Additional resource requirements to request from
                ray.
            predict_kwargs: Keyword arguments passed to the predictor's
                ``predict()`` method.

        Returns:
            Dataset containing scoring results.

        """
        predictor_cls = self.predictor_cls
        checkpoint_ref = self.checkpoint_ref
        predictor_kwargs = self.predictor_kwargs

        class ScoringWrapper:
            def __init__(self):
                self.predictor = predictor_cls.from_checkpoint(
                    Checkpoint.from_object_ref(checkpoint_ref), **predictor_kwargs
                )

            def __call__(self, batch):
                return self.predictor.predict(batch, **predict_kwargs)

        compute = ray.data.ActorPoolStrategy(min_size=1, max_size=max_scoring_actors)

        ray_remote_args = ray_remote_args or {}
        ray_remote_args["num_gpus"] = num_gpus

        return data.map_batches(
            ScoringWrapper,
            compute=compute,
            batch_format="pandas",
            batch_size=batch_size,
            **ray_remote_args
        )
