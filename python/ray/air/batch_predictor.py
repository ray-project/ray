from typing import Type, Optional, Dict, Any

import ray
from ray.air import Checkpoint
from ray.air.predictor import Predictor
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class BatchPredictor:
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
        min_scoring_workers: int = 1,
        max_scoring_workers: Optional[int] = None,
        num_cpus_per_worker: int = 1,
        num_gpus_per_worker: int = 0,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        **predict_kwargs,
    ) -> ray.data.Dataset:
        """Run batch scoring on dataset.

        Args:
            data: Ray dataset to run batch prediction on.
            batch_size: Split dataset into batches of this size for prediction.
            min_scoring_workers: Minimum number of scoring actors.
            max_scoring_workers: If set, specify the maximum number of scoring actors.
            num_cpus_per_worker: Number of CPUs to allocate per scoring worker.
            num_gpus_per_worker: Number of GPUs to allocate per scoring worker.
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
                checkpoint = Checkpoint.from_object_ref(checkpoint_ref)
                self.predictor = predictor_cls.from_checkpoint(
                    checkpoint, **predictor_kwargs
                )

            def __call__(self, batch):
                return self.predictor.predict(batch, **predict_kwargs)

        compute = ray.data.ActorPoolStrategy(
            min_size=min_scoring_workers, max_size=max_scoring_workers
        )

        ray_remote_args = ray_remote_args or {}
        ray_remote_args["num_cpus"] = num_cpus_per_worker
        ray_remote_args["num_gpus"] = num_gpus_per_worker

        return data.map_batches(
            ScoringWrapper,
            compute=compute,
            batch_format="pandas",
            batch_size=batch_size,
            **ray_remote_args,
        )
