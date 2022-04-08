from typing import Type, Optional, Dict, Any

import ray

from ray.ml import Checkpoint
from ray.ml.predictor import Predictor


class BatchScorer:
    """Batch scoring class.

    Takes a predictor class and a checkpoint and provides an interface to run
    batch scoring on Ray datasets.

    Args:
        predictor_cls: Predictor class reference. When scoring, each scoring worker
            will create an instance of this class an call ``predict(batch)`` on it.
        checkpoint: Checkpoint object used by the predictor.
        **predictor_kwargs: Keyword arguments passed to the predictor on
            initialization.

    """

    def __init__(
        self, predictor_cls: Type[Predictor], checkpoint: Checkpoint, **predictor_kwargs
    ):
        self.predictor_cls = predictor_cls
        # Store as object ref so we only serialize it once for all map workers
        self.checkpoint_ref = checkpoint.to_object_ref()
        self.predictor_kwargs = predictor_kwargs

    def score(
        self,
        dataset: ray.data.Dataset,
        *,
        batch_size: int = 4096,
        max_scoring_actors: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        **predict_kwargs
    ) -> ray.data.Dataset:
        """Run batch scoring on dataset.

        Args:
            dataset: Ray dataset to run batch scoring on.
            batch_size: Split dataset into batches of this size for prediction.
            max_scoring_actors: If set, specify the maximum number of scoring actors.
            ray_remote_args: Additional resource requirements to request from
                ray (e.g., num_gpus=1 to request GPUs for the map tasks).
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

        return dataset.map_batches(
            ScoringWrapper,
            compute=compute,
            batch_format="pandas",
            batch_size=batch_size,
            **ray_remote_args
        )
