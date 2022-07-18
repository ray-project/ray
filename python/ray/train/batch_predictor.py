import inspect
from typing import Any, Dict, Optional, List, Type, Union, Callable
import pandas as pd

import ray
from ray.air import Checkpoint
from ray.air.util.data_batch_conversion import convert_batch_type_to_pandas
from ray.train.predictor import Predictor
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
        return cls(checkpoint=checkpoint, predictor_cls=predictor_cls, **kwargs)

    @classmethod
    def from_pandas_udf(
        cls, pandas_udf: Callable[[pd.DataFrame], pd.DataFrame]
    ) -> "BatchPredictor":
        """Create a Predictor from a Pandas UDF.

        Args:
            pandas_udf: A function that takes a pandas.DataFrame and other
                optional kwargs and returns a pandas.DataFrame.
        """

        class PandasUDFPredictor(Predictor):
            @classmethod
            def from_checkpoint(cls, checkpoint, **kwargs):
                return PandasUDFPredictor()

            def _predict_pandas(self, df, **kwargs) -> "pd.DataFrame":
                return pandas_udf(df, **kwargs)

        return cls(
            checkpoint=Checkpoint.from_dict({"dummy": 1}),
            predictor_cls=PandasUDFPredictor,
        )

    def predict(
        self,
        data: Union[ray.data.Dataset, ray.data.DatasetPipeline],
        *,
        feature_columns: Optional[List[str]] = None,
        keep_columns: Optional[List[str]] = None,
        batch_size: int = 4096,
        min_scoring_workers: int = 1,
        max_scoring_workers: Optional[int] = None,
        num_cpus_per_worker: int = 1,
        num_gpus_per_worker: int = 0,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        **predict_kwargs,
    ) -> ray.data.Dataset:
        """Run batch scoring on a Dataset.

        Examples:
            >>> import pandas as pd
            >>> import ray
            >>> from ray.air import Checkpoint
            >>> from ray.train.predictor import Predictor
            >>> from ray.train.batch_predictor import BatchPredictor
            >>> # Create a batch predictor that returns identity as the predictions.
            >>> batch_pred = BatchPredictor.from_pandas_udf( # doctest: +SKIP
            ...     lambda data: data)
            >>> # Create a dummy dataset.
            >>> ds = ray.data.from_pandas(pd.DataFrame({ # doctest: +SKIP
            ...     "feature_1": [1, 2, 3], "label": [1, 2, 3]}))
            >>> # Execute batch prediction using this predictor.
            >>> predictions = batch_pred.predict(ds, # doctest: +SKIP
            ...     feature_columns=["feature_1"], keep_columns=["label"])
            >>> print(predictions) # doctest: +SKIP
            Dataset(num_blocks=1, num_rows=3, schema={a: int64, label: int64})
            >>> # Calculate final accuracy.
            >>> def calculate_accuracy(df):
            ...    return pd.DataFrame({"correct": df["predictions"] == df["label"]})
            >>> correct = predictions.map_batches(calculate_accuracy) # doctest: +SKIP
            >>> print("Final accuracy: ", # doctest: +SKIP
            ...    correct.sum(on="correct") / correct.count())
            Final accuracy: 1.0000

        Args:
            data: Ray dataset or pipeline to run batch prediction on.
            feature_columns: List of columns in data to use for prediction. Columns not
                specified will be dropped from `data` before being passed to the
                predictor. If None, use all columns.
            keep_columns: List of columns in `data` to include in the prediction result.
                This is useful for calculating final accuracies/metrics on the result
                dataset. If None, the columns in the output dataset will contain just
                the prediction results.
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
        # Automatic set use_gpu in predictor constructor if user provided
        # explicit GPU resources
        if (
            "use_gpu" in inspect.signature(predictor_cls.from_checkpoint).parameters
            and num_gpus_per_worker > 0
        ):
            predictor_kwargs["use_gpu"] = True

        class ScoringWrapper:
            def __init__(self):
                checkpoint = Checkpoint.from_object_ref(checkpoint_ref)
                self.predictor = predictor_cls.from_checkpoint(
                    checkpoint, **predictor_kwargs
                )

            def __call__(self, batch):
                if feature_columns:
                    prediction_batch = batch[feature_columns]
                else:
                    prediction_batch = batch
                prediction_output = self.predictor.predict(
                    prediction_batch, **predict_kwargs
                )
                if keep_columns:
                    prediction_output[keep_columns] = batch[keep_columns]
                return convert_batch_type_to_pandas(prediction_output)

        compute = ray.data.ActorPoolStrategy(
            min_size=min_scoring_workers, max_size=max_scoring_workers
        )

        ray_remote_args = ray_remote_args or {}
        ray_remote_args["num_cpus"] = num_cpus_per_worker
        ray_remote_args["num_gpus"] = num_gpus_per_worker

        prediction_results = data.map_batches(
            ScoringWrapper,
            compute=compute,
            batch_format="pandas",
            batch_size=batch_size,
            **ray_remote_args,
        )

        return prediction_results

    def predict_pipelined(
        self,
        data: ray.data.Dataset,
        *,
        blocks_per_window: Optional[int] = None,
        bytes_per_window: Optional[int] = None,
        # The remaining args are from predict().
        feature_columns: Optional[List[str]] = None,
        keep_columns: Optional[List[str]] = None,
        batch_size: int = 4096,
        min_scoring_workers: int = 1,
        max_scoring_workers: Optional[int] = None,
        num_cpus_per_worker: int = 1,
        num_gpus_per_worker: int = 0,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        **predict_kwargs,
    ) -> ray.data.DatasetPipeline:
        """Setup a prediction pipeline for batch scoring.

        Unlike `predict()`, this generates a DatasetPipeline object and does not
        perform execution. Execution can be triggered by pulling from the pipeline.

        This is a convenience wrapper around calling `.window()` on the Dataset prior
        to passing it `BatchPredictor.predict()`.

        Examples:
            >>> import pandas as pd
            >>> import ray
            >>> from ray.air import Checkpoint
            >>> from ray.train.predictor import Predictor
            >>> from ray.train.batch_predictor import BatchPredictor
            >>> # Create a batch predictor that always returns `42` for each input.
            >>> batch_pred = BatchPredictor.from_pandas_udf( # doctest: +SKIP
            ...     lambda data: pd.DataFrame({"a": [42] * len(data)})
            >>> # Create a dummy dataset.
            >>> ds = ray.data.range_tensor(1000, parallelism=4) # doctest: +SKIP
            >>> # Setup a prediction pipeline.
            >>> print(batch_pred.predict_pipelined( # doctest: +SKIP
            ...     ds, blocks_per_window=1))
            DatasetPipeline(num_windows=4, num_stages=3)

        Args:
            data: Ray dataset to run batch prediction on.
            blocks_per_window: The window size (parallelism) in blocks.
                Increasing window size increases pipeline throughput, but also
                increases the latency to initial output, since it decreases the
                length of the pipeline. Setting this to infinity effectively
                disables pipelining.
            bytes_per_window: Specify the window size in bytes instead of blocks.
                This will be treated as an upper bound for the window size, but each
                window will still include at least one block. This is mutually
                exclusive with ``blocks_per_window``.
            feature_columns: List of columns in data to use for prediction. Columns not
                specified will be dropped from `data` before being passed to the
                predictor. If None, use all columns.
            keep_columns: List of columns in `data` to include in the prediction result.
                This is useful for calculating final accuracies/metrics on the result
                dataset. If None, the columns in the output dataset will contain just
                the prediction results.
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
            DatasetPipeline that generates scoring results.
        """

        if blocks_per_window is None and bytes_per_window is None:
            raise ValueError(
                "It is required to specify one of `blocks_per_window` or "
                "`bytes_per_window`."
            )

        pipe = data.window(
            blocks_per_window=blocks_per_window, bytes_per_window=bytes_per_window
        )

        return self.predict(
            pipe,
            batch_size=batch_size,
            feature_columns=feature_columns,
            keep_columns=keep_columns,
            min_scoring_workers=min_scoring_workers,
            max_scoring_workers=max_scoring_workers,
            num_cpus_per_worker=num_cpus_per_worker,
            num_gpus_per_worker=num_gpus_per_worker,
            ray_remote_args=ray_remote_args,
            **predict_kwargs,
        )
