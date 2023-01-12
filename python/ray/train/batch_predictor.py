import inspect
import logging
from typing import Any, Dict, Optional, List, Type, Union, Callable
import pandas as pd
import numpy as np

import ray
from ray.air import Checkpoint
from ray.air.data_batch_type import DataBatchType
from ray.air.util.data_batch_conversion import BatchFormat, BlockFormat
from ray.data import Preprocessor
from ray.data.context import DatasetContext
from ray.train.predictor import Predictor
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
class BatchPredictor:
    """Batch predictor class.

    Takes a predictor class and a checkpoint and provides an interface to run
    batch scoring on Ray datasets.

    This batch predictor wraps around a predictor class and executes it
    in a distributed way when calling ``predict()``.
    """

    def __init__(
        self, checkpoint: Checkpoint, predictor_cls: Type[Predictor], **predictor_kwargs
    ):
        self._checkpoint = checkpoint
        # Store as object ref so we only serialize it once for all map workers
        self._checkpoint_ref = ray.put(checkpoint)
        self._predictor_cls = predictor_cls
        self._predictor_kwargs = predictor_kwargs
        self._override_preprocessor: Optional[Preprocessor] = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(checkpoint={self._checkpoint}, "
            f"predictor_cls={self._predictor_cls.__name__})"
        )

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

    def get_preprocessor(self) -> Preprocessor:
        """Get the preprocessor to use prior to executing predictions."""
        if self._override_preprocessor:
            return self._override_preprocessor

        return self._checkpoint.get_preprocessor()

    def set_preprocessor(self, preprocessor: Preprocessor) -> None:
        """Set the preprocessor to use prior to executing predictions."""
        self._override_preprocessor = preprocessor

    def predict(
        self,
        data: Union[ray.data.Dataset, ray.data.DatasetPipeline],
        *,
        feature_columns: Optional[List[str]] = None,
        keep_columns: Optional[List[str]] = None,
        batch_size: int = 4096,
        min_scoring_workers: int = 1,
        max_scoring_workers: Optional[int] = None,
        num_cpus_per_worker: Optional[int] = None,
        num_gpus_per_worker: Optional[int] = None,
        separate_gpu_stage: bool = True,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        **predict_kwargs,
    ) -> Union[ray.data.Dataset, ray.data.DatasetPipeline]:
        """Run batch scoring on a Dataset.

        Args:
            data: Ray dataset or pipeline to run batch prediction on.
            feature_columns: List of columns in the preprocessed dataset to use for
                prediction. Columns not specified will be dropped
                from `data` before being passed to the predictor.
                If None, use all columns in the preprocessed dataset.
            keep_columns: List of columns in the preprocessed dataset to include
                in the prediction result. This is useful for calculating final
                accuracies/metrics on the result dataset. If None,
                the columns in the output dataset will contain
                just the prediction results.
            batch_size: Split dataset into batches of this size for prediction.
            min_scoring_workers: Minimum number of scoring actors.
            max_scoring_workers: If set, specify the maximum number of scoring actors.
            num_cpus_per_worker: Number of CPUs to allocate per scoring worker.
            num_gpus_per_worker: Number of GPUs to allocate per scoring worker.
            separate_gpu_stage: If using GPUs, specifies whether to execute GPU
                processing in a separate stage (enabled by default). This avoids
                running expensive preprocessing steps on GPU workers.
            ray_remote_args: Additional resource requirements to request from
                ray.
            predict_kwargs: Keyword arguments passed to the predictor's
                ``predict()`` method.

        Returns:
            Dataset containing scoring results.

        Examples:

            .. testcode::

                import pandas as pd
                import ray
                from ray.train.batch_predictor import BatchPredictor

                def calculate_accuracy(df):
                    return pd.DataFrame({"correct": df["preds"] == df["label"]})

                # Create a batch predictor that returns identity as the predictions.
                batch_pred = BatchPredictor.from_pandas_udf(
                    lambda data: pd.DataFrame({"preds": data["feature_1"]}))

                # Create a dummy dataset.
                ds = ray.data.from_pandas(pd.DataFrame({
                    "feature_1": [1, 2, 3], "label": [1, 2, 3]}))

                # Execute batch prediction using this predictor.
                predictions = batch_pred.predict(ds,
                    feature_columns=["feature_1"], keep_columns=["label"])

                # print predictions and calculate final accuracy
                print(predictions)
                correct = predictions.map_batches(calculate_accuracy)
                print(f"Final accuracy: {correct.sum(on='correct') / correct.count()}")

            .. testoutput::

                Dataset(num_blocks=1, num_rows=3, schema={preds: int64, label: int64})
                Final accuracy: 1.0
        """
        if num_gpus_per_worker is None:
            num_gpus_per_worker = 0
        if num_cpus_per_worker is None:
            if num_gpus_per_worker > 0:
                # Don't request a CPU here, to avoid unnecessary contention. The GPU
                # resource request suffices for scheduling.
                num_cpus_per_worker = 0
            else:
                num_cpus_per_worker = 1

        predictor_cls = self._predictor_cls
        checkpoint_ref = self._checkpoint_ref

        # Automatic set use_gpu in predictor constructor if user provided
        # explicit GPU resources
        if (
            "use_gpu" in inspect.signature(predictor_cls.from_checkpoint).parameters
            and "use_gpu" not in self._predictor_kwargs
            and num_gpus_per_worker > 0
        ):
            logger.info(
                "`num_gpus_per_worker` is set for `BatchPreditor`."
                "Automatically enabling GPU prediction for this predictor. To "
                "disable set `use_gpu` to `False` in `BatchPredictor.predict`."
            )
            self._predictor_kwargs["use_gpu"] = True

        predictor_kwargs_ref = ray.put(self._predictor_kwargs)

        # In case of [arrow block] -> [X] -> [Pandas UDF] -> [Y] -> [TorchPredictor]
        # We have two places where we can chose data format with less conversion cost.
        # This is the [X], between data block and first preprocessor.
        preprocessor_batch_format: BatchFormat = (
            self._determine_preprocessor_batch_format(data)
        )
        # This is the [Y] in case of separated GPU stage prediction
        predict_stage_batch_format: BatchFormat = (
            self._predictor_cls._batch_format_to_use()
        )
        ctx = DatasetContext.get_current()
        cast_tensor_columns = ctx.enable_tensor_extension_casting

        class ScoringWrapper:
            def __init__(self, override_prep: Preprocessor = None):
                checkpoint = ray.get(checkpoint_ref)
                predictor_kwargs = ray.get(predictor_kwargs_ref)
                self._predictor = predictor_cls.from_checkpoint(
                    checkpoint, **predictor_kwargs
                )
                if cast_tensor_columns:
                    # Enable automatic tensor column casting at UDF boundaries.
                    self._predictor._set_cast_tensor_columns()

                # We want preprocessing to happen before feature column selection.
                # So we manually apply preprocessing in BatchPredictor rather
                # than in Predictor.
                self.override_prep = override_prep
                self._predictor.set_preprocessor(None)

            def _select_columns_from_input_batch(
                self,
                batch_data: DataBatchType,
                select_columns: Optional[List[str]] = None,
            ):
                """Return a subset of input batch based on provided columns."""
                # No select columns specified, use all columns.
                if not select_columns:
                    return batch_data
                elif isinstance(batch_data, np.ndarray):
                    raise ValueError(
                        f"Column name(s) {select_columns} should not be provided "
                        "for prediction input data type of ``numpy.ndarray``"
                    )
                elif isinstance(batch_data, dict):
                    return {k: v for k, v in batch_data.items() if k in select_columns}
                elif isinstance(batch_data, pd.DataFrame):
                    # Select a subset of the pandas columns.
                    return batch_data[select_columns]

            def _keep_columns_from_input_batch(
                self,
                input_batch: DataBatchType,
                prediction_output_batch: DataBatchType,
                keep_columns: Optional[List[str]] = None,
            ):
                """Return a union of input batch and prediction output batch
                based on provided columns.
                """
                if not keep_columns:
                    return prediction_output_batch
                elif isinstance(input_batch, np.ndarray):
                    raise ValueError(
                        f"Column name(s) {keep_columns} should not be provided "
                        "for prediction input data type of ``numpy.ndarray``"
                    )
                elif isinstance(input_batch, dict):
                    for column in keep_columns:
                        prediction_output_batch[column] = input_batch[column]
                    return prediction_output_batch
                elif isinstance(input_batch, pd.DataFrame):
                    prediction_output_batch[keep_columns] = input_batch[keep_columns]
                    return prediction_output_batch

            def __call__(self, input_batch: DataBatchType) -> DataBatchType:
                # TODO: Delegate separate_gpu_stage flag to Datasets.
                if self.override_prep:
                    # Apply preprocessing before selecting feature columns.
                    input_batch = self.override_prep.transform_batch(input_batch)

                # TODO (jiaodong): Investigate if there's room to optimize prediction
                # result joins to minimize GPU <> CPU transfer
                prediction_batch: DataBatchType = self._select_columns_from_input_batch(
                    input_batch, select_columns=feature_columns
                )

                prediction_output_batch: DataBatchType = self._predictor.predict(
                    prediction_batch, **predict_kwargs
                )
                prediction_output_batch: DataBatchType = (
                    self._keep_columns_from_input_batch(
                        input_batch, prediction_output_batch, keep_columns=keep_columns
                    )
                )

                return prediction_output_batch

        compute = ray.data.ActorPoolStrategy(
            min_size=min_scoring_workers, max_size=max_scoring_workers
        )

        ray_remote_args = ray_remote_args or {}
        ray_remote_args["num_cpus"] = num_cpus_per_worker
        ray_remote_args["num_gpus"] = num_gpus_per_worker

        preprocessor = self.get_preprocessor()
        if preprocessor:
            # TODO: Delegate separate_gpu_stage flag to Datasets.
            if not separate_gpu_stage and num_gpus_per_worker > 0:
                override_prep = preprocessor
            else:
                # In batch prediction, preprocessing is always done in a separate stage.
                # We should not in-line it with prediction, unless separate_gpu_stage is
                # False.
                # Dataset optimizer will fuse preprocessing+prediction stage as
                # necessary.
                override_prep = None

                # TODO: Use preprocessor.transform here.
                # preprocessor.transform will break for DatasetPipeline as it
                # does not support _dataset_format()
                batch_fn = preprocessor._transform_batch

                # Dataset is lazy by default so this map_batches
                # will not trigger execution.
                data = data.map_batches(
                    batch_fn, batch_format=predict_stage_batch_format
                )

        prediction_results = data.map_batches(
            ScoringWrapper,
            compute=compute,
            batch_format=preprocessor_batch_format
            if self.get_preprocessor() is not None
            else predict_stage_batch_format,
            batch_size=batch_size,
            fn_constructor_kwargs={"override_prep": override_prep},
            **ray_remote_args,
        )

        if isinstance(prediction_results, ray.data.Dataset):
            # Force execution because Dataset uses lazy execution by default.
            prediction_results.fully_executed()

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
        num_cpus_per_worker: Optional[int] = None,
        num_gpus_per_worker: Optional[int] = None,
        separate_gpu_stage: bool = True,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        **predict_kwargs,
    ) -> ray.data.DatasetPipeline:
        """Setup a prediction pipeline for batch scoring.

        Unlike `predict()`, this generates a DatasetPipeline object and does not
        perform execution. Execution can be triggered by pulling from the pipeline.

        This is a convenience wrapper around calling `.window()` on the Dataset prior
        to passing it `BatchPredictor.predict()`.

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
            separate_gpu_stage: If using GPUs, specifies whether to execute GPU
                processing in a separate stage (enabled by default). This avoids
                running expensive preprocessing steps on GPU workers.
            ray_remote_args: Additional resource requirements to request from
                ray.
            predict_kwargs: Keyword arguments passed to the predictor's
                ``predict()`` method.

        Returns:
            DatasetPipeline that generates scoring results.

        Examples:

             .. testcode::

                import pandas as pd
                import ray
                from ray.train.batch_predictor import BatchPredictor

                # Create a batch predictor that always returns `42` for each input.
                batch_pred = BatchPredictor.from_pandas_udf(
                    lambda data: pd.DataFrame({"a": [42] * len(data)}))

                # Create a dummy dataset.
                ds = ray.data.range_tensor(1000, parallelism=4)

                # Setup a prediction pipeline.
                print(batch_pred.predict_pipelined(ds, blocks_per_window=1))

             .. testoutput::

                DatasetPipeline(num_windows=4, num_stages=3)
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
            separate_gpu_stage=separate_gpu_stage,
            ray_remote_args=ray_remote_args,
            **predict_kwargs,
        )

    def _determine_preprocessor_batch_format(
        self, ds: Union[ray.data.Dataset, ray.data.DatasetPipeline]
    ) -> BatchFormat:
        """Determine batch format we use for the first preprocessor.

        In case of [arrow block] -> [X] -> [Pandas/Numpy UDF] -> [Predictor]
        We choose the best X based on dataset block format and preprocessor's
        transform type to avoid unnecessary data conversion.

        Args:
            ds (Union[ray.data.Dataset, ray.data.DatasetPipeline]): Input
                dataset or dataset pipeline.

        Returns:
            BatchFormat: Batch format to use for the preprocessor.
        """
        preprocessor = self.get_preprocessor()
        dataset_block_format = ds.dataset_format()
        if dataset_block_format == BlockFormat.SIMPLE:
            # Naive case that we cast to pandas for compatibility.
            # TODO: Revisit
            return BatchFormat.PANDAS

        if preprocessor is None:
            # No preprocessor, just use the predictor format.
            return self._predictor_cls._batch_format_to_use()
        # Code dealing with Chain preprocessor is in Chain._determine_transform_to_use

        # Use same batch format as first preprocessor to minimize data copies.
        return preprocessor._determine_transform_to_use(dataset_block_format)
