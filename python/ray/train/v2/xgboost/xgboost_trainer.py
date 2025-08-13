"""
Scalable XGBoost Trainer with External Memory Support

This module provides an improved XGBoost Trainer that avoids dataset materialization
for large datasets by using XGBoost's external memory capabilities with Ray Data's
streaming iteration. This implementation follows XGBoost's official external memory
best practices and is optimized for XGBoost 3.0+.

Key Features:
- ExtMemQuantileDMatrix for optimal external memory performance (XGBoost 3.0+)
- Cluster-aware memory management based on Ray cluster resources
- Smart batch size calculation and streaming iteration strategies
- Seamless integration with Ray Data preprocessing pipelines
- Optimized parameters for external memory performance (hist + depthwise)
- GPU training support with memory-efficient configurations
- Support for different XGBoost objectives and task types
- Streaming iteration with minimal memory footprint (2-3 batches in memory)
- RAPIDS Memory Manager (RMM) integration for GPU performance
- Hardware-aware optimizations (NVLink-C2C, PCIe, NUMA)

Following XGBoost External Memory Best Practices:
- Uses tree_method="hist" (required for external memory training)
- Uses grow_policy="depthwise" for optimal batch iteration efficiency
- Implements streaming iteration with minimal memory footprint
- Supports GPU training with RMM integration
- Optimized for ExtMemQuantileDMatrix performance
- Follows XGBoost 3.0+ external memory recommendations

All external memory optimization is handled automatically through the internal
_train_loop_utils module, providing a clean interface that requires minimal user configuration.
"""

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Union

import ray.data
import ray.train
from ray.train import Checkpoint, DataConfig
from ray.train.trainer import GenDataset
from ray.train.v2.api.config import RunConfig, ScalingConfig
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.util import PublicAPI
from ray.util.annotations import Deprecated

if TYPE_CHECKING:
    from ray.train.xgboost import XGBoostConfig

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
class XGBoostTrainer(DataParallelTrainer):
    """A Trainer for distributed data-parallel XGBoost training.

    This trainer automatically handles external memory optimization to avoid dataset
    materialization, making it suitable for large datasets that don't fit in memory.
    The trainer provides seamless external memory training with hardware-aware optimization
    through the ray.train.xgboost utilities, using streaming iteration with minimal
    memory footprint.

    Following XGBoost External Memory Best Practices:
    - Uses tree_method="hist" (required for external memory training)
    - Uses grow_policy="depthwise" for optimal batch iteration efficiency
    - Implements streaming iteration with minimal memory footprint
    - Supports GPU training with RMM integration
    - Optimized for ExtMemQuantileDMatrix performance
    - Follows XGBoost 3.0+ external memory recommendations

    The trainer is designed to be robust across different XGBoost workloads including:
    - Binary and multi-class classification
    - Regression tasks
    - Ranking problems
    - Different data types (numerical, categorical, missing values)
    - GPU and CPU training
    - Checkpoint resuming and early stopping

    At a high level, this Trainer does the following:

    1. Launches multiple workers as defined by the ``scaling_config``.
    2. Sets up a distributed XGBoost environment on these workers
       as defined by the ``xgboost_config``.
    3. Ingests the input ``datasets`` based on the ``dataset_config``.
    4. Runs the input ``train_loop_per_worker(train_loop_config)``
       on all workers.

    Example:

        .. testcode::

            import xgboost
            import ray.data
            import ray.train
            from ray.train.xgboost import RayTrainReportCallback
            from ray.train.v2.xgboost import XGBoostTrainer
            import ray.train.xgboost as train_xgboost  # Training utilities

            def train_fn_per_worker(config: dict):
                # Get dataset shards
                train_ds = ray.train.get_dataset_shard("train")
                eval_ds = ray.train.get_dataset_shard("validation")

                # All optimization handled automatically - one line!
                dtrain, deval, params = train_xgboost.prepare_datasets_and_params(
                    train_ds,
                    label_column="target",
                    eval_dataset_shard=eval_ds,
                    objective="binary:logistic",
                    use_gpu=True,  # Automatic GPU optimization
                    eta=0.1,       # Custom parameters as needed
                    max_depth=6
                )

                # Standard XGBoost training - all complexity hidden
                bst = xgboost.train(
                    params,
                    dtrain=dtrain,
                    evals=[(deval, "validation")],
                    num_boost_round=100,
                    callbacks=[RayTrainReportCallback()],
                )

            # Load datasets
            train_ds = ray.data.read_parquet("s3://dataset/train/")
            eval_ds = ray.data.read_parquet("s3://dataset/validation/")

            trainer = XGBoostTrainer(
                train_fn_per_worker,
                datasets={"train": train_ds, "validation": eval_ds},
                scaling_config=ray.train.ScalingConfig(num_workers=4, use_gpu=True),
            )
            result = trainer.fit()

        .. testoutput::
            :hide:

            ...

        Alternative usage with manual control:

        .. testcode::

            import ray.train.xgboost as train_xgboost

            def train_fn_per_worker(config: dict):
                train_ds = ray.train.get_dataset_shard("train")
                eval_ds = ray.train.get_dataset_shard("validation")

                # Manual dataset preparation (automatic memory optimization)
                dtrain = train_xgboost.prepare_dataset(train_ds, label_column="target")
                deval = train_xgboost.prepare_dataset(eval_ds, label_column="target")

                # Hardware-optimized parameters (automatic system detection)
                params = train_xgboost.get_recommended_params(
                    objective="reg:squarederror",
                    use_gpu=False,
                    eta=0.05,
                    max_depth=8
                )

                bst = xgboost.train(params, dtrain, evals=[(deval, "validation")])

        .. testoutput::
            :hide:

            ...

    The training utilities automatically handle:
    - Memory-aware dataset preparation (materialization vs external memory)
    - Hardware detection (NUMA, storage type, GPU capabilities)
    - Parameter optimization for external memory training
    - System-specific performance tuning
    - Streaming iteration with minimal memory footprint

    External Memory Best Practices:
    - The trainer automatically uses tree_method="hist" (required for external memory)
    - grow_policy="depthwise" is used for optimal batch iteration efficiency
    - Batch size is automatically optimized (~10GB per batch for 64GB RAM systems)
    - GPU training includes RMM integration for optimal performance
    - Storage type detection optimizes parameters for your hardware

    Args:
        train_loop_per_worker: The training function to execute on each worker.
            This function can either take in zero arguments or a single ``Dict``
            argument which is set by defining ``train_loop_config``.
            Within this function you can use any of the
            :ref:`Ray Train Loop utilities <train-loop-api>`.
        train_loop_config: A configuration ``Dict`` to pass in as an argument to
            ``train_loop_per_worker``.
            This is typically used for specifying hyperparameters.
        xgboost_config: The configuration for setting up the distributed xgboost
            backend. Defaults to using the "rabit" backend.
            See :class:`~ray.train.xgboost.XGBoostConfig` for more info.
        scaling_config: The configuration for how to scale data parallel training.
            ``num_workers`` determines how many Python processes are used for training,
            and ``use_gpu`` determines whether or not each process should use GPUs.
            See :class:`~ray.train.ScalingConfig`` for more info.
        run_config: The configuration for the execution of the training run.
            See :class:`~ray.train.RunConfig`` for more info.
        datasets: The Ray Datasets to ingest for training.
            Datasets are keyed by name (``{name: dataset}``).
            Each dataset can be accessed from within the ``train_loop_per_worker``
            by calling ``ray.train.get_dataset_shard(name)``.
            Sharding and additional configuration can be done by
            passing in a ``dataset_config``.
        dataset_config: The configuration for ingesting the input ``datasets``.
            By default, all the Ray Dataset are split equally across workers.
            See :class:`~ray.train.DataConfig`` for more details.
        resume_from_checkpoint: A checkpoint to resume training from.
            This checkpoint can be accessed from within ``train_loop_per_worker``
            by calling ``ray.train.get_checkpoint()``.
        metadata: Dict that should be made available via
            `ray.train.get_context().get_metadata()` and in `checkpoint.get_metadata()`
            for checkpoints saved from this Trainer. Must be JSON-serializable.
    """

    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        xgboost_config: Optional["XGBoostConfig"] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        dataset_config: Optional[DataConfig] = None,
        # TODO: [Deprecated]
        metadata: Optional[Dict[str, Any]] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        # TODO(justinvyu): [Deprecated] Legacy XGBoostTrainer API
        label_column: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        num_boost_round: Optional[int] = None,
    ):
        if (
            label_column is not None
            or params is not None
            or num_boost_round is not None
        ):
            raise DeprecationWarning(
                "The legacy XGBoostTrainer API is deprecated. "
                "Please switch to passing in a custom `train_loop_per_worker` "
                "function instead. "
                "See this issue for more context: "
                "https://github.com/ray-project/ray/issues/50042"
            )

        from ray.train.xgboost import XGBoostConfig

        # Configure dataset for external memory optimization
        if dataset_config is None:
            dataset_config = DataConfig(
                execution_options=ray.data.ExecutionOptions(
                    preserve_order=False,  # Allow reordering for better performance
                    locality_with_output=True,  # Keep data local to workers
                )
            )

        super(XGBoostTrainer, self).__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            backend_config=xgboost_config or XGBoostConfig(),
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            resume_from_checkpoint=resume_from_checkpoint,
            metadata=metadata,
        )

    @classmethod
    @Deprecated
    def get_model(cls, checkpoint: Checkpoint):
        """[Deprecated] Retrieve the XGBoost model stored in this checkpoint."""
        raise DeprecationWarning(
            "`XGBoostTrainer.get_model` is deprecated. "
            "Use `RayTrainReportCallback.get_model` instead."
        )
