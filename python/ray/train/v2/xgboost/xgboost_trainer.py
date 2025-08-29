import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Union, List

import ray.train
from ray.train import Checkpoint
from ray.train.trainer import GenDataset
from ray.train.v2.api.config import RunConfig, ScalingConfig
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.util.annotations import Deprecated

if TYPE_CHECKING:
    import xgboost
    from ray.train.xgboost import XGBoostConfig

logger = logging.getLogger(__name__)


class XGBoostTrainer(DataParallelTrainer):
    """A Trainer for distributed data-parallel XGBoost training.

    Example
    -------

    .. testcode::

        import xgboost

        import ray.data
        import ray.train
        from ray.train.xgboost import RayTrainReportCallback
        from ray.train.xgboost import XGBoostTrainer

        def train_fn_per_worker(config: dict):
            # (Optional) Add logic to resume training state from a checkpoint.
            # ray.train.get_checkpoint()

            # 1. Get the dataset shard for the worker and convert to a `xgboost.DMatrix`
            train_ds_iter, eval_ds_iter = (
                ray.train.get_dataset_shard("train"),
                ray.train.get_dataset_shard("validation"),
            )
            train_ds, eval_ds = train_ds_iter.materialize(), eval_ds_iter.materialize()

            train_df, eval_df = train_ds.to_pandas(), eval_ds.to_pandas()
            train_X, train_y = train_df.drop("y", axis=1), train_df["y"]
            eval_X, eval_y = eval_df.drop("y", axis=1), eval_df["y"]

            dtrain = xgboost.DMatrix(train_X, label=train_y)
            deval = xgboost.DMatrix(eval_X, label=eval_y)

            params = {
                "tree_method": "approx",
                "objective": "reg:squarederror",
                "eta": 1e-4,
                "subsample": 0.5,
                "max_depth": 2,
            }

            # 2. Do distributed data-parallel training.
            # Ray Train sets up the necessary coordinator processes and
            # environment variables for your workers to communicate with each other.
            bst = xgboost.train(
                params,
                dtrain=dtrain,
                evals=[(deval, "validation")],
                num_boost_round=10,
                callbacks=[RayTrainReportCallback()],
            )

        train_ds = ray.data.from_items([{"x": x, "y": x + 1} for x in range(32)])
        eval_ds = ray.data.from_items([{"x": x, "y": x + 1} for x in range(16)])
        trainer = XGBoostTrainer(
            train_fn_per_worker,
            datasets={"train": train_ds, "validation": eval_ds},
            scaling_config=ray.train.ScalingConfig(num_workers=4),
        )
        result = trainer.fit()
        booster = RayTrainReportCallback.get_model(result.checkpoint)

    .. testoutput::
        :hide:

        ...

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
            See :class:`~ray.train.ScalingConfig` for more info.
        run_config: The configuration for the execution of the training run.
            See :class:`~ray.train.RunConfig` for more info.
        datasets: The Ray Datasets to ingest for training.
            Datasets are keyed by name (``{name: dataset}``).
            Each dataset can be accessed from within the ``train_loop_per_worker``
            by calling ``ray.train.get_dataset_shard(name)``.
            Sharding and additional configuration can be done by
            passing in a ``dataset_config``.
        dataset_config: The configuration for ingesting the input ``datasets``.
            By default, all the Ray Dataset are split equally across workers.
            See :class:`~ray.train.DataConfig` for more details.
        resume_from_checkpoint: A checkpoint to resume training from.
            This checkpoint can be accessed from within the ``train_loop_per_worker``
            by calling ``ray.train.get_checkpoint()``.
        metadata: Dict that should be made available via
            `ray.train.get_context().get_metadata()` and in `checkpoint.get_metadata()`
            for checkpoints saved from this Trainer. Must be JSON-serializable.
        use_external_memory: Whether to use external memory for training on
            large datasets. When enabled, datasets are automatically converted
            to use XGBoost's external memory API, allowing training on datasets
            larger than available RAM.
        external_memory_cache_dir: Directory for caching external memory files.
            If None, a temporary directory will be used.
        external_memory_device: Device to use for external memory training
            ("cpu" or "cuda").
        external_memory_batch_size: Batch size for external memory iteration.
            If None, an optimal batch size will be automatically determined.
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
        dataset_config: Optional[ray.train.DataConfig] = None,
        # TODO: [Deprecated]
        metadata: Optional[Dict[str, Any]] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        # TODO(justinvyu): [Deprecated] Legacy XGBoostTrainer API
        label_column: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        num_boost_round: Optional[int] = None,
        # External memory configuration
        use_external_memory: bool = False,
        external_memory_cache_dir: Optional[str] = None,
        external_memory_device: str = "cpu",
        external_memory_batch_size: Optional[int] = None,
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

        # Prepare train_loop_config with external memory settings
        if train_loop_config is None:
            train_loop_config = {}

        # Add external memory configuration to train_loop_config
        train_loop_config.update(
            {
                "use_external_memory": use_external_memory,
                "external_memory_cache_dir": external_memory_cache_dir,
                "external_memory_device": external_memory_device,
                "external_memory_batch_size": external_memory_batch_size,
            }
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

    def create_dmatrix(
        self,
        dataset_shard,
        label_column: Union[str, List[str]],
        feature_columns: Optional[List[str]] = None,
        **kwargs,
    ) -> "xgboost.DMatrix":
        """Automatically create the appropriate XGBoost DMatrix based on trainer
        configuration.

        This method automatically chooses between standard DMatrix and external memory
        DMatrix based on the `use_external_memory` parameter passed to the trainer.

        When external memory is enabled, it automatically converts the dataset to use
        XGBoost's external memory API.

        Args:
            dataset_shard: Ray dataset shard to convert.
            label_column: Name(s) of the label column(s).
            feature_columns: Names of feature columns. If None, all non-label
                columns are used.
            **kwargs: Additional arguments passed to DMatrix constructor.

        Returns:
            XGBoost DMatrix object (either standard or external memory).

        Example:
            .. testcode::

                def train_fn_per_worker(config: dict):
                    train_ds_iter = ray.train.get_dataset_shard("train")

                    # Automatically use external memory if enabled in trainer config
                    dtrain = trainer.create_dmatrix(
                        train_ds_iter,
                        label_column="target"
                    )

                    # Train as usual
                    bst = xgboost.train(params, dtrain=dtrain, ...)
        """
        from ray.train.xgboost._external_memory_utils import create_auto_dmatrix

        # Get external memory configuration from train_loop_config
        config = self.train_loop_config or {}
        use_external_memory = config.get("use_external_memory", False)
        external_memory_cache_dir = config.get("external_memory_cache_dir")
        external_memory_device = config.get("external_memory_device", "cpu")
        external_memory_batch_size = config.get("external_memory_batch_size")

        return create_auto_dmatrix(
            dataset_shard=dataset_shard,
            label_column=label_column,
            feature_columns=feature_columns,
            use_external_memory=use_external_memory,
            external_memory_cache_dir=external_memory_cache_dir,
            external_memory_device=external_memory_device,
            external_memory_batch_size=external_memory_batch_size,
            **kwargs,
        )
