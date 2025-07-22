import logging
from typing import Any, Callable, Dict, Optional, Union

import ray.train
from ray.train import Checkpoint
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.trainer import GenDataset
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
        from ray.train.xgboost import RayTrainReportCallback, XGBoostTrainer

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
        datasets: The Ray Datasets to use for training and validation.
        dataset_config: The configuration for ingesting the input ``datasets``.
            By default, all the Ray Datasets are split equally across workers.
            See :class:`~ray.train.DataConfig` for more details.
        scaling_config: The configuration for how to scale data parallel training.
            ``num_workers`` determines how many Python processes are used for training,
            and ``use_gpu`` determines whether or not each process should use GPUs.
            See :class:`~ray.train.ScalingConfig` for more info.
        run_config: The configuration for the execution of the training run.
            See :class:`~ray.train.RunConfig` for more info.
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
        xgboost_config: Optional[XGBoostConfig] = None,
        scaling_config: Optional[ray.train.ScalingConfig] = None,
        run_config: Optional[ray.train.RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        dataset_config: Optional[ray.train.DataConfig] = None,
        metadata: Optional[Dict[str, Any]] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
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
