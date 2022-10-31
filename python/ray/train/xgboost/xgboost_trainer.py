from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from ray.air.checkpoint import Checkpoint
from ray.train.gbdt_trainer import GBDTTrainer
from ray.train.xgboost.xgboost_checkpoint import XGBoostCheckpoint
from ray.util.annotations import PublicAPI

import xgboost
import xgboost_ray
from xgboost_ray.tune import TuneReportCheckpointCallback, TuneReportCallback

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="beta")
class XGBoostTrainer(GBDTTrainer):
    """A Trainer for data parallel XGBoost training.

    This Trainer runs the XGBoost training loop in a distributed manner
    using multiple Ray Actors.

    .. note::
        ``XGBoostTrainer`` does not modify or otherwise alter the working
        of the XGBoost distributed training algorithm.
        Ray only provides orchestration, data ingest and fault tolerance.
        For more information on XGBoost distributed training, refer to
        `XGBoost documentation <https://xgboost.readthedocs.io>`__.

    Example:
        .. code-block:: python

            import ray

            from ray.train.xgboost import XGBoostTrainer
            from ray.air.config import ScalingConfig

            train_dataset = ray.data.from_items(
                [{"x": x, "y": x + 1} for x in range(32)])
            trainer = XGBoostTrainer(
                label_column="y",
                params={"objective": "reg:squarederror"},
                scaling_config=ScalingConfig(num_workers=3),
                datasets={"train": train_dataset}
            )
            result = trainer.fit()

    Args:
        datasets: Ray Datasets to use for training and validation. Must include a
            "train" key denoting the training dataset. If a ``preprocessor``
            is provided and has not already been fit, it will be fit on the training
            dataset. All datasets will be transformed by the ``preprocessor`` if
            one is provided. All non-training datasets will be used as separate
            validation sets, each reporting a separate metric.
        label_column: Name of the label column. A column with this name
            must be present in the training dataset.
        params: XGBoost training parameters.
            Refer to `XGBoost documentation <https://xgboost.readthedocs.io/>`_
            for a list of possible parameters.
        dmatrix_params: Dict of ``dataset name:dict of kwargs`` passed to respective
            :class:`xgboost_ray.RayDMatrix` initializations, which in turn are passed
            to ``xgboost.DMatrix`` objects created on each worker. For example, this can
            be used to add sample weights with the ``weights`` parameter.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        preprocessor: A ray.data.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
        **train_kwargs: Additional kwargs passed to ``xgboost.train()`` function.
    """

    _dmatrix_cls: type = xgboost_ray.RayDMatrix
    _ray_params_cls: type = xgboost_ray.RayParams
    _tune_callback_report_cls: type = TuneReportCallback
    _tune_callback_checkpoint_cls: type = TuneReportCheckpointCallback
    _default_ray_params: Dict[str, Any] = {
        "num_actors": 1,
        "cpus_per_actor": 1,
        "gpus_per_actor": 0,
    }
    _init_model_arg_name: str = "xgb_model"

    def _train(self, **kwargs):
        return xgboost_ray.train(**kwargs)

    def _load_checkpoint(
        self, checkpoint: Checkpoint
    ) -> Tuple[xgboost.Booster, Optional["Preprocessor"]]:
        checkpoint = XGBoostCheckpoint.from_checkpoint(checkpoint)
        return checkpoint.get_model(), checkpoint.get_preprocessor()

    def _save_model(self, model: xgboost.Booster, path: str):
        model.save_model(path)

    def _model_iteration(self, model: xgboost.Booster) -> int:
        if not hasattr(model, "num_boosted_rounds"):
            # Compatibility with XGBoost < 1.4
            return len(model.get_dump())
        return model.num_boosted_rounds()
