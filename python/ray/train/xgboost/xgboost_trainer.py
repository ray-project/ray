import os
from typing import Any, Dict

try:
    from packaging.version import Version
except ImportError:
    from distutils.version import LooseVersion as Version

from ray.train import Checkpoint
from ray.train.gbdt_trainer import GBDTTrainer
from ray.train.xgboost import XGBoostCheckpoint
from ray.util.annotations import PublicAPI

import xgboost
import xgboost_ray
from xgboost_ray.tune import TuneReportCheckpointCallback


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
        .. testcode::

            import ray

            from ray.train.xgboost import XGBoostTrainer
            from ray.train import ScalingConfig

            train_dataset = ray.data.from_items(
                [{"x": x, "y": x + 1} for x in range(32)])
            trainer = XGBoostTrainer(
                label_column="y",
                params={"objective": "reg:squarederror"},
                scaling_config=ScalingConfig(num_workers=3),
                datasets={"train": train_dataset}
            )
            result = trainer.fit()

        .. testoutput::
            :hide:

            ...

    Args:
        datasets: The Ray Datasets to use for training and validation. Must include a
            "train" key denoting the training dataset. All non-training datasets will
            be used as separate validation sets, each reporting a separate metric.
        label_column: Name of the label column. A column with this name
            must be present in the training dataset.
        params: XGBoost training parameters.
            Refer to `XGBoost documentation <https://xgboost.readthedocs.io/>`_
            for a list of possible parameters.
        dmatrix_params: Dict of ``dataset name:dict of kwargs`` passed to respective
            :class:`xgboost_ray.RayDMatrix` initializations, which in turn are passed
            to ``xgboost.DMatrix`` objects created on each worker. For example, this can
            be used to add sample weights with the ``weight`` parameter.
        num_boost_round: Target number of boosting iterations (trees in the model).
            Note that unlike in ``xgboost.train``, this is the target number
            of trees, meaning that if you set ``num_boost_round=10`` and pass a model
            that has already been trained for 5 iterations, it will be trained for 5
            iterations more, instead of 10 more.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        resume_from_checkpoint: A checkpoint to resume training from.
        metadata: Dict that should be made available in `checkpoint.get_metadata()`
            for checkpoints saved from this Trainer. Must be JSON-serializable.
        **train_kwargs: Additional kwargs passed to ``xgboost.train()`` function.
    """

    _dmatrix_cls: type = xgboost_ray.RayDMatrix
    _ray_params_cls: type = xgboost_ray.RayParams
    _tune_callback_checkpoint_cls: type = TuneReportCheckpointCallback
    _default_ray_params: Dict[str, Any] = {
        "num_actors": 1,
        "cpus_per_actor": 1,
        "gpus_per_actor": 0,
    }
    _init_model_arg_name: str = "xgb_model"

    @staticmethod
    def get_model(checkpoint: Checkpoint) -> xgboost.Booster:
        """Retrieve the XGBoost model stored in this checkpoint."""
        with checkpoint.as_directory() as checkpoint_path:
            booster = xgboost.Booster()
            booster.load_model(
                os.path.join(checkpoint_path, XGBoostCheckpoint.MODEL_FILENAME)
            )
            return booster

    def _train(self, **kwargs):
        return xgboost_ray.train(**kwargs)

    def _load_checkpoint(self, checkpoint: Checkpoint) -> xgboost.Booster:
        return self.__class__.get_model(checkpoint)

    def _save_model(self, model: xgboost.Booster, path: str):
        model.save_model(os.path.join(path, XGBoostCheckpoint.MODEL_FILENAME))

    def _model_iteration(self, model: xgboost.Booster) -> int:
        if not hasattr(model, "num_boosted_rounds"):
            # Compatibility with XGBoost < 1.4
            return len(model.get_dump())
        return model.num_boosted_rounds()

    def preprocess_datasets(self) -> None:
        super().preprocess_datasets()

        # XGBoost/LightGBM-Ray requires each dataset to have at least as many
        # blocks as there are workers.
        # This is only applicable for xgboost-ray<0.1.16
        if Version(xgboost_ray.__version__) < Version("0.1.16"):
            self._repartition_datasets_to_match_num_actors()
