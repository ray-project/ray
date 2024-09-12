import tempfile
from collections import OrderedDict
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Dict, List, Optional, Union

from xgboost.core import Booster

import ray.train
from ray.train import Checkpoint
from ray.tune.utils import flatten_dict
from ray.util.annotations import PublicAPI

try:
    from xgboost.callback import TrainingCallback
except ImportError:

    class TrainingCallback:
        pass


class TuneCallback(TrainingCallback):
    # TODO(justinvyu): [code_removal] Remove this after enforcing min xgboost version.
    """Base class for Tune's XGBoost callbacks."""

    def __call__(self, env):
        """Compatibility with xgboost<1.3"""
        return self.after_iteration(
            env.model, env.iteration, env.evaluation_result_list
        )

    def after_iteration(self, model: Booster, epoch: int, evals_log: Dict):
        raise NotImplementedError


@PublicAPI(stability="beta")
class RayTrainReportCallback(TuneCallback):
    """XGBoost callback to save checkpoints and report metrics.

    Args:
        metrics: Metrics to report. If this is a list,
            each item describes the metric key reported to XGBoost,
            and it will be reported under the same name.
            This can also be a dict of {<key-to-report>: <xgboost-metric-key>},
            which can be used to rename xgboost default metrics.
        filename: Customize the saved checkpoint file type by passing
            a filename. Defaults to "model.ubj".
        frequency: How often to save checkpoints, in terms of iterations.
            Defaults to 0 (no checkpoints are saved during training).
        checkpoint_at_end: Whether or not to save a checkpoint at the end of training.
        results_postprocessing_fn: An optional Callable that takes in
            the metrics dict that will be reported (after it has been flattened)
            and returns a modified dict. For example, this can be used to
            average results across CV fold when using ``xgboost.cv``.

    Examples
    --------

    Reporting checkpoints and metrics to Ray Tune when running many
    independent xgboost trials (without data parallelism within a trial).

    .. testcode::
        :skipif: True

        import xgboost

        from ray.tune import Tuner
        from ray.train.xgboost import RayTrainReportCallback

        def train_fn(config):
            # Report log loss to Ray Tune after each validation epoch.
            bst = xgboost.train(
                ...,
                callbacks=[
                    RayTrainReportCallback(
                        metrics={"loss": "eval-logloss"}, frequency=1
                    )
                ],
            )

        tuner = Tuner(train_fn)
        results = tuner.fit()

    Loading a model from a checkpoint reported by this callback.

    .. testcode::
        :skipif: True

        from ray.train.xgboost import RayTrainReportCallback

        # Get a `Checkpoint` object that is saved by the callback during training.
        result = trainer.fit()
        booster = RayTrainReportCallback.get_model(result.checkpoint)

    """

    CHECKPOINT_NAME = "model.ubj"

    def __init__(
        self,
        metrics: Optional[Union[str, List[str], Dict[str, str]]] = None,
        filename: str = CHECKPOINT_NAME,
        frequency: int = 0,
        checkpoint_at_end: bool = True,
        results_postprocessing_fn: Optional[
            Callable[[Dict[str, Union[float, List[float]]]], Dict[str, float]]
        ] = None,
    ):
        if isinstance(metrics, str):
            metrics = [metrics]
        self._metrics = metrics
        self._filename = filename
        self._frequency = frequency
        self._checkpoint_at_end = checkpoint_at_end
        self._results_postprocessing_fn = results_postprocessing_fn

        # Keeps track of the eval metrics from the last iteration,
        # so that the latest metrics can be reported with the checkpoint
        # at the end of training.
        self._evals_log = None
        # Keep track of the last checkpoint iteration to avoid double-checkpointing
        # when using `checkpoint_at_end=True`.
        self._last_checkpoint_iteration = None

    @classmethod
    def get_model(
        cls, checkpoint: Checkpoint, filename: str = CHECKPOINT_NAME
    ) -> Booster:
        """Retrieve the model stored in a checkpoint reported by this callback.

        Args:
            checkpoint: The checkpoint object returned by a training run.
                The checkpoint should be saved by an instance of this callback.
            filename: The filename to load the model from, which should match
                the filename used when creating the callback.
        """
        with checkpoint.as_directory() as checkpoint_path:
            booster = Booster()
            booster.load_model(Path(checkpoint_path, filename).as_posix())
            return booster

    def _get_report_dict(self, evals_log):
        if isinstance(evals_log, OrderedDict):
            # xgboost>=1.3
            result_dict = flatten_dict(evals_log, delimiter="-")
            for k in list(result_dict):
                result_dict[k] = result_dict[k][-1]
        else:
            # xgboost<1.3
            result_dict = dict(evals_log)
        if not self._metrics:
            report_dict = result_dict
        else:
            report_dict = {}
            for key in self._metrics:
                if isinstance(self._metrics, dict):
                    metric = self._metrics[key]
                else:
                    metric = key
                report_dict[key] = result_dict[metric]

        if self._results_postprocessing_fn:
            report_dict = self._results_postprocessing_fn(report_dict)

        return report_dict

    @contextmanager
    def _get_checkpoint(self, model: Booster) -> Optional[Checkpoint]:
        # NOTE: The world rank returns None for Tune usage without Train.
        if ray.train.get_context().get_world_rank() in (0, None):
            with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
                model.save_model(Path(temp_checkpoint_dir, self._filename).as_posix())
                yield Checkpoint(temp_checkpoint_dir)
        else:
            yield None

    def after_iteration(self, model: Booster, epoch: int, evals_log: Dict):
        self._evals_log = evals_log

        checkpointing_disabled = self._frequency == 0
        # Ex: if frequency=2, checkpoint at epoch 1, 3, 5, ... (counting from 0)
        should_checkpoint = (
            not checkpointing_disabled and (epoch + 1) % self._frequency == 0
        )

        report_dict = self._get_report_dict(evals_log)
        if should_checkpoint:
            self._last_checkpoint_iteration = epoch
            with self._get_checkpoint(model=model) as checkpoint:
                ray.train.report(report_dict, checkpoint=checkpoint)
        else:
            ray.train.report(report_dict)

    def after_training(self, model: Booster) -> Booster:
        if not self._checkpoint_at_end:
            return model

        if (
            self._last_checkpoint_iteration is not None
            and model.num_boosted_rounds() - 1 == self._last_checkpoint_iteration
        ):
            # Avoids a duplicate checkpoint if the checkpoint frequency happens
            # to align with the last iteration.
            return model

        report_dict = self._get_report_dict(self._evals_log) if self._evals_log else {}
        with self._get_checkpoint(model=model) as checkpoint:
            ray.train.report(report_dict, checkpoint=checkpoint)

        return model
