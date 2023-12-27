from contextlib import contextmanager
from typing import Callable, Dict, List, Union, Optional, Type

import os
import tempfile

from ray import train

from ray.train import Checkpoint
from ray.train.constants import _DEPRECATED_VALUE
from ray.train.lightgbm import LightGBMCheckpoint
from ray.tune.utils import flatten_dict

from lightgbm.callback import CallbackEnv
from lightgbm.basic import Booster
from ray.util.annotations import Deprecated


class TuneCallback:
    """Base class for Tune's LightGBM callbacks."""

    pass


class TuneReportCheckpointCallback(TuneCallback):
    """Creates a callback that reports metrics and checkpoints model.

    Saves checkpoints after each validation step. Also reports metrics to Tune,
    which is needed for checkpoint registration.

    Args:
        metrics: Metrics to report to Tune. If this is a list,
            each item describes the metric key reported to LightGBM,
            and it will be reported under the same name to Tune. If this is a
            dict, each key will be the name reported to Tune and the respective
            value will be the metric key reported to LightGBM.
        filename: Filename of the checkpoint within the checkpoint
            directory. Defaults to "checkpoint". If this is None,
            all metrics will be reported to Tune under their default names as
            obtained from LightGBM.
        frequency: How often to save checkpoints. Defaults to 0 (no checkpoints
            are saved during training). A checkpoint is always saved at the end
            of training.
        results_postprocessing_fn: An optional Callable that takes in
            the dict that will be reported to Tune (after it has been flattened)
            and returns a modified dict that will be reported instead.

    Example:

    .. code-block:: python

        import lightgbm
        from ray.tune.integration.lightgbm import (
            TuneReportCheckpointCallback
        )

        config = {
            # ...
            "metric": ["binary_logloss", "binary_error"],
        }

        # Report only log loss to Tune after each validation epoch.
        # Save model as `lightgbm.mdl`.
        bst = lightgbm.train(
            config,
            train_set,
            valid_sets=[test_set],
            valid_names=["eval"],
            verbose_eval=False,
            callbacks=[TuneReportCheckpointCallback(
                {"loss": "eval-binary_logloss"}, "lightgbm.mdl)])

    """

    order = 20

    def __init__(
        self,
        metrics: Optional[Union[str, List[str], Dict[str, str]]] = None,
        frequency: int = 1,
        results_postprocessing_fn: Optional[
            Callable[[Dict[str, Union[float, List[float]]]], Dict[str, float]]
        ] = None,
        checkpoint_cls: Type[LightGBMCheckpoint] = LightGBMCheckpoint,
        filename: str = _DEPRECATED_VALUE,
    ):
        if filename != _DEPRECATED_VALUE:
            # TODO(justinvyu): [code_removal] Remove in 2.11.
            raise DeprecationWarning(
                "`filename` is deprecated. Supply a custom `checkpoint_cls` "
                "that subclasses `ray.train.lightgbm.LightGBMCheckpoint` instead."
            )

        if isinstance(metrics, str):
            metrics = [metrics]
        self._metrics = metrics
        self._frequency = frequency
        self._results_postprocessing_fn = results_postprocessing_fn

        if not issubclass(checkpoint_cls, LightGBMCheckpoint):
            raise ValueError(
                "`checkpoint_cls` must subclass `ray.train.lightgbm.LightGBMCheckpoint`"
            )
        self._checkpoint_cls = checkpoint_cls

    def _get_report_dict(self, evals_log: Dict[str, Dict[str, list]]) -> dict:
        result_dict = flatten_dict(evals_log, delimiter="-")
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

    def _get_eval_result(self, env: CallbackEnv) -> dict:
        eval_result = {}
        for entry in env.evaluation_result_list:
            data_name, eval_name, result = entry[0:3]
            if len(entry) > 4:
                stdv = entry[4]
                suffix = "-mean"
            else:
                stdv = None
                suffix = ""
            if data_name not in eval_result:
                eval_result[data_name] = {}
            eval_result[data_name][eval_name + suffix] = result
            if stdv is not None:
                eval_result[data_name][eval_name + "-stdv"] = stdv
        return eval_result

    @contextmanager
    def _get_checkpoint(self, model: Booster) -> Optional[Checkpoint]:
        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            checkpoint = self._checkpoint_cls.from_model(
                model, path=temp_checkpoint_dir
            )
            yield checkpoint

    def __call__(self, env: CallbackEnv) -> None:
        eval_result = self._get_eval_result(env)
        report_dict = self._get_report_dict(eval_result)

        print("DEBUG!!!", env.iteration, env.end_iteration)
        checkpointing_disabled = self._frequency == 0
        # Ex: if frequency=2, checkpoint at epoch 1, 3, 5, ... (counting from 0)
        should_checkpoint = (
            not checkpointing_disabled and (env.iteration + 1) % self._frequency == 0
        )

        if should_checkpoint:
            with self._get_checkpoint(model=env.model) as checkpoint:
                train.report(report_dict, checkpoint=checkpoint)
        else:
            train.report(report_dict)


@Deprecated
class TuneReportCallback(TuneReportCheckpointCallback):
    def __new__(cls: type, *args, **kwargs):
        # TODO(justinvyu): [code_removal] Remove in 2.11.
        raise DeprecationWarning(
            "`TuneReportCallback` is deprecated. "
            "Use `ray.tune.integration.lightgbm.TuneReportCheckpointCallback` instead."
        )
