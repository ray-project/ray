from contextlib import contextmanager
from pathlib import Path
import tempfile
from typing import Callable, Dict, List, Union, Optional


from ray import train

from ray.train import Checkpoint
from ray.tune.utils import flatten_dict

from lightgbm.callback import CallbackEnv
from lightgbm.basic import Booster
from ray.util.annotations import Deprecated


class TuneCallback:
    """Base class for Tune's LightGBM callbacks."""

    pass


class TuneReportCheckpointCallback(TuneCallback):
    """Creates a callback that reports metrics and checkpoints model.

    Args:
        metrics: Metrics to report. If this is a list,
            each item should be a metric key reported by LightGBM,
            and it will be reported to Ray Train/Tune under the same name.
            This can also be a dict of {<key-to-report>: <lightgbm-metric-key>},
            which can be used to rename lightgbm default metrics.
        filename: Customize the saved checkpoint file type by passing
            a filename. Defaults to "model.txt".
        frequency: How often to save checkpoints, in terms of iterations.
            Defaults to 0 (no checkpoints are saved during training).
        checkpoint_at_end: Whether or not to save a checkpoint at the end of training.
        results_postprocessing_fn: An optional Callable that takes in
            the metrics dict that will be reported (after it has been flattened)
            and returns a modified dict.

    Example:

    .. code-block:: python

        import lightgbm
        from ray.tune.integration.lightgbm import TuneReportCheckpointCallback

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
            callbacks=[
                TuneReportCheckpointCallback(
                    metrics={"loss": "eval-binary_logloss"}, frequency=1
                )
            ],
        )

    """

    CHECKPOINT_NAME = "model.txt"
    order = 20

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

    @classmethod
    def get_model(
        cls, checkpoint: Checkpoint, filename: str = CHECKPOINT_NAME
    ) -> Booster:
        """Retrieve the model stored in a checkpoint reported by this callback."""
        with checkpoint.as_directory() as checkpoint_path:
            booster = Booster()
            booster.load_model(Path(checkpoint_path, filename).as_posix())
            return booster

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
            model.save_model(Path(temp_checkpoint_dir, self._filename).as_posix())
            yield Checkpoint.from_directory(temp_checkpoint_dir)

    def __call__(self, env: CallbackEnv) -> None:
        eval_result = self._get_eval_result(env)
        report_dict = self._get_report_dict(eval_result)

        on_last_iter = env.iteration == env.end_iteration - 1
        checkpointing_disabled = self._frequency == 0
        # Ex: if frequency=2, checkpoint_at_end=True and num_boost_rounds=10,
        # you will checkpoint at iterations 1, 3, 5, ..., and 9 (checkpoint_at_end)
        # (counting from 0)
        should_checkpoint = (
            not checkpointing_disabled and (env.iteration + 1) % self._frequency == 0
        ) or (on_last_iter and self._checkpoint_at_end)

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


class _TuneCheckpointCallback(TuneCallback):
    def __new__(cls: type, *args, **kwargs):
        # TODO(justinvyu): [code_removal] Remove ASAP after merging in lightgbm_ray.
        raise DeprecationWarning(
            "Use `ray.tune.integration.lightgbm.TuneReportCheckpointCallback` instead."
        )
