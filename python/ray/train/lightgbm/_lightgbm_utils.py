import tempfile
from abc import abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Union

from lightgbm.basic import Booster
from lightgbm.callback import CallbackEnv

import ray.train
from ray.train import Checkpoint
from ray.tune.utils import flatten_dict
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pandas as pd


@PublicAPI(stability="alpha")
def normalize_pandas_for_lightgbm(df: "pd.DataFrame") -> "pd.DataFrame":
    """Map Arrow-backed pandas dtypes to NumPy-nullable equivalents.

    LightGBM's pandas input validation rejects Arrow-backed dtypes like
    ``int64[pyarrow]``. Since Ray Data 2.56, ``Dataset.to_pandas()`` preserves
    Arrow-backed dtypes when the source was Arrow, so callers passing the
    resulting frame to ``lightgbm.Dataset`` must normalize first.

    This helper is a faster alternative to
    ``df.convert_dtypes(dtype_backend="numpy_nullable")``:

    - It maps dtypes mechanically rather than scanning every value.
    - It only touches ``pd.ArrowDtype`` columns. NumPy-backed columns (e.g.
      from ``ray.data.from_pandas`` shards) keep their original buffers.

    Only numeric and boolean Arrow dtypes are remapped. Other Arrow dtypes
    (string, decimal, timestamp) are left as-is; LightGBM doesn't accept them
    as features anyway.

    Args:
        df: The pandas DataFrame to normalize.

    Returns:
        A DataFrame with Arrow-backed numeric/boolean columns replaced by
        NumPy-nullable equivalents. Other columns are returned unchanged.
    """
    import pandas as pd
    import pyarrow as pa

    dtype_mapping = {}
    for column, dtype in df.dtypes.items():
        if not isinstance(dtype, pd.ArrowDtype):
            continue
        arrow_dtype = dtype.pyarrow_dtype
        if pa.types.is_signed_integer(arrow_dtype):
            dtype_mapping[column] = f"Int{arrow_dtype.bit_width}"
        elif pa.types.is_unsigned_integer(arrow_dtype):
            dtype_mapping[column] = f"UInt{arrow_dtype.bit_width}"
        elif pa.types.is_floating(arrow_dtype):
            dtype_mapping[column] = f"Float{arrow_dtype.bit_width}"
        elif pa.types.is_boolean(arrow_dtype):
            dtype_mapping[column] = "boolean"
    if dtype_mapping:
        df = df.astype(dtype_mapping, copy=False)
    return df


class RayReportCallback:
    CHECKPOINT_NAME = "model.txt"

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
        """Retrieve the model stored in a checkpoint reported by this callback.

        Args:
            checkpoint: The checkpoint object returned by a training run.
                The checkpoint should be saved by an instance of this callback.
            filename: The filename to load the model from, which should match
                the filename used when creating the callback.
        Returns:
            The model loaded from the checkpoint.
        """
        with checkpoint.as_directory() as checkpoint_path:
            return Booster(model_file=Path(checkpoint_path, filename).as_posix())

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

    @abstractmethod
    def _get_checkpoint(self, model: Booster) -> Optional[Checkpoint]:
        """Get checkpoint from model.

        This method needs to be implemented by subclasses.
        """
        raise NotImplementedError

    @abstractmethod
    def _save_and_report_checkpoint(self, report_dict: Dict, model: Booster):
        """Save checkpoint and report metrics corresonding to this checkpoint.

        This method needs to be implemented by subclasses.
        """
        raise NotImplementedError

    @abstractmethod
    def _report_metrics(self, report_dict: Dict):
        """Report Metrics.

        This method needs to be implemented by subclasses.
        """
        raise NotImplementedError

    def __call__(self, env: CallbackEnv) -> None:
        eval_result = self._get_eval_result(env)
        report_dict = self._get_report_dict(eval_result)

        # Ex: if frequency=2, checkpoint_at_end=True and num_boost_rounds=11,
        # you will checkpoint at iterations 1, 3, 5, ..., 9, and 10 (checkpoint_at_end)
        # (iterations count from 0)
        on_last_iter = env.iteration == env.end_iteration - 1
        should_checkpoint_at_end = on_last_iter and self._checkpoint_at_end
        should_checkpoint_with_frequency = (
            self._frequency != 0 and (env.iteration + 1) % self._frequency == 0
        )
        should_checkpoint = should_checkpoint_at_end or should_checkpoint_with_frequency

        if should_checkpoint:
            self._save_and_report_checkpoint(report_dict, env.model)
        else:
            self._report_metrics(report_dict)


@PublicAPI(stability="beta")
class RayTrainReportCallback(RayReportCallback):
    """Creates a callback that reports metrics and checkpoints model.

    Args:
        metrics: Metrics to report. If this is a list,
            each item should be a metric key reported by LightGBM,
            and it will be reported to Ray Train/Tune under the same name.
            This can also be a dict of {<key-to-report>: <lightgbm-metric-key>},
            which can be used to rename LightGBM default metrics.
        filename: Customize the saved checkpoint file type by passing
            a filename. Defaults to "model.txt".
        frequency: How often to save checkpoints, in terms of iterations.
            Defaults to 0 (no checkpoints are saved during training).
        checkpoint_at_end: Whether or not to save a checkpoint at the end of training.
        results_postprocessing_fn: An optional Callable that takes in
            the metrics dict that will be reported (after it has been flattened)
            and returns a modified dict.

    Examples
    --------

    Reporting checkpoints and metrics to Ray Tune when running many
    independent LightGBM trials (without data parallelism within a trial).

    .. testcode::
        :skipif: True

        import lightgbm

        from ray.train.lightgbm import RayTrainReportCallback

        config = {
            # ...
            "metric": ["binary_logloss", "binary_error"],
        }

        # Report only log loss to Tune after each validation epoch.
        bst = lightgbm.train(
            ...,
            callbacks=[
                RayTrainReportCallback(
                    metrics={"loss": "eval-binary_logloss"}, frequency=1
                )
            ],
        )

    Loading a model from a checkpoint reported by this callback.

    .. testcode::
        :skipif: True

        from ray.train.lightgbm import RayTrainReportCallback

        # Get a `Checkpoint` object that is saved by the callback during training.
        result = trainer.fit()
        booster = RayTrainReportCallback.get_model(result.checkpoint)

    """

    @contextmanager
    def _get_checkpoint(self, model: Booster) -> Optional[Checkpoint]:
        if ray.train.get_context().get_world_rank() in (0, None):
            with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
                model.save_model(Path(temp_checkpoint_dir, self._filename).as_posix())
                yield Checkpoint.from_directory(temp_checkpoint_dir)
        else:
            yield None

    def _save_and_report_checkpoint(self, report_dict: Dict, model: Booster):
        with self._get_checkpoint(model=model) as checkpoint:
            ray.train.report(report_dict, checkpoint=checkpoint)

    def _report_metrics(self, report_dict: Dict):
        ray.train.report(report_dict)
