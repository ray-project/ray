from __future__ import print_function

import datetime
from typing import Dict, List, Optional, Union

import collections
import os
import sys
import numpy as np
import time

from ray.util.annotations import PublicAPI, DeveloperAPI
from ray.util.queue import Queue

from ray.tune.callback import Callback
from ray.tune.logger import pretty_print, logger
from ray.tune.result import (
    DEFAULT_METRIC,
    EPISODE_REWARD_MEAN,
    MEAN_ACCURACY,
    MEAN_LOSS,
    NODE_IP,
    PID,
    TRAINING_ITERATION,
    TIME_TOTAL_S,
    TIMESTEPS_TOTAL,
    AUTO_RESULT_KEYS,
)
from ray.tune.trial import DEBUG_PRINT_INTERVAL, Trial, Location
from ray.tune.utils import unflattened_lookup
from ray.tune.utils.log import Verbosity, has_verbosity

try:
    from collections.abc import Mapping, MutableMapping
except ImportError:
    from collections import Mapping, MutableMapping

try:
    from tabulate import tabulate
except ImportError:
    raise ImportError(
        "ray.tune in ray > 0.7.5 requires 'tabulate'. "
        "Please re-run 'pip install ray[tune]' or "
        "'pip install ray[rllib]'."
    )

try:
    class_name = get_ipython().__class__.__name__
    IS_NOTEBOOK = True if "Terminal" not in class_name else False
except NameError:
    IS_NOTEBOOK = False


@PublicAPI
class ProgressReporter:
    """Abstract class for experiment progress reporting.

    `should_report()` is called to determine whether or not `report()` should
    be called. Tune will call these functions after trial state transitions,
    receiving training results, and so on.
    """

    def should_report(self, trials: List[Trial], done: bool = False):
        """Returns whether or not progress should be reported.

        Args:
            trials: Trials to report on.
            done: Whether this is the last progress report attempt.
        """
        raise NotImplementedError

    def report(self, trials: List[Trial], done: bool, *sys_info: Dict):
        """Reports progress across trials.

        Args:
            trials: Trials to report on.
            done: Whether this is the last progress report attempt.
            sys_info: System info.
        """
        raise NotImplementedError

    def set_search_properties(self, metric: Optional[str], mode: Optional[str]):
        return True

    def set_total_samples(self, total_samples: int):
        pass


@DeveloperAPI
class TuneReporterBase(ProgressReporter):
    """Abstract base class for the default Tune reporters.

    If metric_columns is not overridden, Tune will attempt to automatically
    infer the metrics being outputted, up to 'infer_limit' number of
    metrics.

    Args:
        metric_columns: Names of metrics to
            include in progress table. If this is a dict, the keys should
            be metric names and the values should be the displayed names.
            If this is a list, the metric name is used directly.
        parameter_columns: Names of parameters to
            include in progress table. If this is a dict, the keys should
            be parameter names and the values should be the displayed names.
            If this is a list, the parameter name is used directly. If empty,
            defaults to all available parameters.
        max_progress_rows: Maximum number of rows to print
            in the progress table. The progress table describes the
            progress of each trial. Defaults to 20.
        max_error_rows: Maximum number of rows to print in the
            error table. The error table lists the error file, if any,
            corresponding to each trial. Defaults to 20.
        max_report_frequency: Maximum report frequency in seconds.
            Defaults to 5s.
        infer_limit: Maximum number of metrics to automatically infer
            from tune results.
        print_intermediate_tables: Print intermediate result
            tables. If None (default), will be set to True for verbosity
            levels above 3, otherwise False. If True, intermediate tables
            will be printed with experiment progress. If False, tables
            will only be printed at then end of the tuning run for verbosity
            levels greater than 2.
        metric: Metric used to determine best current trial.
        mode: One of [min, max]. Determines whether objective is
            minimizing or maximizing the metric attribute.
        sort_by_metric: Sort terminated trials by metric in the
            intermediate table. Defaults to False.
    """

    # Truncated representations of column names (to accommodate small screens).
    DEFAULT_COLUMNS = collections.OrderedDict(
        {
            MEAN_ACCURACY: "acc",
            MEAN_LOSS: "loss",
            TRAINING_ITERATION: "iter",
            TIME_TOTAL_S: "total time (s)",
            TIMESTEPS_TOTAL: "ts",
            EPISODE_REWARD_MEAN: "reward",
        }
    )
    VALID_SUMMARY_TYPES = {
        int,
        float,
        np.float32,
        np.float64,
        np.int32,
        np.int64,
        type(None),
    }

    def __init__(
        self,
        metric_columns: Union[None, List[str], Dict[str, str]] = None,
        parameter_columns: Union[None, List[str], Dict[str, str]] = None,
        total_samples: Optional[int] = None,
        max_progress_rows: int = 20,
        max_error_rows: int = 20,
        max_report_frequency: int = 5,
        infer_limit: int = 3,
        print_intermediate_tables: Optional[bool] = None,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        sort_by_metric: bool = False,
    ):
        self._total_samples = total_samples
        self._metrics_override = metric_columns is not None
        self._inferred_metrics = {}
        self._metric_columns = metric_columns or self.DEFAULT_COLUMNS.copy()
        self._parameter_columns = parameter_columns or []
        self._max_progress_rows = max_progress_rows
        self._max_error_rows = max_error_rows
        self._infer_limit = infer_limit

        if print_intermediate_tables is None:
            self._print_intermediate_tables = has_verbosity(Verbosity.V3_TRIAL_DETAILS)
        else:
            self._print_intermediate_tables = print_intermediate_tables

        self._max_report_freqency = max_report_frequency
        self._last_report_time = 0

        self._start_time = time.time()

        self._metric = metric
        self._mode = mode

        if metric is None or mode is None:
            self._sort_by_metric = False
        else:
            self._sort_by_metric = sort_by_metric

    def set_search_properties(self, metric: Optional[str], mode: Optional[str]):
        if self._metric and metric:
            return False
        if self._mode and mode:
            return False

        if metric:
            self._metric = metric
        if mode:
            self._mode = mode

        if self._metric is None and self._mode:
            # If only a mode was passed, use anonymous metric
            self._metric = DEFAULT_METRIC

        return True

    def set_total_samples(self, total_samples: int):
        self._total_samples = total_samples

    def set_start_time(self, timestamp: Optional[float] = None):
        if timestamp is not None:
            self._start_time = time.time()
        else:
            self._start_time = timestamp

    def should_report(self, trials: List[Trial], done: bool = False):
        if time.time() - self._last_report_time > self._max_report_freqency:
            self._last_report_time = time.time()
            return True
        return done

    def add_metric_column(self, metric: str, representation: Optional[str] = None):
        """Adds a metric to the existing columns.

        Args:
            metric: Metric to add. This must be a metric being returned
                in training step results.
            representation: Representation to use in table. Defaults to
                `metric`.
        """
        self._metrics_override = True
        if metric in self._metric_columns:
            raise ValueError("Column {} already exists.".format(metric))

        if isinstance(self._metric_columns, MutableMapping):
            representation = representation or metric
            self._metric_columns[metric] = representation
        else:
            if representation is not None and representation != metric:
                raise ValueError(
                    "`representation` cannot differ from `metric` "
                    "if this reporter was initialized with a list "
                    "of metric columns."
                )
            self._metric_columns.append(metric)

    def add_parameter_column(
        self, parameter: str, representation: Optional[str] = None
    ):
        """Adds a parameter to the existing columns.

        Args:
            parameter: Parameter to add. This must be a parameter
                specified in the configuration.
            representation: Representation to use in table. Defaults to
                `parameter`.
        """
        if parameter in self._parameter_columns:
            raise ValueError("Column {} already exists.".format(parameter))

        if isinstance(self._parameter_columns, MutableMapping):
            representation = representation or parameter
            self._parameter_columns[parameter] = representation
        else:
            if representation is not None and representation != parameter:
                raise ValueError(
                    "`representation` cannot differ from `parameter` "
                    "if this reporter was initialized with a list "
                    "of metric columns."
                )
            self._parameter_columns.append(parameter)

    def _progress_str(
        self,
        trials: List[Trial],
        done: bool,
        *sys_info: Dict,
        fmt: str = "psql",
        delim: str = "\n",
    ):
        """Returns full progress string.

        This string contains a progress table and error table. The progress
        table describes the progress of each trial. The error table lists
        the error file, if any, corresponding to each trial. The latter only
        exists if errors have occurred.

        Args:
            trials: Trials to report on.
            done: Whether this is the last progress report attempt.
            fmt: Table format. See `tablefmt` in tabulate API.
            delim: Delimiter between messages.
        """
        if not self._metrics_override:
            user_metrics = self._infer_user_metrics(trials, self._infer_limit)
            self._metric_columns.update(user_metrics)
        messages = [
            "== Status ==",
            time_passed_str(self._start_time, time.time()),
            memory_debug_str(),
            *sys_info,
        ]
        if done:
            max_progress = None
            max_error = None
        else:
            max_progress = self._max_progress_rows
            max_error = self._max_error_rows

        current_best_trial, metric = self._current_best_trial(trials)
        if current_best_trial:
            messages.append(
                best_trial_str(current_best_trial, metric, self._parameter_columns)
            )

        if has_verbosity(Verbosity.V1_EXPERIMENT):
            # Will filter the table in `trial_progress_str`
            messages.append(
                trial_progress_str(
                    trials,
                    metric_columns=self._metric_columns,
                    parameter_columns=self._parameter_columns,
                    total_samples=self._total_samples,
                    force_table=self._print_intermediate_tables,
                    fmt=fmt,
                    max_rows=max_progress,
                    done=done,
                    metric=self._metric,
                    mode=self._mode,
                    sort_by_metric=self._sort_by_metric,
                )
            )
            messages.append(trial_errors_str(trials, fmt=fmt, max_rows=max_error))

        return delim.join(messages) + delim

    def _infer_user_metrics(self, trials: List[Trial], limit: int = 4):
        """Try to infer the metrics to print out."""
        if len(self._inferred_metrics) >= limit:
            return self._inferred_metrics
        self._inferred_metrics = {}
        for t in trials:
            if not t.last_result:
                continue
            for metric, value in t.last_result.items():
                if metric not in self.DEFAULT_COLUMNS:
                    if metric not in AUTO_RESULT_KEYS:
                        if type(value) in self.VALID_SUMMARY_TYPES:
                            self._inferred_metrics[metric] = metric

                if len(self._inferred_metrics) >= limit:
                    return self._inferred_metrics
        return self._inferred_metrics

    def _current_best_trial(self, trials: List[Trial]):
        if not trials:
            return None, None

        metric, mode = self._metric, self._mode
        # If no metric has been set, see if exactly one has been reported
        # and use that one. `mode` must still be set.
        if not metric:
            if len(self._inferred_metrics) == 1:
                metric = list(self._inferred_metrics.keys())[0]

        if not metric or not mode:
            return None, metric

        metric_op = 1.0 if mode == "max" else -1.0
        best_metric = float("-inf")
        best_trial = None
        for t in trials:
            if not t.last_result:
                continue
            if metric not in t.last_result:
                continue
            if not best_metric or t.last_result[metric] * metric_op > best_metric:
                best_metric = t.last_result[metric] * metric_op
                best_trial = t
        return best_trial, metric


@PublicAPI
class JupyterNotebookReporter(TuneReporterBase):
    """Jupyter notebook-friendly Reporter that can update display in-place.

    Args:
        overwrite: Flag for overwriting the last reported progress.
        metric_columns: Names of metrics to
            include in progress table. If this is a dict, the keys should
            be metric names and the values should be the displayed names.
            If this is a list, the metric name is used directly.
        parameter_columns: Names of parameters to
            include in progress table. If this is a dict, the keys should
            be parameter names and the values should be the displayed names.
            If this is a list, the parameter name is used directly. If empty,
            defaults to all available parameters.
        max_progress_rows: Maximum number of rows to print
            in the progress table. The progress table describes the
            progress of each trial. Defaults to 20.
        max_error_rows: Maximum number of rows to print in the
            error table. The error table lists the error file, if any,
            corresponding to each trial. Defaults to 20.
        max_report_frequency: Maximum report frequency in seconds.
            Defaults to 5s.
        infer_limit: Maximum number of metrics to automatically infer
            from tune results.
        print_intermediate_tables: Print intermediate result
            tables. If None (default), will be set to True for verbosity
            levels above 3, otherwise False. If True, intermediate tables
            will be printed with experiment progress. If False, tables
            will only be printed at then end of the tuning run for verbosity
            levels greater than 2.
        metric: Metric used to determine best current trial.
        mode: One of [min, max]. Determines whether objective is
            minimizing or maximizing the metric attribute.
        sort_by_metric: Sort terminated trials by metric in the
            intermediate table. Defaults to False.
    """

    def __init__(
        self,
        overwrite: bool,
        metric_columns: Union[None, List[str], Dict[str, str]] = None,
        parameter_columns: Union[None, List[str], Dict[str, str]] = None,
        total_samples: Optional[int] = None,
        max_progress_rows: int = 20,
        max_error_rows: int = 20,
        max_report_frequency: int = 5,
        infer_limit: int = 3,
        print_intermediate_tables: Optional[bool] = None,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        sort_by_metric: bool = False,
    ):
        super(JupyterNotebookReporter, self).__init__(
            metric_columns,
            parameter_columns,
            total_samples,
            max_progress_rows,
            max_error_rows,
            max_report_frequency,
            infer_limit,
            print_intermediate_tables,
            metric,
            mode,
            sort_by_metric,
        )

        if not IS_NOTEBOOK:
            logger.warning(
                "You are using the `JupyterNotebookReporter`, but not "
                "IPython/Jupyter-compatible environment was detected. "
                "If this leads to unformatted output (e.g. like "
                "<IPython.core.display.HTML object>), consider passing "
                "a `CLIReporter` as the `progress_reporter` argument "
                "to `tune.run()` instead."
            )

        self._overwrite = overwrite
        self._output_queue = None

    def set_output_queue(self, queue: Queue):
        self._output_queue = queue

    def report(self, trials: List[Trial], done: bool, *sys_info: Dict):
        overwrite = self._overwrite
        progress_str = self._progress_str(
            trials, done, *sys_info, fmt="html", delim="<br>"
        )

        def update_output():
            from IPython.display import clear_output
            from IPython.core.display import display, HTML

            if overwrite:
                clear_output(wait=True)

            display(HTML(progress_str))

        if self._output_queue is not None:
            # If an output queue is set, send callable (e.g. when using
            # Ray client)
            self._output_queue.put(update_output)
        else:
            # Else, output directly
            update_output()


@PublicAPI
class CLIReporter(TuneReporterBase):
    """Command-line reporter

    Args:
        metric_columns: Names of metrics to
            include in progress table. If this is a dict, the keys should
            be metric names and the values should be the displayed names.
            If this is a list, the metric name is used directly.
        parameter_columns: Names of parameters to
            include in progress table. If this is a dict, the keys should
            be parameter names and the values should be the displayed names.
            If this is a list, the parameter name is used directly. If empty,
            defaults to all available parameters.
        max_progress_rows: Maximum number of rows to print
            in the progress table. The progress table describes the
            progress of each trial. Defaults to 20.
        max_error_rows: Maximum number of rows to print in the
            error table. The error table lists the error file, if any,
            corresponding to each trial. Defaults to 20.
        max_report_frequency: Maximum report frequency in seconds.
            Defaults to 5s.
        infer_limit: Maximum number of metrics to automatically infer
            from tune results.
        print_intermediate_tables: Print intermediate result
            tables. If None (default), will be set to True for verbosity
            levels above 3, otherwise False. If True, intermediate tables
            will be printed with experiment progress. If False, tables
            will only be printed at then end of the tuning run for verbosity
            levels greater than 2.
        metric: Metric used to determine best current trial.
        mode: One of [min, max]. Determines whether objective is
            minimizing or maximizing the metric attribute.
        sort_by_metric: Sort terminated trials by metric in the
            intermediate table. Defaults to False.
    """

    def __init__(
        self,
        metric_columns: Union[None, List[str], Dict[str, str]] = None,
        parameter_columns: Union[None, List[str], Dict[str, str]] = None,
        total_samples: Optional[int] = None,
        max_progress_rows: int = 20,
        max_error_rows: int = 20,
        max_report_frequency: int = 5,
        infer_limit: int = 3,
        print_intermediate_tables: Optional[bool] = None,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        sort_by_metric: bool = False,
    ):

        super(CLIReporter, self).__init__(
            metric_columns,
            parameter_columns,
            total_samples,
            max_progress_rows,
            max_error_rows,
            max_report_frequency,
            infer_limit,
            print_intermediate_tables,
            metric,
            mode,
            sort_by_metric,
        )

    def report(self, trials: List[Trial], done: bool, *sys_info: Dict):
        print(self._progress_str(trials, done, *sys_info))


def memory_debug_str():
    try:
        import ray  # noqa F401
        import psutil

        total_gb = psutil.virtual_memory().total / (1024 ** 3)
        used_gb = total_gb - psutil.virtual_memory().available / (1024 ** 3)
        if used_gb > total_gb * 0.9:
            warn = (
                ": ***LOW MEMORY*** less than 10% of the memory on "
                "this node is available for use. This can cause "
                "unexpected crashes. Consider "
                "reducing the memory used by your application "
                "or reducing the Ray object store size by setting "
                "`object_store_memory` when calling `ray.init`."
            )
        else:
            warn = ""
        return "Memory usage on this node: {}/{} GiB{}".format(
            round(used_gb, 1), round(total_gb, 1), warn
        )
    except ImportError:
        return "Unknown memory usage. Please run `pip install psutil` to resolve)"


def time_passed_str(start_time: float, current_time: float):
    current_time_dt = datetime.datetime.fromtimestamp(current_time)
    start_time_dt = datetime.datetime.fromtimestamp(start_time)
    delta: datetime.timedelta = current_time_dt - start_time_dt

    rest = delta.total_seconds()
    days = rest // (60 * 60 * 24)

    rest -= days * (60 * 60 * 24)
    hours = rest // (60 * 60)

    rest -= hours * (60 * 60)
    minutes = rest // 60

    seconds = rest - minutes * 60

    if days > 0:
        running_for_str = f"{days:.0f} days, "
    else:
        running_for_str = ""

    running_for_str += f"{hours:02.0f}:{minutes:02.0f}:{seconds:05.2f}"

    return (
        f"Current time: {current_time_dt:%Y-%m-%d %H:%M:%S} "
        f"(running for {running_for_str})"
    )


def _get_trials_by_state(trials: List[Trial]):
    trials_by_state = collections.defaultdict(list)
    for t in trials:
        trials_by_state[t.status].append(t)
    return trials_by_state


def trial_progress_str(
    trials: List[Trial],
    metric_columns: Union[List[str], Dict[str, str]],
    parameter_columns: Union[None, List[str], Dict[str, str]] = None,
    total_samples: int = 0,
    force_table: bool = False,
    fmt: str = "psql",
    max_rows: Optional[int] = None,
    done: bool = False,
    metric: Optional[str] = None,
    mode: Optional[str] = None,
    sort_by_metric: bool = False,
):
    """Returns a human readable message for printing to the console.

    This contains a table where each row represents a trial, its parameters
    and the current values of its metrics.

    Args:
        trials: List of trials to get progress string for.
        metric_columns: Names of metrics to include.
            If this is a dict, the keys are metric names and the values are
            the names to use in the message. If this is a list, the metric
            name is used in the message directly.
        parameter_columns: Names of parameters to
            include. If this is a dict, the keys are parameter names and the
            values are the names to use in the message. If this is a list,
            the parameter name is used in the message directly. If this is
            empty, all parameters are used in the message.
        total_samples: Total number of trials that will be generated.
        force_table: Force printing a table. If False, a table will
            be printed only at the end of the training for verbosity levels
            above `Verbosity.V2_TRIAL_NORM`.
        fmt: Output format (see tablefmt in tabulate API).
        max_rows: Maximum number of rows in the trial table. Defaults to
            unlimited.
        done: True indicates that the tuning run finished.
        metric: Metric used to sort trials.
        mode: One of [min, max]. Determines whether objective is
            minimizing or maximizing the metric attribute.
        sort_by_metric: Sort terminated trials by metric in the
            intermediate table. Defaults to False.
    """
    messages = []
    delim = "<br>" if fmt == "html" else "\n"
    if len(trials) < 1:
        return delim.join(messages)

    num_trials = len(trials)
    trials_by_state = _get_trials_by_state(trials)

    for local_dir in sorted({t.local_dir for t in trials}):
        messages.append("Result logdir: {}".format(local_dir))

    num_trials_strs = [
        "{} {}".format(len(trials_by_state[state]), state)
        for state in sorted(trials_by_state)
    ]

    if total_samples and total_samples >= sys.maxsize:
        total_samples = "infinite"

    messages.append(
        "Number of trials: {}{} ({})".format(
            num_trials,
            f"/{total_samples}" if total_samples else "",
            ", ".join(num_trials_strs),
        )
    )

    if force_table or (has_verbosity(Verbosity.V2_TRIAL_NORM) and done):
        messages += trial_progress_table(
            trials,
            metric_columns,
            parameter_columns,
            fmt,
            max_rows,
            metric,
            mode,
            sort_by_metric,
        )

    return delim.join(messages)


def trial_progress_table(
    trials: List[Trial],
    metric_columns: Union[List[str], Dict[str, str]],
    parameter_columns: Union[None, List[str], Dict[str, str]] = None,
    fmt: str = "psql",
    max_rows: Optional[int] = None,
    metric: Optional[str] = None,
    mode: Optional[str] = None,
    sort_by_metric: bool = False,
):
    messages = []
    num_trials = len(trials)
    trials_by_state = _get_trials_by_state(trials)

    # Sort terminated trials by metric and mode, descending if mode is "max"
    if sort_by_metric:
        trials_by_state[Trial.TERMINATED] = sorted(
            trials_by_state[Trial.TERMINATED],
            reverse=(mode == "max"),
            key=lambda t: t.last_result[metric],
        )

    state_tbl_order = [
        Trial.RUNNING,
        Trial.PAUSED,
        Trial.PENDING,
        Trial.TERMINATED,
        Trial.ERROR,
    ]
    max_rows = max_rows or float("inf")
    if num_trials > max_rows:
        # TODO(ujvl): suggestion for users to view more rows.
        trials_by_state_trunc = _fair_filter_trials(
            trials_by_state, max_rows, sort_by_metric
        )
        trials = []
        overflow_strs = []
        for state in state_tbl_order:
            if state not in trials_by_state:
                continue
            trials += trials_by_state_trunc[state]
            num = len(trials_by_state[state]) - len(trials_by_state_trunc[state])
            if num > 0:
                overflow_strs.append("{} {}".format(num, state))
        # Build overflow string.
        overflow = num_trials - max_rows
        overflow_str = ", ".join(overflow_strs)
    else:
        overflow = False
        overflow_str = ""
        trials = []
        for state in state_tbl_order:
            if state not in trials_by_state:
                continue
            trials += trials_by_state[state]

    # Pre-process trials to figure out what columns to show.
    if isinstance(metric_columns, Mapping):
        metric_keys = list(metric_columns.keys())
    else:
        metric_keys = metric_columns

    metric_keys = [
        k
        for k in metric_keys
        if any(
            unflattened_lookup(k, t.last_result, default=None) is not None
            for t in trials
        )
    ]

    if not parameter_columns:
        parameter_keys = sorted(set().union(*[t.evaluated_params for t in trials]))
    elif isinstance(parameter_columns, Mapping):
        parameter_keys = list(parameter_columns.keys())
    else:
        parameter_keys = parameter_columns

    # Build trial rows.
    trial_table = [
        _get_trial_info(trial, parameter_keys, metric_keys) for trial in trials
    ]
    # Format column headings
    if isinstance(metric_columns, Mapping):
        formatted_metric_columns = [metric_columns[k] for k in metric_keys]
    else:
        formatted_metric_columns = metric_keys
    if isinstance(parameter_columns, Mapping):
        formatted_parameter_columns = [parameter_columns[k] for k in parameter_keys]
    else:
        formatted_parameter_columns = parameter_keys
    columns = (
        ["Trial name", "status", "loc"]
        + formatted_parameter_columns
        + formatted_metric_columns
    )
    # Tabulate.
    messages.append(
        tabulate(trial_table, headers=columns, tablefmt=fmt, showindex=False)
    )
    if overflow:
        messages.append(
            "... {} more trials not shown ({})".format(overflow, overflow_str)
        )
    return messages


def trial_errors_str(
    trials: List[Trial], fmt: str = "psql", max_rows: Optional[int] = None
):
    """Returns a readable message regarding trial errors.

    Args:
        trials: List of trials to get progress string for.
        fmt: Output format (see tablefmt in tabulate API).
        max_rows: Maximum number of rows in the error table. Defaults to
            unlimited.
    """
    messages = []
    failed = [t for t in trials if t.error_file]
    num_failed = len(failed)
    if num_failed > 0:
        messages.append("Number of errored trials: {}".format(num_failed))
        if num_failed > (max_rows or float("inf")):
            messages.append(
                "Table truncated to {} rows ({} overflow)".format(
                    max_rows, num_failed - max_rows
                )
            )
        error_table = []
        for trial in failed[:max_rows]:
            row = [str(trial), trial.num_failures, trial.error_file]
            error_table.append(row)
        columns = ["Trial name", "# failures", "error file"]
        messages.append(
            tabulate(error_table, headers=columns, tablefmt=fmt, showindex=False)
        )
    delim = "<br>" if fmt == "html" else "\n"
    return delim.join(messages)


def best_trial_str(
    trial: Trial,
    metric: str,
    parameter_columns: Union[None, List[str], Dict[str, str]] = None,
):
    """Returns a readable message stating the current best trial."""
    val = trial.last_result[metric]
    config = trial.last_result.get("config", {})
    parameter_columns = parameter_columns or list(config.keys())
    if isinstance(parameter_columns, Mapping):
        parameter_columns = parameter_columns.keys()
    params = {p: unflattened_lookup(p, config) for p in parameter_columns}
    return (
        f"Current best trial: {trial.trial_id} with {metric}={val} and "
        f"parameters={params}"
    )


def _fair_filter_trials(
    trials_by_state: Dict[str, List[Trial]],
    max_trials: int,
    sort_by_metric: bool = False,
):
    """Filters trials such that each state is represented fairly.

    The oldest trials are truncated if necessary.

    Args:
        trials_by_state: Maximum number of trials to return.
    Returns:
        Dict mapping state to List of fairly represented trials.
    """
    num_trials_by_state = collections.defaultdict(int)
    no_change = False
    # Determine number of trials to keep per state.
    while max_trials > 0 and not no_change:
        no_change = True
        for state in sorted(trials_by_state):
            if num_trials_by_state[state] < len(trials_by_state[state]):
                no_change = False
                max_trials -= 1
                num_trials_by_state[state] += 1
    # Sort by start time, descending if the trails is not sorted by metric.
    sorted_trials_by_state = dict()
    for state in sorted(trials_by_state):
        if state == Trial.TERMINATED and sort_by_metric:
            sorted_trials_by_state[state] = trials_by_state[state]
        else:
            sorted_trials_by_state[state] = sorted(
                trials_by_state[state], reverse=False, key=lambda t: t.trial_id
            )
    # Truncate oldest trials.
    filtered_trials = {
        state: sorted_trials_by_state[state][: num_trials_by_state[state]]
        for state in sorted(trials_by_state)
    }
    return filtered_trials


def _get_trial_location(trial: Trial, result: dict) -> Location:
    # we get the location from the result, as the one in trial will be
    # reset when trial terminates
    node_ip, pid = result.get(NODE_IP, None), result.get(PID, None)
    if node_ip and pid:
        location = Location(node_ip, pid)
    else:
        # fallback to trial location if there hasn't been a report yet
        location = trial.location
    return location


def _get_trial_info(trial: Trial, parameters: List[str], metrics: List[str]):
    """Returns the following information about a trial:

    name | status | loc | params... | metrics...

    Args:
        trial: Trial to get information for.
        parameters: Names of trial parameters to include.
        metrics: Names of metrics to include.
    """
    result = trial.last_result
    config = trial.config
    location = _get_trial_location(trial, result)
    trial_info = [str(trial), trial.status, str(location)]
    trial_info += [
        unflattened_lookup(param, config, default=None) for param in parameters
    ]
    trial_info += [
        unflattened_lookup(metric, result, default=None) for metric in metrics
    ]
    return trial_info


@DeveloperAPI
class TrialProgressCallback(Callback):
    """Reports (prints) intermediate trial progress.

    This callback is automatically added to the callback stack. When a
    result is obtained, this callback will print the results according to
    the specified verbosity level.

    For ``Verbosity.V3_TRIAL_DETAILS``, a full result list is printed.

    For ``Verbosity.V2_TRIAL_NORM``, only one line is printed per received
    result.

    All other verbosity levels do not print intermediate trial progress.

    Result printing is throttled on a per-trial basis. Per default, results are
    printed only once every 30 seconds. Results are always printed when a trial
    finished or errored.

    """

    def __init__(self, metric: Optional[str] = None):
        self._last_print = collections.defaultdict(float)
        self._completed_trials = set()
        self._last_result_str = {}
        self._metric = metric

    def on_trial_result(
        self,
        iteration: int,
        trials: List["Trial"],
        trial: "Trial",
        result: Dict,
        **info,
    ):
        self.log_result(trial, result, error=False)

    def on_trial_error(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        self.log_result(trial, trial.last_result, error=True)

    def on_trial_complete(
        self, iteration: int, trials: List["Trial"], trial: "Trial", **info
    ):
        # Only log when we never logged that a trial was completed
        if trial not in self._completed_trials:
            self._completed_trials.add(trial)

            print_result_str = self._print_result(trial.last_result)
            last_result_str = self._last_result_str.get(trial, "")
            # If this is a new result, print full result string
            if print_result_str != last_result_str:
                self.log_result(trial, trial.last_result, error=False)
            else:
                print(f"Trial {trial} completed. " f"Last result: {print_result_str}")

    def log_result(self, trial: "Trial", result: Dict, error: bool = False):
        done = result.get("done", False) is True
        last_print = self._last_print[trial]
        if done and trial not in self._completed_trials:
            self._completed_trials.add(trial)
        if has_verbosity(Verbosity.V3_TRIAL_DETAILS) and (
            done or error or time.time() - last_print > DEBUG_PRINT_INTERVAL
        ):
            print("Result for {}:".format(trial))
            print("  {}".format(pretty_print(result).replace("\n", "\n  ")))
            self._last_print[trial] = time.time()
        elif has_verbosity(Verbosity.V2_TRIAL_NORM) and (
            done or error or time.time() - last_print > DEBUG_PRINT_INTERVAL
        ):
            info = ""
            if done:
                info = " This trial completed."

            metric_name = self._metric or "_metric"
            metric_value = result.get(metric_name, -99.0)

            print_result_str = self._print_result(result)

            self._last_result_str[trial] = print_result_str

            error_file = os.path.join(trial.logdir, "error.txt")

            if error:
                message = (
                    f"The trial {trial} errored with "
                    f"parameters={trial.config}. "
                    f"Error file: {error_file}"
                )
            elif self._metric:
                message = (
                    f"Trial {trial} reported "
                    f"{metric_name}={metric_value:.2f} "
                    f"with parameters={trial.config}.{info}"
                )
            else:
                message = (
                    f"Trial {trial} reported "
                    f"{print_result_str} "
                    f"with parameters={trial.config}.{info}"
                )

            print(message)
            self._last_print[trial] = time.time()

    def _print_result(self, result: Dict):
        print_result = result.copy()
        print_result.pop("config", None)
        print_result.pop("hist_stats", None)
        print_result.pop("trial_id", None)
        print_result.pop("experiment_tag", None)
        print_result.pop("done", None)
        for auto_result in AUTO_RESULT_KEYS:
            print_result.pop(auto_result, None)

        print_result_str = ",".join([f"{k}={v}" for k, v in print_result.items()])
        return print_result_str


def detect_reporter(**kwargs) -> TuneReporterBase:
    """Detect progress reporter class.

    Will return a :class:`JupyterNotebookReporter` if a IPython/Jupyter-like
    session was detected, and a :class:`CLIReporter` otherwise.

    Keyword arguments are passed on to the reporter class.
    """
    if IS_NOTEBOOK:
        kwargs.setdefault("overwrite", not has_verbosity(Verbosity.V2_TRIAL_NORM))
        progress_reporter = JupyterNotebookReporter(**kwargs)
    else:
        progress_reporter = CLIReporter(**kwargs)
    return progress_reporter
