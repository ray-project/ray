from __future__ import print_function

import collections
import sys

import numpy as np
import time

from ray.tune.result import (DEFAULT_METRIC, EPISODE_REWARD_MEAN,
                             MEAN_ACCURACY, MEAN_LOSS, TRAINING_ITERATION,
                             TIME_TOTAL_S, TIMESTEPS_TOTAL, AUTO_RESULT_KEYS)
from ray.tune.trial import Trial
from ray.tune.utils import unflattened_lookup

try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping

try:
    from tabulate import tabulate
except ImportError:
    raise ImportError("ray.tune in ray > 0.7.5 requires 'tabulate'. "
                      "Please re-run 'pip install ray[tune]' or "
                      "'pip install ray[rllib]'.")


class ProgressReporter:
    """Abstract class for experiment progress reporting.

    `should_report()` is called to determine whether or not `report()` should
    be called. Tune will call these functions after trial state transitions,
    receiving training results, and so on.
    """

    def should_report(self, trials, done=False):
        """Returns whether or not progress should be reported.

        Args:
            trials (list[Trial]): Trials to report on.
            done (bool): Whether this is the last progress report attempt.
        """
        raise NotImplementedError

    def report(self, trials, done, *sys_info):
        """Reports progress across trials.

        Args:
            trials (list[Trial]): Trials to report on.
            done (bool): Whether this is the last progress report attempt.
            sys_info: System info.
        """
        raise NotImplementedError


class TuneReporterBase(ProgressReporter):
    """Abstract base class for the default Tune reporters.

    If metric_columns is not overridden, Tune will attempt to automatically
    infer the metrics being outputted, up to 'infer_limit' number of
    metrics.

    Args:
        metric_columns (dict[str, str]|list[str]): Names of metrics to
            include in progress table. If this is a dict, the keys should
            be metric names and the values should be the displayed names.
            If this is a list, the metric name is used directly.
        parameter_columns (dict[str, str]|list[str]): Names of parameters to
            include in progress table. If this is a dict, the keys should
            be parameter names and the values should be the displayed names.
            If this is a list, the parameter name is used directly. If empty,
            defaults to all available parameters.
        max_progress_rows (int): Maximum number of rows to print
            in the progress table. The progress table describes the
            progress of each trial. Defaults to 20.
        max_error_rows (int): Maximum number of rows to print in the
            error table. The error table lists the error file, if any,
            corresponding to each trial. Defaults to 20.
        max_report_frequency (int): Maximum report frequency in seconds.
            Defaults to 5s.
        infer_limit (int): Maximum number of metrics to automatically infer
            from tune results.
        metric (str): Metric used to determine best current trial.
        mode (str): One of [min, max]. Determines whether objective is
            minimizing or maximizing the metric attribute.
    """

    # Truncated representations of column names (to accommodate small screens).
    DEFAULT_COLUMNS = collections.OrderedDict({
        MEAN_ACCURACY: "acc",
        MEAN_LOSS: "loss",
        TRAINING_ITERATION: "iter",
        TIME_TOTAL_S: "total time (s)",
        TIMESTEPS_TOTAL: "ts",
        EPISODE_REWARD_MEAN: "reward",
    })
    VALID_SUMMARY_TYPES = {
        int, float, np.float32, np.float64, np.int32, np.int64,
        type(None)
    }

    def __init__(self,
                 metric_columns=None,
                 parameter_columns=None,
                 total_samples=None,
                 max_progress_rows=20,
                 max_error_rows=20,
                 max_report_frequency=5,
                 infer_limit=3,
                 metric=None,
                 mode=None):
        self._total_samples = total_samples
        self._metrics_override = metric_columns is not None
        self._inferred_metrics = {}
        self._metric_columns = metric_columns or self.DEFAULT_COLUMNS.copy()
        self._parameter_columns = parameter_columns or []
        self._max_progress_rows = max_progress_rows
        self._max_error_rows = max_error_rows
        self._infer_limit = infer_limit

        self._max_report_freqency = max_report_frequency
        self._last_report_time = 0

        self._metric = metric
        self._mode = mode

    def set_search_properties(self, metric, mode):
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

    def set_total_samples(self, total_samples):
        self._total_samples = total_samples

    def should_report(self, trials, done=False):
        if time.time() - self._last_report_time > self._max_report_freqency:
            self._last_report_time = time.time()
            return True
        return done

    def add_metric_column(self, metric, representation=None):
        """Adds a metric to the existing columns.

        Args:
            metric (str): Metric to add. This must be a metric being returned
                in training step results.
            representation (str): Representation to use in table. Defaults to
                `metric`.
        """
        self._metrics_override = True
        if metric in self._metric_columns:
            raise ValueError("Column {} already exists.".format(metric))

        if isinstance(self._metric_columns, Mapping):
            representation = representation or metric
            self._metric_columns[metric] = representation
        else:
            if representation is not None and representation != metric:
                raise ValueError(
                    "`representation` cannot differ from `metric` "
                    "if this reporter was initialized with a list "
                    "of metric columns.")
            self._metric_columns.append(metric)

    def add_parameter_column(self, parameter, representation=None):
        """Adds a parameter to the existing columns.

        Args:
            parameter (str): Parameter to add. This must be a parameter
                specified in the configuration.
            representation (str): Representation to use in table. Defaults to
                `parameter`.
        """
        if parameter in self._parameter_columns:
            raise ValueError("Column {} already exists.".format(parameter))

        if isinstance(self._parameter_columns, Mapping):
            representation = representation or parameter
            self._parameter_columns[parameter] = representation
        else:
            if representation is not None and representation != parameter:
                raise ValueError(
                    "`representation` cannot differ from `parameter` "
                    "if this reporter was initialized with a list "
                    "of metric columns.")
            self._parameter_columns.append(parameter)

    def _progress_str(self, trials, done, *sys_info, fmt="psql", delim="\n"):
        """Returns full progress string.

        This string contains a progress table and error table. The progress
        table describes the progress of each trial. The error table lists
        the error file, if any, corresponding to each trial. The latter only
        exists if errors have occurred.

        Args:
            trials (list[Trial]): Trials to report on.
            done (bool): Whether this is the last progress report attempt.
            fmt (str): Table format. See `tablefmt` in tabulate API.
            delim (str): Delimiter between messages.
        """
        if not self._metrics_override:
            user_metrics = self._infer_user_metrics(trials, self._infer_limit)
            self._metric_columns.update(user_metrics)
        messages = ["== Status ==", memory_debug_str(), *sys_info]
        if done:
            max_progress = None
            max_error = None
        else:
            max_progress = self._max_progress_rows
            max_error = self._max_error_rows

        current_best_trial, metric = self._current_best_trial(trials)
        if current_best_trial:
            messages.append(
                best_trial_str(current_best_trial, metric,
                               self._parameter_columns))

        messages.append(
            trial_progress_str(
                trials,
                metric_columns=self._metric_columns,
                parameter_columns=self._parameter_columns,
                total_samples=self._total_samples,
                fmt=fmt,
                max_rows=max_progress))
        messages.append(trial_errors_str(trials, fmt=fmt, max_rows=max_error))

        return delim.join(messages) + delim

    def _infer_user_metrics(self, trials, limit=4):
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

    def _current_best_trial(self, trials):
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

        metric_op = 1. if mode == "max" else -1.
        best_metric = float("-inf")
        best_trial = None
        for t in trials:
            if not t.last_result:
                continue
            if metric not in t.last_result:
                continue
            if not best_metric or \
               t.last_result[metric] * metric_op > best_metric:
                best_metric = t.last_result[metric] * metric_op
                best_trial = t
        return best_trial, metric


class JupyterNotebookReporter(TuneReporterBase):
    """Jupyter notebook-friendly Reporter that can update display in-place.

    Args:
        overwrite (bool): Flag for overwriting the last reported progress.
        metric_columns (dict[str, str]|list[str]): Names of metrics to
            include in progress table. If this is a dict, the keys should
            be metric names and the values should be the displayed names.
            If this is a list, the metric name is used directly.
        parameter_columns (dict[str, str]|list[str]): Names of parameters to
            include in progress table. If this is a dict, the keys should
            be parameter names and the values should be the displayed names.
            If this is a list, the parameter name is used directly. If empty,
            defaults to all available parameters.
        max_progress_rows (int): Maximum number of rows to print
            in the progress table. The progress table describes the
            progress of each trial. Defaults to 20.
        max_error_rows (int): Maximum number of rows to print in the
            error table. The error table lists the error file, if any,
            corresponding to each trial. Defaults to 20.
        max_report_frequency (int): Maximum report frequency in seconds.
            Defaults to 5s.
    """

    def __init__(self,
                 overwrite,
                 metric_columns=None,
                 parameter_columns=None,
                 total_samples=None,
                 max_progress_rows=20,
                 max_error_rows=20,
                 max_report_frequency=5,
                 infer_limit=3,
                 metric=None,
                 mode=None):
        super(JupyterNotebookReporter,
              self).__init__(metric_columns, parameter_columns, total_samples,
                             max_progress_rows, max_error_rows,
                             max_report_frequency, infer_limit, metric, mode)
        self._overwrite = overwrite

    def report(self, trials, done, *sys_info):
        from IPython.display import clear_output
        from IPython.core.display import display, HTML
        if self._overwrite:
            clear_output(wait=True)
        progress_str = self._progress_str(
            trials, done, *sys_info, fmt="html", delim="<br>")
        display(HTML(progress_str))


class CLIReporter(TuneReporterBase):
    """Command-line reporter

    Args:
        metric_columns (dict[str, str]|list[str]): Names of metrics to
            include in progress table. If this is a dict, the keys should
            be metric names and the values should be the displayed names.
            If this is a list, the metric name is used directly.
        parameter_columns (dict[str, str]|list[str]): Names of parameters to
            include in progress table. If this is a dict, the keys should
            be parameter names and the values should be the displayed names.
            If this is a list, the parameter name is used directly. If empty,
            defaults to all available parameters.
        max_progress_rows (int): Maximum number of rows to print
            in the progress table. The progress table describes the
            progress of each trial. Defaults to 20.
        max_error_rows (int): Maximum number of rows to print in the
            error table. The error table lists the error file, if any,
            corresponding to each trial. Defaults to 20.
        max_report_frequency (int): Maximum report frequency in seconds.
            Defaults to 5s.
    """

    def __init__(self,
                 metric_columns=None,
                 parameter_columns=None,
                 total_samples=None,
                 max_progress_rows=20,
                 max_error_rows=20,
                 max_report_frequency=5,
                 infer_limit=3,
                 metric=None,
                 mode=None):

        super(CLIReporter,
              self).__init__(metric_columns, parameter_columns, total_samples,
                             max_progress_rows, max_error_rows,
                             max_report_frequency, infer_limit, metric, mode)

    def report(self, trials, done, *sys_info):
        print(self._progress_str(trials, done, *sys_info))


def memory_debug_str():
    try:
        import ray  # noqa F401
        import psutil
        total_gb = psutil.virtual_memory().total / (1024**3)
        used_gb = total_gb - psutil.virtual_memory().available / (1024**3)
        if used_gb > total_gb * 0.9:
            warn = (": ***LOW MEMORY*** less than 10% of the memory on "
                    "this node is available for use. This can cause "
                    "unexpected crashes. Consider "
                    "reducing the memory used by your application "
                    "or reducing the Ray object store size by setting "
                    "`object_store_memory` when calling `ray.init`.")
        else:
            warn = ""
        return "Memory usage on this node: {}/{} GiB{}".format(
            round(used_gb, 1), round(total_gb, 1), warn)
    except ImportError:
        return ("Unknown memory usage. Please run `pip install psutil` "
                "(or ray[debug]) to resolve)")


def trial_progress_str(trials,
                       metric_columns,
                       parameter_columns=None,
                       total_samples=0,
                       fmt="psql",
                       max_rows=None):
    """Returns a human readable message for printing to the console.

    This contains a table where each row represents a trial, its parameters
    and the current values of its metrics.

    Args:
        trials (list[Trial]): List of trials to get progress string for.
        metric_columns (dict[str, str]|list[str]): Names of metrics to include.
            If this is a dict, the keys are metric names and the values are
            the names to use in the message. If this is a list, the metric
            name is used in the message directly.
        parameter_columns (dict[str, str]|list[str]): Names of parameters to
            include. If this is a dict, the keys are parameter names and the
            values are the names to use in the message. If this is a list,
            the parameter name is used in the message directly. If this is
            empty, all parameters are used in the message.
        total_samples (int): Total number of trials that will be generated.
        fmt (str): Output format (see tablefmt in tabulate API).
        max_rows (int): Maximum number of rows in the trial table. Defaults to
            unlimited.
    """
    messages = []
    delim = "<br>" if fmt == "html" else "\n"
    if len(trials) < 1:
        return delim.join(messages)

    num_trials = len(trials)
    trials_by_state = collections.defaultdict(list)
    for t in trials:
        trials_by_state[t.status].append(t)

    for local_dir in sorted({t.local_dir for t in trials}):
        messages.append("Result logdir: {}".format(local_dir))

    num_trials_strs = [
        "{} {}".format(len(trials_by_state[state]), state)
        for state in sorted(trials_by_state)
    ]

    state_tbl_order = [
        Trial.RUNNING, Trial.PAUSED, Trial.PENDING, Trial.TERMINATED,
        Trial.ERROR
    ]

    max_rows = max_rows or float("inf")
    if num_trials > max_rows:
        # TODO(ujvl): suggestion for users to view more rows.
        trials_by_state_trunc = _fair_filter_trials(trials_by_state, max_rows)
        trials = []
        overflow_strs = []
        for state in state_tbl_order:
            if state not in trials_by_state:
                continue
            trials += trials_by_state_trunc[state]
            num = len(trials_by_state[state]) - len(
                trials_by_state_trunc[state])
            if num > 0:
                overflow_strs.append("{} {}".format(num, state))
        # Build overflow string.
        overflow = num_trials - max_rows
        overflow_str = ", ".join(overflow_strs)
    else:
        overflow = False
        trials = []
        for state in state_tbl_order:
            if state not in trials_by_state:
                continue
            trials += trials_by_state[state]

    if total_samples and total_samples >= sys.maxsize:
        total_samples = "infinite"

    messages.append("Number of trials: {}{} ({})".format(
        num_trials, f"/{total_samples}"
        if total_samples else "", ", ".join(num_trials_strs)))

    # Pre-process trials to figure out what columns to show.
    if isinstance(metric_columns, Mapping):
        metric_keys = list(metric_columns.keys())
    else:
        metric_keys = metric_columns
    metric_keys = [
        k for k in metric_keys if any(
            t.last_result.get(k) is not None for t in trials)
    ]

    if not parameter_columns:
        parameter_keys = sorted(
            set().union(*[t.evaluated_params for t in trials]))
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
        formatted_parameter_columns = [
            parameter_columns[k] for k in parameter_keys
        ]
    else:
        formatted_parameter_columns = parameter_keys
    columns = (["Trial name", "status", "loc"] + formatted_parameter_columns +
               formatted_metric_columns)
    # Tabulate.
    messages.append(
        tabulate(trial_table, headers=columns, tablefmt=fmt, showindex=False))
    if overflow:
        messages.append("... {} more trials not shown ({})".format(
            overflow, overflow_str))
    return delim.join(messages)


def trial_errors_str(trials, fmt="psql", max_rows=None):
    """Returns a readable message regarding trial errors.

    Args:
        trials (list[Trial]): List of trials to get progress string for.
        fmt (str): Output format (see tablefmt in tabulate API).
        max_rows (int): Maximum number of rows in the error table. Defaults to
            unlimited.
    """
    messages = []
    failed = [t for t in trials if t.error_file]
    num_failed = len(failed)
    if num_failed > 0:
        messages.append("Number of errored trials: {}".format(num_failed))
        if num_failed > (max_rows or float("inf")):
            messages.append("Table truncated to {} rows ({} overflow)".format(
                max_rows, num_failed - max_rows))
        error_table = []
        for trial in failed[:max_rows]:
            row = [str(trial), trial.num_failures, trial.error_file]
            error_table.append(row)
        columns = ["Trial name", "# failures", "error file"]
        messages.append(
            tabulate(
                error_table, headers=columns, tablefmt=fmt, showindex=False))
    delim = "<br>" if fmt == "html" else "\n"
    return delim.join(messages)


def best_trial_str(trial, metric, parameter_columns=None):
    """Returns a readable message stating the current best trial."""
    val = trial.last_result[metric]
    config = trial.last_result.get("config", {})
    parameter_columns = parameter_columns or list(config.keys())
    if isinstance(parameter_columns, Mapping):
        parameter_columns = parameter_columns.keys()
    params = {p: config.get(p) for p in parameter_columns}
    return f"Current best trial: {trial.trial_id} with {metric}={val} and " \
           f"parameters={params}"


def _fair_filter_trials(trials_by_state, max_trials):
    """Filters trials such that each state is represented fairly.

    The oldest trials are truncated if necessary.

    Args:
        trials_by_state (dict[str, list[Trial]]: Trials by state.
        max_trials (int): Maximum number of trials to return.
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
    # Sort by start time, descending.
    sorted_trials_by_state = {
        state: sorted(
            trials_by_state[state], reverse=False, key=lambda t: t.trial_id)
        for state in sorted(trials_by_state)
    }
    # Truncate oldest trials.
    filtered_trials = {
        state: sorted_trials_by_state[state][:num_trials_by_state[state]]
        for state in sorted(trials_by_state)
    }
    return filtered_trials


def _get_trial_info(trial, parameters, metrics):
    """Returns the following information about a trial:

    name | status | loc | params... | metrics...

    Args:
        trial (Trial): Trial to get information for.
        parameters (list[str]): Names of trial parameters to include.
        metrics (list[str]): Names of metrics to include.
    """
    result = trial.last_result
    config = trial.config
    trial_info = [str(trial), trial.status, str(trial.location)]
    trial_info += [
        unflattened_lookup(param, config, default=None) for param in parameters
    ]
    trial_info += [
        unflattened_lookup(metric, result, default=None) for metric in metrics
    ]
    return trial_info
