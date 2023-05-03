from typing import List, Dict, Optional, Tuple, Any, TYPE_CHECKING, Collection

import contextlib
import collections
from dataclasses import dataclass
import datetime
from enum import IntEnum
import logging
import math
import numbers
import numpy as np
import os
import pandas as pd
import textwrap
import time

try:
    import rich
    import rich.layout
    import rich.live
except ImportError:
    rich = None

import ray
from ray._private.dict import unflattened_lookup
from ray._private.thirdparty.tabulate.tabulate import (
    tabulate,
    TableFormat,
    Line,
    DataRow,
)
from ray.air._internal.checkpoint_manager import _TrackedCheckpoint
from ray.tune.callback import Callback
from ray.tune.result import (
    AUTO_RESULT_KEYS,
    EPISODE_REWARD_MEAN,
    MEAN_ACCURACY,
    MEAN_LOSS,
    TIME_TOTAL_S,
    TIMESTEPS_TOTAL,
    TRAINING_ITERATION,
)
from ray.tune.experiment.trial import Trial

if TYPE_CHECKING:
    from ray.tune.stopper import Stopper

logger = logging.getLogger(__name__)

# defines the mapping of the key in result and the key to be printed in table.
# Note this is ordered!
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

# The order of summarizing trials.
ORDER = [
    Trial.RUNNING,
    Trial.TERMINATED,
    Trial.PAUSED,
    Trial.PENDING,
    Trial.ERROR,
]


class AirVerbosity(IntEnum):
    SILENT = 0
    DEFAULT = 1
    VERBOSE = 2


IS_NOTEBOOK = ray.widgets.util.in_notebook()


def get_air_verbosity() -> Optional[AirVerbosity]:
    verbosity = os.environ.get("AIR_VERBOSITY", None)
    if verbosity:
        return AirVerbosity(int(verbosity)) if verbosity else None


def _get_time_str(start_time: float, current_time: float) -> Tuple[str, str]:
    """Get strings representing the current and elapsed time.

    Args:
        start_time: POSIX timestamp of the start of the tune run
        current_time: POSIX timestamp giving the current time

    Returns:
        Current time and elapsed time for the current run
    """
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

    return f"{current_time_dt:%Y-%m-%d %H:%M:%S}", running_for_str


def _get_trials_by_state(trials: List[Trial]) -> Dict[str, List[Trial]]:
    trials_by_state = collections.defaultdict(list)
    for t in trials:
        trials_by_state[t.status].append(t)
    return trials_by_state


def _infer_user_metrics(trials: List[Trial], limit: int = 4) -> List[str]:
    """Try to infer the metrics to print out.

    By default, only the first 4 meaningful metrics in `last_result` will be
    inferred as user implied metrics.
    """
    # Using OrderedDict for OrderedSet.
    result = collections.OrderedDict()
    for t in trials:
        if not t.last_result:
            continue
        for metric, value in t.last_result.items():
            if metric not in DEFAULT_COLUMNS:
                if metric not in AUTO_RESULT_KEYS:
                    if type(value) in VALID_SUMMARY_TYPES:
                        result[metric] = ""  # not important

            if len(result) >= limit:
                return list(result.keys())
    return list(result.keys())


def _current_best_trial(
    trials: List[Trial], metric: Optional[str], mode: Optional[str]
) -> Tuple[Optional[Trial], Optional[str]]:
    """
    Returns the best trial and the metric key. If anything is empty or None,
    returns a trivial result of None, None.

    Args:
        trials: List of trials.
        metric: Metric that trials are being ranked.
        mode: One of "min" or "max".

    Returns:
         Best trial and the metric key.
    """
    if not trials or not metric or not mode:
        return None, None

    metric_op = 1.0 if mode == "max" else -1.0
    best_metric = float("-inf")
    best_trial = None
    for t in trials:
        if not t.last_result:
            continue
        metric_value = unflattened_lookup(metric, t.last_result, default=None)
        if pd.isnull(metric_value):
            continue
        if not best_trial or metric_value * metric_op > best_metric:
            best_metric = metric_value * metric_op
            best_trial = t
    return best_trial, metric


@dataclass
class _PerStatusTrialTableData:
    trial_infos: List[List[str]]
    more_info: str


@dataclass
class _TrialTableData:
    header: List[str]
    data: List[_PerStatusTrialTableData]


def _max_len(value: Any, max_len: int = 20, wrap: bool = False) -> Any:
    """Abbreviate a string representation of an object to `max_len` characters.

    For numbers, booleans and None, the original value will be returned for
    correct rendering in the table formatting tool.

    Args:
        value: Object to be represented as a string.
        max_len: Maximum return string length.
    """
    if value is None or isinstance(value, (int, float, numbers.Number, bool)):
        return value

    string = str(value)
    if len(string) <= max_len:
        return string

    if wrap:
        # Maximum two rows.
        # Todo: Make this configurable in the refactor
        if len(value) > max_len * 2:
            value = "..." + string[(3 - (max_len * 2)) :]

        wrapped = textwrap.wrap(value, width=max_len)
        return "\n".join(wrapped)

    result = "..." + string[(3 - max_len) :]
    return result


def _get_trial_info(trial: Trial, metric_keys: List[str]) -> List[str]:
    """Returns the following information about a trial:

    name | status | metrics...

    Args:
        trial: Trial to get information for.
        metric_keys: Names of metrics to include.
    """
    result = trial.last_result
    trial_info = [str(trial), trial.status]
    trial_info.extend(
        [
            _max_len(
                unflattened_lookup(metric, result, default=None),
            )
            for metric in metric_keys
        ]
    )
    return trial_info


def _get_trial_table_data_per_status(
    status: str,
    trials: List[Trial],
    metric_keys: List[str],
    force_max_rows: bool = False,
) -> Optional[_PerStatusTrialTableData]:
    """Gather all information of trials pertained to one `status`.

    Args:
        status: The trial status of interest.
        trials: all the trials of that status.
        metric_keys: *Ordered* list of metrics to be displayed in the table.
            Including both default and user defined.
        force_max_rows: Whether or not to enforce a max row number for this status.
            If True, only a max of `5` rows will be shown.

    Returns:
        All information of trials pertained to the `status`.
    """
    # TODO: configure it.
    max_row = 5 if force_max_rows else math.inf
    if not trials:
        return None

    trial_infos = list()
    more_info = None
    for t in trials:
        if len(trial_infos) >= max_row:
            more_info = f"... and {str(len(trials) - max_row)} more {status} ..."
            break
        trial_infos.append(_get_trial_info(t, metric_keys))
    return _PerStatusTrialTableData(trial_infos, more_info)


def _get_trial_table_data(
    trials: List[Trial],
    metric_keys: List[str],
) -> _TrialTableData:
    """Generate a table showing the current progress of tuning trials.

    Args:
        trials: List of trials for which progress is to be shown.
        metric_keys: Ordered list of metrics to be displayed in the table.
            Including both default and user defined.
            Will only be shown if at least one trial is having the key.

    Returns:
        Trial table data, including header and trial table per each status.
    """
    # TODO: configure
    max_trial_num_to_show = 20
    max_column_length = 20
    trials_by_state = _get_trials_by_state(trials)

    # get the right metric to show.
    metric_keys = [
        k
        for k in metric_keys
        if any(
            unflattened_lookup(k, t.last_result, default=None) is not None
            for t in trials
        )
    ]

    # get header from metric keys
    formatted_metric_columns = [
        _max_len(k, max_len=max_column_length, wrap=True) for k in metric_keys
    ]
    # Map to the abbreviated version if necessary.
    header = ["Trial name", "status"] + [
        DEFAULT_COLUMNS[key] if key in DEFAULT_COLUMNS else key
        for key in formatted_metric_columns
    ]

    trial_data = list()
    for t_status in ORDER:
        trial_data_per_status = _get_trial_table_data_per_status(
            t_status,
            trials_by_state[t_status],
            metric_keys=formatted_metric_columns,
            force_max_rows=len(trials) > max_trial_num_to_show,
        )
        if trial_data_per_status:
            trial_data.append(trial_data_per_status)
    return _TrialTableData(header, trial_data)


def _best_trial_str(
    trial: Trial,
    metric: str,
):
    """Returns a readable message stating the current best trial."""
    # returns something like
    # Current best trial: 18ae7_00005 with loss=0.5918508041056858 and params={'train_loop_config': {'lr': 0.059253447253394785}}. # noqa
    val = unflattened_lookup(metric, trial.last_result, default=None)
    config = trial.last_result.get("config", {})
    parameter_columns = list(config.keys())
    params = {p: unflattened_lookup(p, config) for p in parameter_columns}
    return (
        f"Current best trial: {trial.trial_id} with {metric}={val} and "
        f"params={params}"
    )


def _render_table_item(key: str, item: Any, prefix: str = ""):
    key = prefix + key
    if isinstance(item, float):
        # tabulate does not work well with mixed-type columns, so we format
        # numbers ourselves.
        yield key, f"{item:.5f}".rstrip("0")
    elif isinstance(item, list):
        yield key, None
        for sv in item:
            yield from _render_table_item("", sv, prefix=prefix + "-")
    elif isinstance(item, Dict):
        yield key, None
        for sk, sv in item.items():
            yield from _render_table_item(str(sk), sv, prefix=prefix + "/")
    else:
        yield key, item


def _get_dict_as_table_data(
    data: Dict,
    exclude: Optional[Collection] = None,
    upper_keys: Optional[Collection] = None,
):
    exclude = exclude or set()
    upper_keys = upper_keys or set()

    upper = []
    lower = []

    for key, value in sorted(data.items()):
        if key in exclude:
            continue

        for k, v in _render_table_item(str(key), value):
            if key in upper_keys:
                upper.append([k, v])
            else:
                lower.append([k, v])

    if not upper:
        return lower
    elif not lower:
        return upper
    else:
        return upper + lower


# Copied/adjusted from tabulate
AIR_TABULATE_TABLEFMT = TableFormat(
    lineabove=Line("╭", "─", "─", "╮"),
    linebelowheader=Line("├", "─", "─", "┤"),
    linebetweenrows=None,
    linebelow=Line("╰", "─", "─", "╯"),
    headerrow=DataRow("│", " ", "│"),
    datarow=DataRow("│", " ", "│"),
    padding=1,
    with_header_hide=None,
)


def _print_dict_as_table(
    data: Dict,
    header: Optional[str] = None,
    exclude: Optional[Collection] = None,
    division: Optional[Collection] = None,
):
    table_data = _get_dict_as_table_data(
        data=data, exclude=exclude, upper_keys=division
    )

    headers = [header, ""] if header else []

    if not table_data:
        return

    print(
        tabulate(
            table_data,
            headers=headers,
            colalign=("left", "right"),
            tablefmt=AIR_TABULATE_TABLEFMT,
        )
    )


class ProgressReporter:
    """Periodically prints out status update."""

    # TODO: Make this configurable
    _heartbeat_freq = 30  # every 30 sec
    # to be updated by subclasses.
    _heartbeat_threshold = None

    def __init__(self, verbosity: AirVerbosity):
        """

        Args:
            verbosity: AirVerbosity level.
        """
        self._verbosity = verbosity
        self._start_time = time.time()
        self._last_heartbeat_time = 0

    @property
    def _time_heartbeat_str(self):
        current_time_str, running_for_str = _get_time_str(self._start_time, time.time())
        return f"Current time: {current_time_str} " f"(running for {running_for_str})"

    def print_heartbeat(self, trials, *args, force: bool = False):
        if self._verbosity < self._heartbeat_threshold:
            return
        if force or time.time() - self._last_heartbeat_time > self._heartbeat_freq:
            self._print_heartbeat(trials, *args)
            self._last_heartbeat_time = time.time()

    def _print_heartbeat(self, trials, *args):
        raise NotImplementedError


def _detect_reporter(
    verbosity: AirVerbosity,
    num_samples: int,
    metric: Optional[str] = None,
    mode: Optional[str] = None,
):
    # TODO: Add JupyterNotebook and Ray Client case later.
    rich_enabled = "ENABLE_RICH" in os.environ
    if num_samples and num_samples > 1:
        if rich_enabled:
            if not rich:
                raise ImportError("Please run `pip install rich`. ")
            reporter = TuneRichReporter(verbosity, num_samples, metric, mode)
        else:
            reporter = TuneTerminalReporter(verbosity, num_samples, metric, mode)
    else:
        if rich_enabled:
            logger.warning("`ENABLE_RICH` is only effective with Tune usecase.")
        reporter = TrainReporter(verbosity)
    return reporter


class TuneReporterBase(ProgressReporter):
    _heartbeat_threshold = AirVerbosity.DEFAULT

    def __init__(
        self,
        verbosity: AirVerbosity,
        num_samples: int,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
    ):
        self._num_samples = num_samples
        self._metric = metric
        self._mode = mode
        # will be populated when first result comes in.
        self._inferred_metric = None
        super(TuneReporterBase, self).__init__(verbosity=verbosity)

    def _get_overall_trial_progress_str(self, trials):
        result = " | ".join(
            [
                f"{len(trials)} {status}"
                for status, trials in _get_trials_by_state(trials).items()
            ]
        )
        return f"Trial status: {result}"

    # TODO: Return a more structured type to share code with Jupyter flow.
    def _get_heartbeat(self, trials, *sys_args) -> Tuple[List[str], _TrialTableData]:
        result = list()
        # Trial status: 1 RUNNING | 7 PENDING
        result.append(self._get_overall_trial_progress_str(trials))
        # Current time: 2023-02-24 12:35:39 (running for 00:00:37.40)
        result.append(self._time_heartbeat_str)
        # Logical resource usage: 8.0/64 CPUs, 0/0 GPUs
        result.extend(sys_args)
        # Current best trial: TRIAL NAME, metrics: {...}, parameters: {...}
        current_best_trial, metric = _current_best_trial(
            trials, self._metric, self._mode
        )
        if current_best_trial:
            result.append(_best_trial_str(current_best_trial, metric))
        # Now populating the trial table data.
        if not self._inferred_metric:
            # try inferring again.
            self._inferred_metric = _infer_user_metrics(trials)

        all_metrics = list(DEFAULT_COLUMNS.keys()) + self._inferred_metric

        trial_table_data = _get_trial_table_data(trials, all_metrics)
        return result, trial_table_data

    def _print_heartbeat(self, trials, *sys_args):
        raise NotImplementedError


class TuneTerminalReporter(TuneReporterBase):
    def _print_heartbeat(self, trials, *sys_args):
        if self._verbosity < self._heartbeat_threshold:
            return
        heartbeat_strs, table_data = self._get_heartbeat(trials, *sys_args)
        for s in heartbeat_strs:
            print(s)
        # now print the table using Tabulate
        all_infos = []
        header = table_data.header
        table_data_list = table_data.data
        for table in table_data_list:
            all_infos.extend(table.trial_infos)
            if table.more_info:
                all_infos.append(table.more_info)
        print(
            tabulate(
                all_infos,
                headers=header,
                tablefmt="simple",
                showindex=False,
            )
        )
        print()


class TuneRichReporter(TuneReporterBase):
    def __init__(
        self,
        verbosity: AirVerbosity,
        num_samples: int,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
    ):
        super().__init__(verbosity, num_samples, metric, mode)
        self._live = None

    # since sticky table, we can afford to do that more often.
    _heartbeat_freq = 5

    @contextlib.contextmanager
    def with_live(self):
        with rich.live.Live(
            refresh_per_second=4, redirect_stdout=True, redirect_stderr=True
        ) as live:
            self._live = live
            yield
            self._live = None

    def _render_layout(self, heartbeat_strs: List[str], table_data: _TrialTableData):
        # generate a nested table, the top table will write some basic info
        # and the bottom table shows trial status.
        table = rich.table.Table(
            show_header=False,
            show_edge=False,
            show_lines=False,
        )
        table_basic_info = rich.table.Table(
            show_header=False,
            show_edge=False,
            show_lines=False,
        )
        for s in heartbeat_strs:
            table_basic_info.add_row(str(s))
        table_trial = rich.table.Table(
            box=rich.box.SQUARE,
            expand=True,
            show_header=False,
            title=":glowing_star: Ray Tune Trial Status Table :glowing_star:",
        )
        header = table_data.header
        table_data = table_data.data
        for _ in header:
            table_trial.add_column(overflow="fold")
        table_trial.add_row(*header)
        for per_status_info in table_data:
            trial_infos = per_status_info.trial_infos
            more_info = per_status_info.more_info
            for trial_info in trial_infos:
                table_trial.add_row(*[str(_) for _ in trial_info])
            if more_info:
                table_trial.add_row(more_info)
        table.add_row(table_basic_info)
        table.add_row(table_trial)

        self._live.update(table)

    def _print_heartbeat(self, trials, *args):
        if not rich:
            return
        if not self._live:
            logger.warning(
                "`print_heartbeat` is not supposed to "
                "be called without `with_live` context manager."
            )
            return
        heartbeat_strs, table_data = self._get_heartbeat(trials, *args)
        self._render_layout(heartbeat_strs, table_data)


class TrainReporter(ProgressReporter):
    # the minimal verbosity threshold at which heartbeat starts getting printed.
    _heartbeat_threshold = AirVerbosity.VERBOSE

    def _get_heartbeat(self, trials: List[Trial]):
        # Training on iteration 1. Current time: 2023-03-22 15:29:25 (running for 00:00:03.24)  # noqa
        if len(trials) == 0:
            return
        trial = trials[0]
        if trial.status != Trial.RUNNING:
            return " ".join(
                [f"Training is in {trial.status} status.", self._time_heartbeat_str]
            )
        if not trial.last_result or TRAINING_ITERATION not in trial.last_result:
            iter_num = 1
        else:
            iter_num = trial.last_result[TRAINING_ITERATION] + 1
        return " ".join(
            [f"Training on iteration {iter_num}.", self._time_heartbeat_str]
        )

    def _print_heartbeat(self, trials, *args):
        print(self._get_heartbeat(trials))


# These keys are blacklisted for printing out training/tuning intermediate/final result!
BLACKLISTED_KEYS = {
    "config",
    "date",
    "done",
    "hostname",
    "iterations_since_restore",
    "node_ip",
    "pid",
    "time_since_restore",
    "timestamp",
    "trial_id",
    "experiment_tag",
    "should_checkpoint",
}


class AirResultCallbackWrapper(Callback):
    # This is only to bypass the issue that by the time default callbacks
    # are added, there is no information on `num_samples` yet.
    def __init__(self, verbosity):
        self._verbosity = verbosity
        self._callback = None

    def setup(
        self,
        stop: Optional["Stopper"] = None,
        num_samples: Optional[int] = None,
        total_num_samples: Optional[int] = None,
        **info,
    ):
        self._callback = (
            TuneResultProgressCallback(self._verbosity)
            if total_num_samples > 1
            else TrainResultProgressCallback(self._verbosity)
        )

    # everything ELSE is just passing through..
    def __getattribute__(self, attr):
        proxy_through_keys = {
            "on_trial_result",
            "on_trial_complete",
            "on_checkpoint",
            "on_trial_start",
        }
        if attr not in proxy_through_keys:
            return super().__getattribute__(attr)
        return getattr(self._callback, attr)


class AirResultProgressCallback(Callback):
    # to be filled in by subclasses.
    _start_end_verbosity = None
    _intermediate_result_verbosity = None
    _addressing_tmpl = None

    def __init__(self, verbosity):
        self._verbosity = verbosity
        self._start_time = time.time()
        self._trial_last_printed_results = {}

    def _print_result(self, trial, result: Optional[Dict] = None, force: bool = False):
        """Only print result if a different result has been reported, or force=True"""
        result = result or trial.last_result

        last_result_iter = self._trial_last_printed_results.get(trial.trial_id, -1)
        this_iter = result.get(TRAINING_ITERATION, 0)

        if this_iter != last_result_iter or force:
            _print_dict_as_table(
                result,
                header=f"{self._addressing_tmpl.format(trial)} result",
                exclude=BLACKLISTED_KEYS,
                division=AUTO_RESULT_KEYS,
            )
            self._trial_last_printed_results[trial.trial_id] = this_iter

    def _print_config(self, trial):
        _print_dict_as_table(
            trial.config, header=f"{self._addressing_tmpl.format(trial)} config"
        )

    def on_trial_result(
        self,
        iteration: int,
        trials: List[Trial],
        trial: Trial,
        result: Dict,
        **info,
    ):
        if self._verbosity < self._intermediate_result_verbosity:
            return
        curr_time, running_time = _get_time_str(self._start_time, time.time())
        print(
            f"{self._addressing_tmpl.format(trial)} "
            f"finished iteration {result[TRAINING_ITERATION]} "
            f"at {curr_time} (running for {running_time})."
        )
        self._print_result(trial, result)

    def on_trial_complete(
        self, iteration: int, trials: List[Trial], trial: Trial, **info
    ):
        if self._verbosity < self._start_end_verbosity:
            return
        curr_time, running_time = _get_time_str(self._start_time, time.time())
        finished_iter = 0
        if trial.last_result and TRAINING_ITERATION in trial.last_result:
            finished_iter = trial.last_result[TRAINING_ITERATION]
        print(
            f"{self._addressing_tmpl.format(trial)} "
            f"completed training after {finished_iter} iterations "
            f"at {curr_time} (running for {running_time})."
        )
        self._print_result(trial)

    def on_checkpoint(
        self,
        iteration: int,
        trials: List[Trial],
        trial: Trial,
        checkpoint: "_TrackedCheckpoint",
        **info,
    ):
        if self._verbosity < self._intermediate_result_verbosity:
            return
        # don't think this is supposed to happen but just to be save.
        saved_iter = "?"
        if trial.last_result and TRAINING_ITERATION in trial.last_result:
            saved_iter = trial.last_result[TRAINING_ITERATION]
        print(
            f"{self._addressing_tmpl.format(trial)} "
            f"saved a checkpoint for iteration {saved_iter} "
            f"at: {checkpoint.dir_or_data}"
        )

    def on_trial_start(self, iteration: int, trials: List[Trial], trial: Trial, **info):
        if self._verbosity < self._start_end_verbosity:
            return
        has_config = bool(trial.config)

        if has_config:
            print(
                f"{self._addressing_tmpl.format(trial)} " f"started with configuration:"
            )
            self._print_config(trial)
        else:
            print(
                f"{self._addressing_tmpl.format(trial)} "
                f"started without custom configuration."
            )


class TuneResultProgressCallback(AirResultProgressCallback):
    _intermediate_result_verbosity = AirVerbosity.VERBOSE
    _start_end_verbosity = AirVerbosity.DEFAULT
    _addressing_tmpl = "Trial {}"


class TrainResultProgressCallback(AirResultProgressCallback):
    _intermediate_result_verbosity = AirVerbosity.DEFAULT
    _start_end_verbosity = AirVerbosity.DEFAULT
    _addressing_tmpl = "Training"
