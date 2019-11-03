from __future__ import print_function

import os

from ray.tune.result import (DEFAULT_RESULT_KEYS, CONFIG_PREFIX, PID,
                             EPISODE_REWARD_MEAN, MEAN_ACCURACY, MEAN_LOSS,
                             HOSTNAME, TRAINING_ITERATION, TIME_TOTAL_S,
                             TIMESTEPS_TOTAL)
from ray.tune.util import flatten_dict

try:
    from tabulate import tabulate
except ImportError:
    raise ImportError("ray.tune in ray > 0.7.5 requires 'tabulate'. "
                      "Please re-run 'pip install ray[tune]' or "
                      "'pip install ray[rllib]'.")

DEFAULT_PROGRESS_KEYS = DEFAULT_RESULT_KEYS + (EPISODE_REWARD_MEAN, )
# Truncated representations of column names (to accommodate small screens).
REPORTED_REPRESENTATIONS = {
    EPISODE_REWARD_MEAN: "reward",
    MEAN_ACCURACY: "acc",
    MEAN_LOSS: "loss",
    TIME_TOTAL_S: "total time (s)",
    TIMESTEPS_TOTAL: "timesteps",
    TRAINING_ITERATION: "iter",
}


class ProgressReporter(object):
    def report(self, trial_runner):
        """Reports progress across all trials of the trial runner.

        Args:
            trial_runner: Trial runner to report on.
        """
        raise NotImplementedError


class JupyterNotebookReporter(ProgressReporter):
    def __init__(self, overwrite):
        """Initializes a new JupyterNotebookReporter.

        Args:
            overwrite (bool): Flag for overwriting the last reported progress.
        """
        self.overwrite = overwrite

    def report(self, trial_runner):
        delim = "<br>"
        messages = [
            "== Status ==",
            memory_debug_str(),
            trial_runner.debug_string(delim=delim),
            trial_progress_str(trial_runner.get_trials(), fmt="html")
        ]
        from IPython.display import clear_output
        from IPython.core.display import display, HTML
        if self.overwrite:
            clear_output(wait=True)
        display(HTML(delim.join(messages) + delim))


class CLIReporter(ProgressReporter):
    def report(self, trial_runner):
        messages = [
            "== Status ==",
            memory_debug_str(),
            trial_runner.debug_string(),
            trial_progress_str(trial_runner.get_trials())
        ]
        print("\n".join(messages) + "\n")


def memory_debug_str():
    try:
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


def trial_progress_str(trials, metrics=None, fmt="psql", max_rows=100):
    """Returns a human readable message for printing to the console.

    This contains a table where each row represents a trial, its parameters
    and the current values of its metrics.

    Args:
        trials (List[Trial]): List of trials to get progress string for.
        metrics (List[str]): Names of metrics to include. Defaults to
            metrics defined in DEFAULT_RESULT_KEYS.
        fmt (str): Output format (see tablefmt in tabulate API).
        max_rows (int): Maximum number of rows in the trial table.
    """
    messages = []
    delim = "<br>" if fmt == "html" else "\n"
    if len(trials) < 1:
        return delim.join(messages)

    num_trials = len(trials)
    trials_per_state = {}
    for t in trials:
        trials_per_state[t.status] = trials_per_state.get(t.status, 0) + 1
    messages.append("Number of trials: {} ({})".format(num_trials,
                                                       trials_per_state))
    for local_dir in sorted({t.local_dir for t in trials}):
        messages.append("Result logdir: {}".format(local_dir))

    if num_trials > max_rows:
        overflow = num_trials - max_rows
        # TODO(ujvl): suggestion for users to view more rows.
        messages.append("Table truncated to {} rows ({} overflow).".format(
            max_rows, overflow))

    # Pre-process trials to figure out what columns to show.
    keys = list(metrics or DEFAULT_PROGRESS_KEYS)
    keys = [k for k in keys if any(t.last_result.get(k) for t in trials)]
    has_failed = any(t.error_file for t in trials)
    # Build rows.
    trial_table = []
    params = list(set().union(*[t.evaluated_params for t in trials]))
    for trial in trials[:min(num_trials, max_rows)]:
        trial_table.append(_get_trial_info(trial, params, keys, has_failed))
    # Parse columns.
    parsed_columns = [REPORTED_REPRESENTATIONS.get(k, k) for k in keys]
    columns = ["Trial name", "status", "loc"]
    columns += ["failures", "error file"] if has_failed else []
    columns += params + parsed_columns
    messages.append(
        tabulate(trial_table, headers=columns, tablefmt=fmt, showindex=False))
    return delim.join(messages)


def _get_trial_info(trial, parameters, metrics, include_error_data=False):
    """Returns the following information about a trial:

    name | status | loc | # failures | error_file | params... | metrics...

    Args:
        trial (Trial): Trial to get information for.
        parameters (List[str]): Names of trial parameters to include.
        metrics (List[str]): Names of metrics to include.
        include_error_data (bool): Include error file and # of failures.
    """
    result = flatten_dict(trial.last_result)
    trial_info = [str(trial), trial.status]
    trial_info += [_location_str(result.get(HOSTNAME), result.get(PID))]
    if include_error_data:
        # TODO(ujvl): File path is too long to display in a single row.
        trial_info += [trial.num_failures, trial.error_file]
    trial_info += [result.get(CONFIG_PREFIX + param) for param in parameters]
    trial_info += [result.get(metric) for metric in metrics]
    return trial_info


def _location_str(hostname, pid):
    if not pid:
        return ""
    elif hostname == os.uname()[1]:
        return "pid={}".format(pid)
    else:
        return "{}:{}".format(hostname, pid)
