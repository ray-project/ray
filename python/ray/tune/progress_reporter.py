from __future__ import print_function

import collections

from ray.tune.result import (DEFAULT_RESULT_KEYS, CONFIG_PREFIX,
                             EPISODE_REWARD_MEAN, MEAN_ACCURACY, MEAN_LOSS,
                             TRAINING_ITERATION, TIME_TOTAL_S, TIMESTEPS_TOTAL)
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
    # TODO(ujvl): Expose ProgressReporter in tune.run for custom reporting.

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
            trial_runner.scheduler_alg.debug_string(),
            trial_runner.trial_executor.debug_string(),
            trial_progress_str(trial_runner.get_trials(), fmt="html"),
            trial_errors_str(trial_runner.get_trials(), fmt="html"),
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
            trial_runner.scheduler_alg.debug_string(),
            trial_runner.trial_executor.debug_string(),
            trial_progress_str(trial_runner.get_trials()),
            trial_errors_str(trial_runner.get_trials()),
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


def trial_progress_str(trials, metrics=None, fmt="psql", max_rows=20):
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
    trials_by_state = collections.defaultdict(list)
    for t in trials:
        trials_by_state[t.status].append(t)

    for local_dir in sorted({t.local_dir for t in trials}):
        messages.append("Result logdir: {}".format(local_dir))

    num_trials_strs = [
        "{} {}".format(len(trials_by_state[state]), state)
        for state in trials_by_state
    ]
    messages.append("Number of trials: {} ({})".format(
        num_trials, ", ".join(num_trials_strs)))

    if num_trials > max_rows:
        # TODO(ujvl): suggestion for users to view more rows.
        trials_by_state_trunc = _fair_filter_trials(trials_by_state, max_rows)
        trials = []
        overflow_strs = []
        for state in trials_by_state:
            trials += trials_by_state_trunc[state]
            overflow = len(trials_by_state[state]) - len(
                trials_by_state_trunc[state])
            overflow_strs.append("{} {}".format(overflow, state))
        # Build overflow string.
        overflow = num_trials - max_rows
        overflow_str = ", ".join(overflow_strs)
        messages.append("Table truncated to {} rows. {} trials ({}) not "
                        "shown.".format(max_rows, overflow, overflow_str))

    # Pre-process trials to figure out what columns to show.
    keys = list(metrics or DEFAULT_PROGRESS_KEYS)
    keys = [k for k in keys if any(t.last_result.get(k) for t in trials)]
    # Build trial rows.
    params = list(set().union(*[t.evaluated_params for t in trials]))
    trial_table = [_get_trial_info(trial, params, keys) for trial in trials]
    # Parse columns.
    parsed_columns = [REPORTED_REPRESENTATIONS.get(k, k) for k in keys]
    columns = ["Trial name", "status", "loc"]
    columns += params + parsed_columns
    messages.append(
        tabulate(trial_table, headers=columns, tablefmt=fmt, showindex=False))
    return delim.join(messages)


def trial_errors_str(trials, fmt="psql", max_rows=20):
    """Returns a readable message regarding trial errors.

    Args:
        trials (List[Trial]): List of trials to get progress string for.
        fmt (str): Output format (see tablefmt in tabulate API).
        max_rows (int): Maximum number of rows in the error table.
    """
    messages = []
    failed = [t for t in trials if t.error_file]
    num_failed = len(failed)
    if num_failed > 0:
        messages.append("Number of errored trials: {}".format(num_failed))
        if num_failed > max_rows:
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


def _fair_filter_trials(trials_by_state, max_trials):
    """Filters trials such that each state is represented fairly.

    The oldest trials are truncated if necessary.

    Args:
        trials_by_state (Dict[str, List[Trial]]: Trials by state.
        max_trials (int): Maximum number of trials to return.
    Returns:
        Dict mapping state to List of fairly represented trials.
    """
    num_trials_by_state = collections.defaultdict(int)
    no_change = False
    # Determine number of trials to keep per state.
    while max_trials > 0 and not no_change:
        no_change = True
        for state in trials_by_state:
            if num_trials_by_state[state] < len(trials_by_state[state]):
                no_change = False
                max_trials -= 1
                num_trials_by_state[state] += 1
    # Sort by start time, descending.
    sorted_trials_by_state = {
        state: sorted(
            trials_by_state[state],
            reverse=True,
            key=lambda t: t.start_time if t.start_time else float("-inf"))
        for state in trials_by_state
    }
    # Truncate oldest trials.
    filtered_trials = {
        state: sorted_trials_by_state[state][:num_trials_by_state[state]]
        for state in trials_by_state
    }
    return filtered_trials


def _get_trial_info(trial, parameters, metrics):
    """Returns the following information about a trial:

    name | status | loc | params... | metrics...

    Args:
        trial (Trial): Trial to get information for.
        parameters (List[str]): Names of trial parameters to include.
        metrics (List[str]): Names of metrics to include.
    """
    result = flatten_dict(trial.last_result)
    trial_info = [str(trial), trial.status, str(trial.address)]
    trial_info += [result.get(CONFIG_PREFIX + param) for param in parameters]
    trial_info += [result.get(metric) for metric in metrics]
    return trial_info
