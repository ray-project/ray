import collections
from tabulate import tabulate

from ray.tune.result import DEFAULT_RESULT_KEYS, CONFIG_PREFIX
from ray.tune.util import flatten_dict


class ProgressReporter(object):
    def report(self, trial_runner):
        """Reports progress across all trials of the trial runner.

        Args:
            trial_runner: Trial runner to report on.
        """
        raise NotImplementedError


class JupyterNotebookReporter(ProgressReporter):
    def __init__(self, verbosity):
        self.verbosity = verbosity

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
        if self.verbosity < 2:
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


def trial_progress_str(trials,
                       parameters=None,
                       metrics=None,
                       fmt="psql",
                       max_rows=100):
    """Returns a human readable message for printing to the console.

    This contains a table where each row represents a trial, its parameters
    and the current values of its metrics.

    Args:
        trials (List[Trial]): List of trials to get progress string for.
        parameters (List[str]): Names of trial parameters to include.
            Defaults to all parameters across all trials.
        metrics (List[str]): Names of metrics to include. Defaults to
            metrics defined in DEFAULT_RESULT_KEYS.
        fmt (str): Output format (see tablefmt in tabulate API).
        max_rows (int): Maximum number of rows in the trial table.
    """
    messages = []
    delim = "<br>" if fmt == "html" else "\n"
    if len(trials) < 1:
        return delim.join(messages) + delim

    num_trials = len(trials)
    num_trials_per_state = collections.defaultdict(int)
    for t in trials:
        num_trials_per_state[t.status] += 1
    messages.append("Number of trials: {} ({})"
                    "".format(num_trials, num_trials_per_state))

    if num_trials > max_rows:
        overflow = num_trials - max_rows
        # TODO(ujvl): suggestion for users to view more rows.
        messages.append("Table truncated to {} rows ({} overflow).".format(
            max_rows, overflow))

    trial_table = []
    if not parameters:
        parameters = set().union(*[t.evaluated_params for t in trials])
    metrics = list(metrics or DEFAULT_RESULT_KEYS)

    for i in range(min(num_trials, max_rows)):
        trial = trials[i]
        result = flatten_dict(trial.last_result)
        trial_info = [trial.trial_id, trial.status]
        trial_info += [
            result.get(CONFIG_PREFIX + param) for param in parameters
        ]
        trial_info += [result.get(metric) for metric in metrics]
        trial_table.append(trial_info)

    parsed_parameters = [param for param in parameters]
    keys = ["Trial ID", "Status"] + parsed_parameters + metrics
    messages.append(
        tabulate(trial_table, headers=keys, tablefmt=fmt, showindex=False))
    return delim.join(messages) + delim
