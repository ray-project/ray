import time

from ray.air.execution.impl.tune.tune_controller import TuneController
from ray.tune.progress_reporter import _detect_reporter, ProgressReporter


def _report_progress(
    controller: TuneController, reporter: ProgressReporter, done: bool = False
):
    """Reports experiment progress.

    Args:
        runner: Trial runner to report on.
        reporter: Progress reporter.
        done: Whether this is the last progress report attempt.
    """
    trials = controller._all_trials.copy()
    if reporter.should_report(trials, done=done):
        sched_debug_str = controller._scheduler.debug_string()
        reporter.report(trials, done, sched_debug_str, "")


def tune_run(tune_controller: TuneController):
    progress_reporter = _detect_reporter()
    progress_reporter.setup(
        start_time=time.time(),
        total_samples=tune_controller._searcher.total_samples,
        metric=None,
        mode=None,
    )
    while not tune_controller.is_finished():
        tune_controller.step()
        _report_progress(tune_controller, progress_reporter)

    _report_progress(tune_controller, progress_reporter, done=True)
