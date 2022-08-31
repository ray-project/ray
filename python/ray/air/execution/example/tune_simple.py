import time

from ray import tune
from ray.air.execution.actor_manager import ActorManager
from ray.air.execution.impl.tune.tune_controller import TuneController
from ray.air.execution.resources.fixed import FixedResourceManager
from ray.tune.progress_reporter import _detect_reporter, ProgressReporter
from ray.tune.trainable import wrap_function


def train_fn(config):
    return {"metric": config["A"] - config["B"]}


tune_controller = TuneController(
    trainable_cls=wrap_function(train_fn),
    param_space={"A": tune.randint(0, 100), "B": tune.randint(20, 30)},
)
fixed_resource_manager = FixedResourceManager(total_resources={"CPU": 4})
manager = ActorManager(
    controller=tune_controller, resource_manager=fixed_resource_manager
)


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


progress_reporter = _detect_reporter()
progress_reporter.setup(
    start_time=time.time(),
    total_samples=tune_controller._searcher.total_samples,
    metric=None,
    mode=None,
)
while not manager.is_finished():
    manager.step()
    _report_progress(tune_controller, progress_reporter)

_report_progress(tune_controller, progress_reporter, done=True)

manager.step_until_finished()
