import time

from ray import tune
from ray._private.dict import flatten_dict, unflatten_dict
from ray.air.execution.impl.split.split_controller import SplitController
from ray.air.execution.impl.tune.tune_controller import TuneController
from ray.air.execution.resources.fixed import FixedResourceManager
from ray.air.execution.resources.virtual import VirtualResourceManager
from ray.tune.progress_reporter import _detect_reporter, ProgressReporter
from ray.tune.search import BasicVariantGenerator
from ray.tune.trainable import wrap_function


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


def tune_loop(tune_controller: TuneController):
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


def tune_run(
    trainable, num_samples=1, param_space=None, search_alg=None, resource_manager=None
):
    search_alg = search_alg or BasicVariantGenerator(max_concurrent=4)
    resource_manager = resource_manager or FixedResourceManager(
        total_resources={"CPU": 4}
    )

    tune_controller = TuneController(
        trainable_cls=wrap_function(trainable),
        num_samples=num_samples,
        param_space=param_space,
        search_alg=search_alg,
        resource_manager=resource_manager,
    )
    tune_loop(tune_controller=tune_controller)


def _split(available_resources, index, trainable, num_samples, param_space, search_alg):
    if isinstance(search_alg, BasicVariantGenerator):
        search_alg._uuid_prefix = f"split_{index:05d}_"

    tune_controller = TuneController(
        trainable_cls=wrap_function(trainable),
        num_samples=num_samples,
        param_space=param_space,
        search_alg=search_alg,
        resource_manager=FixedResourceManager(available_resources),
    )
    tune_loop(tune_controller=tune_controller)


def tune_split(
    trainable,
    split_by,
    num_samples=1,
    param_space=None,
    search_alg=None,
    resource_manager=None,
    resources_per_split=None,
):
    flat_space = flatten_dict(param_space)
    flat_split_by = f"{split_by}/grid_search"
    split_values = flat_space.pop(flat_split_by, {})

    resource_manager = resource_manager or VirtualResourceManager()
    resources_per_split = resources_per_split or {"CPU": 4}

    if not split_values:
        raise ValueError(
            f"Can only split for grid search variables, got: {split_values}"
        )

    split_controller = SplitController(
        resource_manager=resource_manager,
    )

    for index, split_value in enumerate(split_values):
        split_param_flat = flat_space.copy()
        split_param_flat[split_by] = tune.choice([split_value])
        split_param_space = unflatten_dict(split_param_flat)

        split_controller.add_split(
            _split,
            resources=resources_per_split,
            available_resources=resources_per_split,
            index=index,
            trainable=trainable,
            num_samples=num_samples,
            param_space=split_param_space,
            search_alg=search_alg,
        )

    while not split_controller.is_finished():
        split_controller.step()
