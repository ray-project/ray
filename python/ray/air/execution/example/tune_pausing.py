from ray import tune
from ray.air import session, Checkpoint
from ray.air.execution.actor_manager import ActorManager
from ray.air.execution.impl.tune.progress_loop import tune_run
from ray.air.execution.impl.tune.tune_controller import TuneController
from ray.air.execution.resources.fixed import FixedResourceManager
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.search import BasicVariantGenerator
from ray.tune.trainable import wrap_function

# THIS EXAMPLE DOES NOT WORK YET
# Checkpointing is not fully implemented.


class FrequentPausesScheduler(FIFOScheduler):
    def on_trial_result(self, trial_runner, trial, result):
        return TrialScheduler.PAUSE


def train_fn(config):
    checkpoint = session.get_checkpoint()
    if checkpoint:
        start = checkpoint.to_dict()["start"] + 1
    else:
        start = 0

    for i in range(start, 10):
        session.report(
            {"metric": config["A"] - config["B"], "step": i, "done": i > 5},
            checkpoint=Checkpoint.from_dict({"start": i}),
        )


tune_controller = TuneController(
    trainable_cls=wrap_function(train_fn),
    param_space={
        "A": tune.randint(0, 100),
        "B": tune.randint(20, 30),
    },
    search_alg=BasicVariantGenerator(max_concurrent=4),
    scheduler=FrequentPausesScheduler(),
)
fixed_resource_manager = FixedResourceManager(total_resources={"CPU": 4})
manager = ActorManager(
    controller=tune_controller, resource_manager=fixed_resource_manager
)

tune_run(manager=manager, tune_controller=tune_controller)
