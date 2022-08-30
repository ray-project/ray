from typing import List, Type, Optional

from ray.air.execution.actor_request import ActorRequest
from ray.air.execution.controller import Controller

# Legacy tune
from ray.tune.trainable import Trainable
from ray.tune.stopper import Stopper
from ray.tune.experiment import Experiment, Trial
from ray.tune.schedulers import FIFOScheduler
from ray.tune.search import SearchAlgorithm, BasicVariantGenerator
from ray.tune.stopper import NoopStopper


class TuneController(Controller):
    def __init__(self, trainable_cls: Type[Trainable], param_space: dict):
        self._param_space = param_space

        self._trials = []

        self._searcher: SearchAlgorithm = BasicVariantGenerator(max_concurrent=4)
        self._stopper: Stopper = NoopStopper()
        self._scheduler = FIFOScheduler()

        self._searcher.add_configurations(
            Experiment(
                name="some_test",
                run=trainable_cls,
                config=self._param_space,
                num_samples=8,
            )
        )

    def is_finished(self) -> bool:
        return self._searcher.is_finished() or self._stopper.stop_all()

    def _create_trial(self) -> Optional[Trial]:
        return self._searcher.next_trial()

    def get_actor_requests(self) -> List[ActorRequest]:
        pass
