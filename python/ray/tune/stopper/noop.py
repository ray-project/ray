from ray.tune.stopper.stopper import Stopper
from ray.util.annotations import PublicAPI


@PublicAPI
class NoopStopper(Stopper):
    def __call__(self, trial_id, result):
        return False

    def stop_all(self):
        return False
