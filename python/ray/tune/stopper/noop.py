from ray.tune.stopper.stopper import Stopper
from ray.util.annotations import PublicAPI


@PublicAPI
class NoopStopper(Stopper):
    """A stopper that never stops a trial or the experiment."""

    def __call__(self, trial_id, result):
        return False

    def stop_all(self):
        return False
