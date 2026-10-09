import abc

from ray.train.health.decision import HealthDecision
from ray.train.health.state import HealthState
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class Evaluator(abc.ABC):
    """Decides what the run should do from probe readings. Implement
    ``evaluate()``."""

    @abc.abstractmethod
    def evaluate(self, state: HealthState) -> HealthDecision:
        """Decide what the run should do.

        Args:
            state: The latest probe readings.

        Returns:
            A ``HealthDecision``. ``Noop()`` if no action is needed.
        """
        raise NotImplementedError

    def on_worker_group_start(self) -> None:
        """Called at the start of each attempt of the training run."""
