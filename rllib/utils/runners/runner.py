import abc
import logging

from typing import Any, TYPE_CHECKING

from ray.rllib.utils.actor_manager import FaultAwareApply
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.typing import DeviceType, TensorType

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

logger = logging.getLogger(__name__)


class Runner(FaultAwareApply, metaclass=abc.ABCMeta):
    def __init__(self, *, config: "AlgorithmConfig", **kwargs):
        """Initializes a `Runner` instance.

        Args:
            config: The `AlgorithmConfig` to use to setup this `Runner`.
            **kwargs: Forward compatibility `kwargs`.
        """
        self.worker_index: int = kwargs.get("worker_index")
        self.config: AlgorithmConfig = config.copy(copy_frozen=False)
        # Set the device.
        self.set_device()
        # Generate the `RLModule`.
        self.make_module()
        self._weights_seq_no = 0

        # Create a MetricsLogger object for logging custom stats.
        self.metrics: MetricsLogger = MetricsLogger()

        # Initialize the `FaultAwareApply`.
        super().__init__()

    @abc.abstractmethod
    def assert_healthy(self):
        """Checks that self.__init__() has been completed properly.

        Useful in case an `Runner` is run as @ray.remote (Actor) and the owner
        would like to make sure the Ray Actor has been properly initialized.

        Raises:
            AssertionError: If the `Runner` Actor has NOT been properly initialized.
        """

    @abc.abstractmethod
    def make_module(self):
        """Creates the `RLModule` for this `Runner` and assigns it to `self.module`.

        Note that users should be able to change the `Runner`'s config (e.g. change
        `self.config.rl_module_spec`) and then call this method to create a new `RLModule`
        with the updated configuration.
        """
        pass

    @abc.abstractmethod
    def run(self, **kwargs) -> Any:
        """Runs the `Runner`.

        The exact logic of this method could have very different forms.

        Args:
            **kwargs: Forward compatibility kwargs.

        Returns:
            Anything.
        """

    @abc.abstractmethod
    def get_metrics(self) -> Any:
        """Returns metrics (in any form) of the logic run in this `Runner`.

        Returns:
            Metrics of any form.
        """

    @abc.abstractmethod
    def stop(self) -> None:
        """Releases all resources used by this `Runner`.

        For example, when using a `gym.Env` in this `Runner`, you should make sure
        that its `close()` method is called.
        """

    @property
    @abc.abstractmethod
    def _device(self) -> DeviceType:
        """Returns the device of this `Runner`."""
        pass

    @abc.abstractmethod
    def set_device(self) -> None:
        """Sets the device for this `Runner`."""
        pass

    @abc.abstractmethod
    def __del__(self) -> None:
        """If this Actor is deleted, clears all resources used by it."""

    @abc.abstractmethod
    def _convert_to_tensor(self, struct) -> TensorType:
        """Converts structs to a framework-specific tensor."""
