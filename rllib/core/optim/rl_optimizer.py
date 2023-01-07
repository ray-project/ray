import abc
from typing import Any, Mapping, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from ray.rllib.core.optim.marl_optimizer import MultiAgentRLOptimizer

from ray.rllib.core.rl_module import RLModule
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.typing import TensorType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="beta")
class RLOptimizer(abc.ABC):
    """Base class for defining a loss function and optimizer for a RLModule.

    Abstract Methods:
        compute_loss: computing a loss to optimize rl_module over.
        _configure_optimizers: constructing the optimizers for rl_module.
        get_state: getting the state of the optimizer.
        set_state: setting the state of the optimizer.

    Example:
    .. code-block:: python

        module = RLModule(...)
        rl_optim = RLOptimizer.from_module(module, config)
        sample_batch = ...
        fwd_out = module.forward_train(sample_batch)
        loss_dict = rl_optim.compute_loss(fwd_out, sample_batch)

        # compute gradients of loss w.r.t. trainable variables
        ...

        for network_id, optim in rl_optim.get_optimizers().items():
            optim.step()
            optim.zero_grad()

    """

    def __init_subclass__(cls, **kwargs):
        # Automatically add a __post_init__ method to all subclasses of RLModule.
        # This method is called after the __init__ method of the subclass.
        def init_decorator(previous_init):
            def new_init(self, *args, **kwargs):
                previous_init(self, *args, **kwargs)
                if type(self) == cls:
                    self.__post_init__()

            return new_init

        cls.__init__ = init_decorator(cls.__init__)

    def __post_init__(self):
        """Called automatically after the __init__ method of the subclass.

        The module first calls the __init__ method of the subclass, With in the
        __init__ you should call the super().__init__ method. Then after the __init__
        method of the subclass is called, the __post_init__ method is called.

        This is a good place to do any initialization that requires access to the
        subclass's attributes.
        """
        self._optimizers = self._configure_optimizers()

    @classmethod
    def from_module(cls, module: RLModule, config: Mapping[str, Any]):
        """Constructs an RLOptimizer from a RLModule.

        Args:
            module: The RLModule to optimize.
            config: The configuration for the optimizer.

        Returns:
            An RLOptimizer for optimizing module.
        """

    @abc.abstractmethod
    def compute_loss(
        self, fwd_out: Mapping[str, Any], batch: Mapping[str, Any]
    ) -> Union[TensorType, Mapping[str, Any]]:
        """Computes variables for optimizing self._module based on fwd_out.

        Args:
            fwd_out: Output from a call to `forward_train` on self._module during
                training.
            batch: The data that was used to compute fwd_out.

        Returns:
            Either a single loss tensor which can be used for computing
            gradients through, or a dictionary of losses. NOTE the dictionary
            must contain one protected key "total_loss" which will be used for
            computing gradients through.
        """

    @abc.abstractmethod
    def _configure_optimizers(self) -> Mapping[str, Any]:
        """Configures the optimizers for self._module.

        Returns:
            A map of optimizers to be used for optimizing self._module.
        """

    def get_optimizers(self) -> Mapping[str, Any]:
        """Returns the map of optimizers for this RLOptimizer."""
        return self._optimizers

    @abc.abstractmethod
    def get_state(self) -> Mapping[str, Any]:
        """Returns the optimizer state.

        Returns:
            The optimizer state.
        """

    @abc.abstractmethod
    def set_state(self, state: Mapping[str, Any]):
        """Sets the optimizer state.

        Args:
            state: The optimizer state.
        """

    def as_multi_agent(self) -> "MultiAgentRLOptimizer":
        """Wraps this optimizer in a MultiAgentOptimizer."""
        from ray.rllib.core.optim.marl_optimizer import MultiAgentRLOptimizer

        return MultiAgentRLOptimizer({DEFAULT_POLICY_ID: self})
