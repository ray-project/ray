import abc
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
    from ray.rllib.utils.typing import ModuleID


class RLModuleWithTargetNetworksInterface(abc.ABC):
    """An RLModule Mixin for adding an interface for target networks.

    This is used for identifying the target networks that are used for stabilizing
    the updates of the current trainable networks of this RLModule.
    """

    @abc.abstractmethod
    def sync_target_networks(
        self,
        module_id: "ModuleID",
        config: "AlgorithmConfig",
        tau: Optional[float] = None,
    ) -> None:
        """Update the target network(s) from their corresponding "main" networks.

        The update is made via Polyak averaging (if tau=1.0, the target network(s)
        are completely overridden by the main network(s)' weights, if tau=0.0, the
        target network(s) are left as-is).

        Args:
            module_id: The RLModule ID to update the target nets for.
            config: The module specific AlgorithmConfig to be used.
            tau: An optional tau value to use for polyak averaging. If None, should try
                using the `tau` setting given in `config.
        """
