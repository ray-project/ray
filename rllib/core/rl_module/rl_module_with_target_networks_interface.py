import abc


class RLModuleWithTargetNetworksInterface(abc.ABC):
    """An RLModule Mixin for adding an interface for target networks.

    This is used for identifying the target networks that are used for stabilizing
    the updates of the current trainable networks of this RLModule.
    """

    @abc.abstractmethod
    def sync_target_networks(self, tau: float = 1.0) -> None:
        """Update the target network(s) from their corresponding "main" networks.

        The update is made via Polyak averaging (if tau=1.0, the target network(s)
        are completely overridden by the main network(s)' weights, if tau=0.0, the
        target network(s) are left as-is).

        Args:
            tau: The tau value to use for polyak averaging.
        """
