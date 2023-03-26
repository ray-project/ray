import abc
from typing import List, Tuple

from ray.rllib.utils.typing import NetworkType


class RLModuleWithTargetNetworksInterface(abc.ABC):
    """An RLModule Mixin for adding an interface for target networks.

    This is used for identifying the target networks that are used for stabilizing
    the updates of the current trainable networks of this RLModule.
    """

    @abc.abstractmethod
    def get_target_network_pairs(self) -> List[Tuple[NetworkType, NetworkType]]:
        """Returns a list of (target, current) networks.

        This is used for identifying the target networks that are used for stabilizing
        the updates of the current trainable networks of this RLModule.

        Returns:
            A list of (target, current) networks.
        """
