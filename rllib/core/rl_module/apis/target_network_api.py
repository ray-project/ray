import abc
from typing import Any, Dict, List, Tuple

from ray.rllib.utils.typing import NetworkType


class TargetNetworkAPI(abc.ABC):
    """An API to be implemented by RLModules for handling target networks.

    RLModules implementing this API must override the `get_nets_that_need_target_nets`
    method and return a list of `NetworkType` from there, representing all network
    modules that require a target network to be created and synched from time to time.

    Note that the respective Learner that owns the RLModule handles all target net
    creation- and syncing logic.
    """
    @abc.abstractmethod
    def make_target_networks(self):
        """"""


    @abc.abstractmethod
    def get_target_network_pairs(self) -> List[Tuple[NetworkType, NetworkType]]:
        """"""

    @abc.abstractmethod
    def forward_target(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        """"""
