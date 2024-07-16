import abc
from typing import Any, Dict, List, Tuple

from ray.rllib.utils.typing import NetworkType


class ValueFunctionAPI(abc.ABC):
    """An API to be implemented by RLModules for handling value function-based learning.

    RLModules implementing this API must override the `get_nets_that_need_target_nets`
    method and return a list of `NetworkType` from there, representing all network
    modules that require a target network to be created and synched from time to time.

    Note that the respective Learner that owns the RLModule handles all target net
    creation- and syncing logic.
    """

    @abc.abstractmethod
    def make_target_networks(self) -> None:
        """Creates the required target nets for this RLModule.

        You should use the convenience
        `ray.rllib.core.learner.utils.make_target_network()` utility and pass in
        an already existing, corresponding "main" net (for which you need a target net).
        This function already takes care of initialization (from the "main" net).
        """

    @abc.abstractmethod
    def get_target_network_pairs(self) -> List[Tuple[NetworkType, NetworkType]]:
        """Returns a list of 2-tuples of (main_net, target_net).

        For example, if your RLModule has a property: `self.q_net` and this network
        has a corresponding target net `self.target_q_net`, return from this
        (overridden) method: [(self.q_net, self.target_q_net)].

        Note that you need to create all target nets in your overridden
        `make_target_networks` method and store the target nets in any properly of your
        choice.

        Returns:
            A list of 2-tuples of (main_net, target_net)
        """

    @abc.abstractmethod
    def forward_target(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        """Performs the forward pass through the target net(s).

        Args:
            batch: The batch to use for the forward pass.

        Returns:
            The results from the forward pass(es) through the target net(s).
        """
