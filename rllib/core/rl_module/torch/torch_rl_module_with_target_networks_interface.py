from ray.rllib.core.rl_module.rl_module_with_target_networks_interface import (
    RLModuleWithTargetNetworksInterface,
)


class TorchRLModuleWithTargetNetworksInterface(RLModuleWithTargetNetworksInterface):
    def _sync_target_networks(self, tau: float) -> None:
        # Loop through all individual networks that have a corresponding target net.
        for target_net, main_net in self.get_target_network_pairs():
            # Get the current parameters from the main network.
            state_dict = main_net.state_dict()
            # Use here Polyak averaging.
            new_target_state_dict = {
                k: tau * state_dict[k] + (1 - tau) * v
                for k, v in target_net.state_dict().items()
            }
            # Apply the new parameters to the target Q network.
            target_net.load_state_dict(new_target_state_dict)
