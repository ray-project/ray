from ray.rllib.algorithms.appo.appo_rl_module import APPORLModule
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.utils.annotations import override


class APPOTorchRLModule(PPOTorchRLModule, APPORLModule):
    @override(PPOTorchRLModule)
    def _set_inference_only_state_dict_keys(self) -> None:
        # Get the model_parameters from the `PPOTorchRLModule`.
        super()._set_inference_only_state_dict_keys()
        # Get the model_parameters.
        state_dict = self.state_dict()
        # Note, these keys are only known to the learner module. Furthermore,
        # we want this to be run once during setup and not for each worker.
        self._inference_only_state_dict_keys["unexpected_keys"].extend(
            [name for name in state_dict if "old" in name]
        )
