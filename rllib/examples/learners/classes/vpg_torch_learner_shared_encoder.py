from ray.rllib.examples.learners.classes.vpg_torch_learner import VPGTorchLearner
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


class VPGTorchLearnerSharedEncoder(VPGTorchLearner):
    """
        In order for a shared encoder to learn properly, a special, multi-agent Learner
        accounting for the shared encoder has been set up. There is only one optimizer
        (used to train all submodules: encoder and the n policy nets), in order to not
        destabilize learning. The latter may happen if more than one optimizer would try
        to alternatingly optimize the same shared encoder submodule.
    """

    @override(TorchLearner)
    def configure_optimizers(self) -> None:
        # Get and aggregate parameters for every module
        param_list = []
        for module_id in self.module.keys():
            m = self.module[module_id]
            if self.rl_module_is_compatible(self.module[module_id]):
              param_list += m.parameters()
        #
        self.register_optimizer(
            optimizer_name="shared_optimizer",
            optimizer=torch.optim.Adam(params=param_list),
            params=param_list,
            # For the policy learning rate, we use the "main" lr in the AlgorithmConfig.
            lr_or_lr_schedule=self.config.lr,
        )
