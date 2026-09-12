from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.examples.learners.classes.vpg_torch_learner import VPGTorchLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


class VPGTorchLearnerSharedOptimizer(VPGTorchLearner):
    """
    In order for a shared module to learn properly, a special, multi-agent Learner
    has been set up. There is only one optimizer (used to train all submodules, e.g.
    a shared encoder and n policy nets), in order to not destabilize learning. The
    latter may happen if more than one optimizer would try to alternatingly optimize
    the same shared submodule.
    """

    @override(TorchLearner)
    def configure_optimizers(self) -> None:
        # Get and aggregate parameters for every module
        param_list = []
        for m in self.module.values():
            if self.rl_module_is_compatible(m):
                param_list.extend(m.parameters())

        self.register_optimizer(
            optimizer_name="shared_optimizer",
            optimizer=torch.optim.Adam(params=param_list),
            params=param_list,
            # For the policy learning rate, we use the "main" lr in the AlgorithmConfig.
            lr_or_lr_schedule=self.config.lr,
        )
