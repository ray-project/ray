"""Multi Agent Trainer."""
from typing import Any

from rllib2.core.trainer.rl_trainer import RLTrainer

from ray.rllib.policy.sample_batch import MultiAgentBatch
from ..module.rl_module import MARLModule


class MARLTrainer(RLTrainer):
    """A MARLTrainer that trains each module through its own trainer's loss function.

    This trainer keeps track of a separate module and exactly one trainer for each
    moodule. To update the modules, it runs their corresponding trainer.update() method
    indepenedently, and performs an update step on all the losses at the same time.

    """

    def __init__(self, module: Optional[MARLModule] = None) -> None:
        super().__init__()
        if module:
            self._module = module
        else:
            self._module = self._make_module()

        # create a dict to keep track of the trainers for each module
        self._module_trainers = self._make_module_trainers()


    @abc.abstractmethod
    def compute_grads_and_apply_if_needed(
        self, batch: BatchType, fwd_out, loss_out, apply_grad: bool = True, **kwargs
    ) -> Any:
        """To be overriden by specific framwork mixins."""
        raise NotImplementedError

    def update(
        self,
        batch: MultiAgentBatch,
        fwd_kwargs: Optional[Dict[str, Any]] = None,
        loss_kwargs: Optional[Dict[str, Any]] = None,
        grad_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Any:
        fwd_kwargs = fwd_kwargs or {}
        loss_kwargs = loss_kwargs or {}
        grad_kwargs = grad_kwargs or {}

        self._model.train()

        fwd_out_dict = {}
        loss_out_dict = {}
        for module_id, s_batch in batch.items():
            module = self._module[module_id]
            trainer = self._module_trainers[module_id]

            # run forward train of each module on the corresponding sample batch
            fwd_out = module.forward_train(s_batch, **fwd_kwargs)
            fwd_out_dict[module_id] = fwd_out

            # run loss of each module on the corresponding sample batch
            loss_out = trainer.compute_loss(s_batch, fwd_out, **loss_kwargs)
            loss_out_dict[module_id] = loss_out

        loss_out_total = self.compute_loss(
            batch, fwd_out_dict, loss_out_dict, **loss_kwargs
        )

        update_out = self.compute_grads_and_apply_if_needed(
            batch, fwd_out_dict, loss_out_total, **grad_kwargs
        )

        return update_out

    def compute_loss(
        self, batch: MultiAgentBatch, fwd_out, loss_out, **kwargs
    ) -> Dict["LossID", "TensorType"]:
        """
        To be overriden by specific algorithms. Each specific multiagent algorithm will also override the optimizer construction that conforms to these losses.

        Computes the loss for each sub-module of the algorithm and returns the loss
        tensor computed for each loss_id that needs to get back-propagated and updated
        according to the corresponding optimizer.

        This method should use self.model.forward_train() to compute the forward-pass
        tensors required for training.

        Args:
            train_batch: SampleBatch to train with.

        Returns:
            Dict of optimizer names map their loss tensors.
        """
        loss_out_total = {}
        for mid, loss_mid in loss_out.items():
            for key, value in loss_mid.items():
                loss_out_total[f"{mid}_{key}"] = value
        return loss_out_total

    def _make_module(self) -> MARLModule:
        module = MARLModule(
            shared_modules=self._config.shared_modules,
            modules=self._config.modules,
        )

        return module

    def _make_module_trainers(self):
        trainers = {}
        for pid, config in self.configs.items():
            rl_module = self.modules[pid]
            trainers[pid] = self._config.base_trainer(module=rl_module, config=config)
        return trainers
