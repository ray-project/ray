import abc
from dataclasses import dataclass
from typing import Dict, Type, Union

import torch
from torch.optim import Optimizer

from rllib2.data.sample_batch import SampleBatch
from rllib2.models.torch.torch_rl_module import RLModuleConfig, TorchRLModule


@dataclass
class UnitTrainerConfig:
    model_class: Optional[Union[str, Type[TorchRLModule]]] = None
    model_config: Optional[RLModuleConfig] = None
    optimizer_config: Optional[Dict[str, Any]] = None


class TorchRLTrainer:
    def __init__(self, module, config):
        self._config = config

        # # register the RLModule model
        # self._model = self._make_model()
        self._module = module

        # register optimizers
        self._optimizers: Dict[str, Optimizer] = self.make_optimizer()

    @property
    def config(self) -> UnitTrainerConfig:
        return self._config

    @property
    def model(self) -> TorchRLModule:
        return self._model

    @property
    def optimizers(self) -> Dict[str, Optimizer]:
        return self._optimizers

    #
    # @property
    # def default_rl_module(self) -> Union[str, Type[TorchRLModule]]:
    #     return ''

    @abc.abstractmethod
    def make_optimizer(self) -> Dict[str, Optimizer]:
        raise NotImplementedError

    # @abc.abstractmethod
    # def _make_model(self) -> TorchRLModule:
    #     config = self.config
    #     rl_module_class = config.get('model_class', self.default_rl_module())
    #     rl_module_config = config['model_config']
    #
    #     # import rl_module_class with rl_module_config
    #     # TODO
    #     rl_module: TorchRLModule = None
    #     return rl_module

    @abc.abstractmethod
    def loss(
        self, train_batch: SampleBatch, fwd_train_dict: RLModuleOutput
    ) -> Dict[LossID, torch.Tensor]:
        """
        Computes the loss for each sub-module of the algorithm and returns the loss
        tensor computed for each loss that needs to get back-propagated and updated
        according to the corresponding optimizer.

        This method should use self.model.forward_train() to compute the forward-pass
        tensors required for training.

        Args:
            train_batch: SampleBatch to train with.

        Returns:
            Dict of optimizer names map their loss tensors.
        """
        raise NotImplementedError

    def update(self, train_batch: SampleBatch, **kwargs):

        self.model.train()
        fwd_train_dict = self.model.forward_train(train_batch)
        loss_dict = self.loss(train_batch, fwd_train_dict)

        for loss_key, loss_value in loss_dict.items():
            self._optimizers[loss_key].zero_grad()
            loss_value.backward()
            self._optimizers[loss_key].step()


"""
TODO: for cases where we need augmenting agent's local observation for training/evaluation
we should provide callback hooks?

Example use-case: Centralized Critic 2 -> Callback for observation augmentation + centralized shared vf
"""

"""
TODO: We should be able to have a type of TorchMARLTrainer[PPOTrainer] which basically 
says that it's a multi-agent wrapper for the base of PPOTrainer. 
"""


class TorchMARLTrainer:
    type: Type[TorchRLTrainer]

    def __init__(self, configs):
        # basically the Type class is going to get constructed for each key inside cnofigs
        self.configs = configs
        self.shared_modules = self.make_shared_models()
        self.modules = self.make_modules()

        self.module_trainers = self.make_module_trainers()
        self.optimizers = ...

    def make_shared_models(self):
        """Something like QMix would go here too."""
        ma_config = self.config["multi_agent"]
        shared_config = ma_config["shared_modules"]

        shared_mod_infos = defaultdict({})  # mapping from policy to kwarg and value
        for mod_name, mod_info in shared_config.items():
            mod_class = mod_info["class"]
            mod_config = mod_info["config"]
            mod_obj = mod_class(mod_config)

            for pid, kw in mod_info["shared_between"].items():
                shared_mod_infos[pid][kw] = mod_obj

        """
        shared_mod_infos = 'policy_kwargs'{
            'A': {'encoder': encoder, 'dynamics': dyna},
            'B': {'encoder': encoder, 'dynamics': dyna},
            '__all__': {'mixer': mixer}
        }
        """
        return shared_mod_infos

    def make_modules(self):
        """
        I don't know of any scenarios that you'd need to override this method
        """
        shared_mod_info = self.shared_modules
        policies = self.config["multi_agent"]["policies"]
        modules = {}
        for pid, pid_info in policies.items():
            rl_mod_class, rl_mod_config = pid_info
            kwargs = shared_mod_info[pid]
            rl_mod_obj = rl_mod_class(config=rl_mod_config, **kwargs)
            modules[pid] = rl_mod_obj

        return modules

    def make_module_trainers(self):
        trainers = {}
        for pid, config in self.configs.items():
            rl_module = self.modules[pid]
            trainers[pid] = self.type(module=rl_module, config=config)
        return trainers

    def update(self, ma_batch: MultiAgentBatch):
        # this function augments each policies batch with other agent's batches if necessary
        self.callbacks.before_update_on_sample_batch(train_batch)

        losses = {}
        all_fwd_dicts = {}
        for pid, s_batch in ma_batch.items():
            rl_module_trainer = self.modules[pid]
            rl_module = rl_module_trainer.module
            fwd_dict = rl_module.forward_train(s_batch)
            loss_dict = rl_module_trainer.loss(s_batch, fwd_dict)
            all_fwd_dicts[pid] = fwd_dict
            losses[pid] = loss_dict

        total_loss_dict = self.loss(ma_batch, all_fwd_dicts, losses)

        for loss_key, loss_value in total_loss_dict.items():
            self._optimizers[loss_key].zero_grad()
            loss_value.backward()
            self._optimizers[loss_key].step()

    @override
    def make_optimizers(self):
        pass

    @override
    def loss(
        self, train_batch: MultiAgentBatch, all_fwd_dicts, losses
    ) -> Dict[str, torch.Tensor]:
        pass


class IndependantMARLTrainer(TorchMARLTrainer):
    """
    This will be MARL agent that independently runs each trainer update on its own
    """

    def make_optimizers(self):
        optimizers = {}
        for pid, trainer in self.module_trainers.items():
            for optim_key, optimizer in trainer.optimizers.items():
                optimizers[f"{pid}_{optim_key}"] = optimizer
        return optimizers

    def loss(
        self, train_batch: MultiAgentBatch, all_fwd_dicts, losses
    ) -> Dict[str, torch.Tensor]:
        marl_loss_dict = {}
        for pid, loss_dict in losses.items():
            for loss_key, loss_value in loss_dict.items():
                marl_loss_dict[f"{pid}_{loss_key}"] = loss_value


class QMixMARLTrainer(TorchMARLTrainer):
    def make_shared_models(self):
        shared_models = super(QMixMARLTrainer, self).make_shared_models()
        shared_models["__all__"]["mixer"]: nn.Module = Mixer(...)

    def make_optimizers(self):
        all_mods = nn.ModuleDict(
            {"shared": self.shared_modules, "modules": self.modules}
        )
        optimizers = {"q_loss": Adam(all_mods.parameters(), lr=self.config["lr"])}
        return optimizers

    def loss(
        self, train_batch: MultiAgentBatch, all_fwd_dicts, losses
    ) -> Dict[str, torch.Tensor]:
        q_vals = self.mixer(
            q_vals=all_fwd_dicts["q_vals"], s_t=train_batch["__all__"]["state"]
        )
        targets = compute_targets(...)
        q_loss = ((q_vals - targets) ** 2).mean()
        loss = {"q_loss": q_loss}
        return loss
