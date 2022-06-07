
from typing import Dict
import torch
from torch.optim import Optimizer, Adam

from rllib2.data.sample_batch import SampleBatch
from rllib2.core.torch.torch_unit_trainer import UnitTrainer
from rllib2.core.torch.torch_rl_module import TorchRLModule


class PPOTorchUnitTrainer(UnitTrainer):

    def make_model(self) -> TorchRLModule:
        # ? how do we make a generic customizable TorchRLModule?
        pass

    def make_optimizer(self) -> Dict[str, Optimizer]:
        return {
            'pi': Adam(self.model.pi.parameters(), lr=self.config['pi_lr']),
            'vf': Adam(self.model.vf.parameters(), lr=self.config['vf_lr'])
        }

    def loss(
        self,
        train_batch: SampleBatch,
        fwd_batch_output
    ) -> Dict[str, torch.Tensor]:

        pi_loss = ...
        vf_loss = ...

        return {
            'pi': pi_loss,
            'vf': vf_loss
        }


