from rllib2.algorithms.dqn.torch.dqn_module import (
    DQNModuleOutput,
    DQNRLModuleConfig,
    DQNTorchRLModule,
)
from rllib2.core.torch.torch_unit_trainer import TorchUnitTrainer, UnitTrainerConfig


class DQNUnitTrainerConfig(UnitTrainerConfig):
    pass


class DQNUnitTrainer(TorchUnitTrainer):
    def __init__(self, config: DQNUnitTrainerConfig):
        super().__init__(config)

    def default_rl_module(self) -> Union[str, Type[TorchRLModule]]:
        return DQNTorchRLModule

    def make_optimizer(self) -> Dict[str, Optimizer]:
        config = self.config.optimizer_config
        return {"total_loss": torch.optim.Adam(self.model.parameters(), lr=config.lr)}

    def loss(
        self, train_batch: SampleBatch, fwd_train_dict: DQNModuleOutput
    ) -> Dict[str, torch.Tensor]:

        return {"total_loss": total_loss}
