from rllib2.core.torch.torch_rl_module import (
    RLModuleConfig,
    RLModuleOutput,
    TorchRLModule,
)
from rllib2.models.torch.pi_distribution import CategoricalPiDistribution
from rllib2.models.torch.q_function import (
    DiscreteQFunction,
    EnsembleQFunction,
    QFunctionOutput,
)


@dataclass
class DQNRLModuleConfig(RLModuleConfig):
    pi: Optional[PiConfig] = None
    qf: Optional[VFuctionConfig] = None

    def __postinit__(self):
        if not self.action_space.is_discrete:
            raise ValueError("Only discrete actions are supported.")


@dataclass
class DQNModuleOutput(RLModuleOutput):
    pass


class DQNTorchRLModule(TorchRLModule):
    def __init__(self, config: DQNRLModuleConfig):
        super().__init__(config)

        self.pi: Pi = model_catalog.make_pi(
            obs_space=config.obs_space,
            action_space=config.action_space,
            pi_config=config.pi,
        )

        self.qf: EnsembleQFunction[DiscreteQFunction] = model_catalog.make_qf(
            obs_space=config.obs_space,
            action_space=config.action_space,
            qf_config=config.qf,
        )

    def forward(self, batch: SampleBatch, **kwargs) -> PiDistribution:
        q_output: QFunctionOutput = self.qf({"obs": batch["obs"]})
        # this will go through the exploration module -> softQ / epsilon greedy
        return CategoricalPiDistribution(q_output.q_logits)

    def forward_train(self, batch: SampleBatch, **kwargs) -> PPOModuleOutput:
        """Forward-pass during computing loss function"""
        pi_out_cur: PiOutput = self.pi(batch)
        pi_out_prev = self.pi({"obs": batch[SampleBatch.ACTION_DIST_INPUTS]})

        vf = None
        if self.vf:
            vf = self.vf(batch)

        return PPOModuleOutput(pi_out_cur=pi_out_cur, pi_out_prev=pi_out_prev, vf=vf)
