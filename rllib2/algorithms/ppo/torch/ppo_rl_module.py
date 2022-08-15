####################################################
################# PPO RLModule
# This is what you customize when modifying the model
####################################################

from rllib2.core.torch.torch_rl_module import (
    RLModuleConfig,
    RLModuleOutput,
    TorchRLModule,
)
from rllib2.models.torch.pi import Pi, PiOutput


# TODO: Make a decision about where explore=True / False should go?
@dataclass
class PPORLModuleConfig(RLModuleConfig):
    action_space: Optional[rllib.env.Space] = None
    obs_space: Optional[rllib.env.Space] = None
    pi: Optional[PiConfig] = None
    vf: Optional[VFuctionConfig] = None
    kl_coeff: Optional[float] = None


@dataclass
class PPOModuleOutput(RLModuleOutput):
    pi_out_cur: Optional[PiOutput] = None
    pi_out_prev: Optional[PiOutput] = None
    vf: Optional[VFOutput] = None


class PPOTorchRLModule(TorchRLModule):
    def __init__(self, config: PPORLModuleConfig):
        super().__init__(config)

        self.pi: Pi = model_catalog.make_pi(
            obs_space=config.obs_space,
            action_space=config.action_space,
            pi_config=config.pi,
        )

        self.vf = None
        if config.use_ciritic:
            self.vf = model_catalog.make_vf(
                obs_space=config.obs_space,
                action_space=config.action_space,
                vf_config=config.vf,
            )

        if config.kl_coeff is not None:
            kl_coeff = nn.Parameter(torch.Tensor(config.kl_coeff), requires_grad=False)
            self.register_parameter("kl_coeff", kl_coeff)

    def forward(self, batch: SampleBatch, explore=False, **kwargs) -> PiDistribution:

        # option 1: when pi_output has separate target_sample() and behavioral_sample()
        # in this case explore is totally ignored inside the forward() method.
        pi_output: PiOutput = self.pi(batch)

        # option 2: when pi_output has one sample() method whose behavior changes
        # depending on a flag
        pi_output: PiOutput = self.pi(batch, explore=explore)

        return pi_output

    def forward_train(self, batch: SampleBatch, **kwargs) -> PPOModuleOutput:
        """Forward-pass during computing loss function"""
        pi_out_cur: PiOutput = self.pi(batch)
        pi_out_prev = self.pi({"obs": batch[SampleBatch.ACTION_DIST_INPUTS]})

        vf = None
        if self.vf:
            vf = self.vf(batch)

        return PPOModuleOutput(pi_out_cur=pi_out_cur, pi_out_prev=pi_out_prev, vf=vf)
