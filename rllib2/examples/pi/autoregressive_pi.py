

###################################################################
########### Auto-regressive action space policy
# action_space = {'torques': Box(-1, 1)^6, 'gripper': Discrete(n=2)}
# a ~ MixedDistribution({
#   'torques' ~ squashedNorm(mu(s), std(s)),
#   'gripper' ~ Categorical([p1(s, torques), p2(s, torques)])
# })
# example usage of the mixed_dist with autoregressive outputs
# >>> actor_loss = mixed_dist.log_prob(a)
# >>> {'torques': Tensor(0.63), 'gripper': Tensor(0.63)}
###################################################################


class GripperObsTorqueEncoder(Encoder):
    def __init__(self, torque_out_dim: int, obs_encoder: nn.Module) -> None:
        super().__init__()
        self.obs_encoder = obs_encoder

        self.linear = nn.Linear(
            self.obs_encoder.out_dim + torque_out_dim,
            64,
        )

    def forward(self, input_dict: SampleBatch) -> torch.Tensor:
        obs = input_dict["obs"]
        torque = input_dict["torque"]
        obs = self.obs_encoder(obs)
        z_t = torch.cat([obs, torque], -1)
        out = self.linear(z_t)
        return out


class CustomAutoregressivePi(PiBase):
    def __init__(self, config: PiConfig):
        super().__init__(config)

        torque_configs = deepcopy(config)
        torque_configs.action_space = config.action_space["torque"]
        torque_configs.observation_space = config.observation_space["torque"]
        self.torque_pi = Pi(torque_configs)

        gripper_configs = deepcopy(config)
        gripper_configs.action_space = config.action_space["gripper"]
        gripper_configs.observation_space = config.observation_space["gripper"]
        # get the obs encoder from torque's pi to share parameters with gripper's pi
        obs_encoder = self.torque_pi.encoder
        gripper_configs.encoder = GripperObsTorqueEncoder(
            torque_out_dim=torque_configs.fcnet_hiddens[-1], encoder=obs_encoder
        )

        self.gripper_pi = Pi(gripper_configs)

    def forward(self, batch: SampleBatch, **kwargs) -> PiOutput:

        torque_output = self.torque_pi(batch)
        torque_actions = torque_output.sample()

        sample_batch = {"torque": torque_actions, "obs": batch.obs}
        gripper_actions = self.gripper_pi(sample_batch)
        ...

        return PiOutput(action_dist=..., action_sampled=...)
