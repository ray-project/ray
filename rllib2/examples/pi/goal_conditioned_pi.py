################################################################
########### Goal conditioned policies a ~ N(mu(s, g), std(s, g))
################################################################


# see how easy it is to modify the encoder
# encode goal observation and current observation and concat them as the policy
# input
class Encoder(nn.Module):
    def __init__(self, observation_space, action_space) -> None:
        super().__init__()
        config = ModelConfig(
            observation_space=observation_space, action_space=action_space
        )
        self.encoding_layer = model_catalog.get_encoder(config)

    def forward(self, input_dict):
        obs = input_dict["obs"]
        goal = input_dict["goal"]

        z_obs = self.encoding_layer(obs)
        z_goal = self.encoding_layer(goal)
        return torch.cat([z_obs, z_goal], -1)

register_model("goal_conditioned_encoder", Encoder)
config = PiConfig(
    encoder="goal_conditioned_encoder",
    observation_space=Box(low=-1, high=1, shape=(10,)),
    action_space=Discrete(2),
    is_deterministic=True,
    # free_log_std=True,
)

pi = Pi(config)
print(pi)
