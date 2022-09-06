


###################################################################
########### Recurrent Policies
###################################################################
from rllib2.models.torch.pi import Pi, PiConfig
from rllib2.models.torch.encoder import RNNEncoder

rnn_config = ModelConfig()
rnn_config.rnn_cell = "lstm"
rnn_encoder = RNNEncoder(rnn_config)

config = PiConfig(
    observation_space=Box(low=-1, high=1, shape=(10,)),
    action_space=Discrete(2),
    encoder=rnn_encoder,
)
# TODO: Do we want to pass pi_config to PPOConfig
# or pi directly?
pi = Pi(config)
print(pi)

if __name__ == "__main__":

    config = PPOConfig()(
        pi=pi,
    )

    config.build()
    config.train()
