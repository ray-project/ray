###################################################################
########### Transformer policy
###################################################################

from rllib2.models.torch.encoder import TransformerEncoder
from rllib2.models.torch.pi import Pi, PiConfig

xformer_config = ModelConfig()
xformer_config.rnn_cell = "lstm"
encoder = TransformerEncoder(rnn_config)

config = PiConfig(
    observation_space=Box(low=-1, high=1, shape=(10,)),
    action_space=Discrete(2),
    encoder=encoder,
)
pi = Pi(config)
print(pi)

if __name__ == "__main__":

    config = PPOConfig()(
        # TODO: Do we want to pass pi_config to PPOConfig
        # or pi directly?
        pi=pi,
    )

    config.build()
    config.train()
