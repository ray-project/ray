import gym
from gym.spaces import Discrete

from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.models.torch.misc import SlimFC

torch, nn = try_import_torch()


class CuriosityTorchModel(TorchModelV2, nn.Module):

    def __init__(self,
                 obs_space,
                 action_space,
                 num_outputs,
                 model_config,
                 name):
        """Initializes a CuriosityTorchModel object.
        """

        obs_space_dim = 16
        embedding_dim = 16
        action_space_dim = 1

        # List of dimension of each layer
        features_dims = [obs_space_dim, 64, embedding_dim]
        inverse_dims = [2 * obs_space_dim, 32, action_space_dim]
        forwards_dims = [embedding_dim + action_space_dim, 123, embedding_dim]

        self.features_model = nn.Sequential(
            SlimFC(
                in_size=features_dims[0],
                out_size=features_dims[1],
                use_bias=False,
                activation_fn=nn.ReLU),
            SlimFC(
                in_size=features_dims[1],
                out_size=features_dims[2],
                use_bias=False,
                activation_fn=nn.ReLU))

        self.inverse_model = nn.Sequential(
            SlimFC(
                in_size=inverse_dims[0],
                out_size=inverse_dims[1],
                use_bias=False,
                activation_fn=nn.ReLU),
            SlimFC(
                in_size=inverse_dims[1],
                out_size=inverse_dims[2],
                use_bias=False,
                activation_fn=nn.ReLU))

        self.forwards_model = None # how do we learn embedding? VAE?

        nn.Module.__init__(self)

        # why was this in the dyna code ??
        input_space = gym.spaces.Box(
            obs_space.low[0],
            obs_space.high[0],
            shape=(obs_space.shape[0] + action_space.n, ))

        super(CuriosityTorchModel, self).__init__(input_space, action_space,
                                             num_outputs, model_config, name)

    def predict_action(self, state, next_state):
        return self.inverse_model( torch.cat((state, next_state), axis=0))

    def get_latent_vector(self, state):
        return self.features_model(state)

    def predict_next_state(self, state, action):
        return self.forwards_model( torch.cat((state, action), axis=0))

    # how is the action info being fed in the input_dict
    def forward(self, blah):
        state = torch.randn(10)
        action = 1
        next_state = torch.randn(10)

        state_emb = self.get_latent_vector(state)
        next_state_emd = self.get_latent_vector(next_state)



