import numpy as np

from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import try_import_torch
from ray.rllib.utils.annotations import override

torch, nn = try_import_torch()


class DQNTorchModel(TorchModelV2, nn.Module):
    """Extension of standard TorchModelV2 to provide dueling-Q functionality.
    """

    def __init__(
            self,
            obs_space,
            action_space,
            num_outputs,
            model_config,
            name,
            *,
            dueling=False,
            dueling_hiddens=(256, ),
            dueling_activation="relu",
            use_noisy=False,
            sigma0=0.5,
            # TODO(sven): Move `add_layer_norm` into ModelCatalog as
            #  generic option, then error if we use ParameterNoise as
            #  Exploration type and do not have any LayerNorm layers in
            #  the net.
            add_layer_norm=False):
        """Initialize variables of this model.

        Extra model kwargs:
            dueling (bool): Whether to build the advantage(A)/value(V) heads
                for DDQN. If True, Q-values are calculated as:
                Q = (A - mean[A]) + V. If False, raw NN output is interpreted
                as Q-values.
            dueling_hiddens (List[int]): List of layer-sizes after(!) the
                Advantages(A)/Value(V)-split. Hence, each of the A- and V-
                branches will have this structure of Dense layers. To define
                the NN before this A/V-split, use - as always -
                config["model"]["fcnet_hiddens"].
            dueling_activation (str): The activation to use for all dueling
                layers (A- and V-branch). One of "relu", "tanh", "linear".
            use_noisy (bool): use noisy nets
            sigma0 (float): initial value of noisy nets
            add_layer_norm (bool): Enable layer norm (for param noise).
        """

        super().__init__(obs_space, action_space, num_outputs,
                         model_config, name)
        nn.Module.__init__(self)

        self.dueling = dueling

        # Dueling case: Build the shared (advantages and value) fc-network.
        # Non-dueling case: Build simple fc-net with Q-output layer.
        #shared_module = nn.Sequential()
        ins = obs_space.shape[-1]
        #for i, n in enumerate(dueling_hiddens):
        #    shared_module.add_module("dueling_{}".format(i), nn.Linear(ins, n))
        #    # Add activations if necessary.
        #    if dueling_activation == "relu":
        #        shared_module.add_module("dueling_act_{}".format(i), nn.ReLU())
        #    elif dueling_activation == "tanh":
        #        shared_module.add_module("dueling_act_{}".format(i), nn.Tanh())
        #    # Add LayerNorm after each Dense.
        #    if add_layer_norm:
        #        shared_module.add_module(
        #            "LayerNorm_{}".format(i),
        #            nn.LayerNorm(normalized_shape=(n, )))
        #    ins = n

        # Build dueling setup.
        advantage_module = nn.Sequential()
        value_module = nn.Sequential()
        if self.dueling:
            for i, n in enumerate(dueling_hiddens):
                advantage_module.add_module("dueling_A_{}".format(i),
                                            nn.Linear(ins, n))
                value_module.add_module("dueling_V_{}".format(i),
                                        nn.Linear(ins, n))
                # Add activations if necessary.
                if dueling_activation == "relu":
                    advantage_module.add_module("dueling_A_act_{}".format(i),
                                                nn.ReLU())
                    value_module.add_module("dueling_V_act_{}".format(i),
                                            nn.ReLU())
                elif dueling_activation == "tanh":
                    advantage_module.add_module("dueling_A_act_{}".format(i),
                                                nn.Tanh())
                    value_module.add_module("dueling_V_act_{}".format(i),
                                            nn.Tanh())

                # Add LayerNorm after each Dense.
                if add_layer_norm:
                    advantage_module.add_module("LayerNorm_A_{}".format(i),
                                                nn.LayerNorm(n))
                    value_module.add_module("LayerNorm_V_{}".format(i),
                                            nn.LayerNorm(n))
                ins = n
            # Actual Advantages layer (nodes=num-actions) and
            # value layer (nodes=1).
            advantage_module.add_module("A", nn.Linear(ins, num_outputs))
            value_module.add_module("V", nn.Linear(ins, 1))
        # Non-dueling:
        # Q-value layer (use Advantage module's outputs as Q-values).
        else:
            advantage_module.add_module("Q", nn.Linear(ins, num_outputs))

        self.advantage_module = advantage_module
        self.value_module = value_module
        #self.shared_module = shared_module

    #@override(TorchModelV2)
    #def forward(self, input_dict, hidden_state=None, seq_lens=None):
    #    out = input_dict[SampleBatch.CUR_OBS]
    #    # Calculate Q- or advantage values (num_outputs=number of
    #    # discrete actions).
    #    shared_out = self.shared_module(out)
    #    q_values = shared_out
    #    # In the dueling case, add the state-value to advantages and subtract
    #    # by the mean advantage (see paper on dueling Q).
    #    if self.dueling:
    #        advantages = self.advantage_module(shared_out)
    #        values = self.value_module(shared_out)
    #        q_values = (advantages - advantages.mean()) + values
    #    return q_values, []

    def _noisy_layer(self, action_in, out_size, sigma0, non_linear=True):
        """
        a common dense layer: y = w^{T}x + b
        a noisy layer: y = (w + \\epsilon_w*\\sigma_w)^{T}x +
            (b+\\epsilon_b*\\sigma_b)
        where \epsilon are random variables sampled from factorized normal
        distributions and \\sigma are trainable variables which are expected to
        vanish along the training procedure
        """
        in_size = int(action_in.shape[1])

        epsilon_in = torch.normal(size=[in_size])
        epsilon_out = torch.normal(size=[out_size])
        epsilon_in = self._f_epsilon(epsilon_in)
        epsilon_out = self._f_epsilon(epsilon_out)
        epsilon_w = torch.matmul(
            torch.unsqueeze(epsilon_in, -1),
            other=torch.unsqueeze(epsilon_out, 0))
        epsilon_b = epsilon_out

        sigma_w = torch.Tensor(
            data=np.random.uniform(
                low=-1.0 / np.sqrt(float(in_size)),
                high=1.0 / np.sqrt(float(in_size)),
                size=[in_size, out_size]),
            dtype=torch.float32,
            requires_grad=True)
        # TF noise generation can be unreliable on GPU
        # If generating the noise on the CPU,
        # lowering sigma0 to 0.1 may be helpful
        sigma_b = torch.Tensor(
            data=np.full(
                shape=[out_size], fill_value=sigma0 / np.sqrt(float(in_size))),
            requires_grad=True)
        w = torch.Tensor(
            data=np.full(
                shape=[in_size, out_size],
                fill_value=6 / np.sqrt(float(in_size) + float(out_size))),
            requires_grad=True)
        b = torch.Tensor(data=np.zeros([out_size]), requires_grad=True)
        action_activation = torch.matmul(action_in, w + sigma_w * epsilon_w) \
            + b + sigma_b * epsilon_b

        if not non_linear:
            return action_activation
        return nn.functional.relu(action_activation)

    def _f_epsilon(self, x):
        return torch.sign(x) * torch.pow(torch.abs(x), 0.5)
