import numpy as np

from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils import try_import_torch

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
            q_hiddens=(256, ),
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
            q_hiddens (List[int]): List of layer-sizes after(!) the
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
        nn.Module.__init__(self)
        super(DQNTorchModel, self).__init__(obs_space, action_space,
                                            num_outputs, model_config, name)

        self.dueling = dueling
        ins = num_outputs

        # Dueling case: Build the shared (advantages and value) fc-network.
        advantage_module = nn.Sequential()
        value_module = None
        if self.dueling:
            value_module = nn.Sequential()
            for i, n in enumerate(q_hiddens):
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
            advantage_module.add_module("A", nn.Linear(ins, action_space.n))
            value_module.add_module("V", nn.Linear(ins, 1))
        # Non-dueling:
        # Q-value layer (use main module's outputs as Q-values).
        else:
            pass

        self.advantage_module = advantage_module
        self.value_module = value_module

    def get_advantages_or_q_values(self, model_out):
        """Returns distributional values for Q(s, a) given a state embedding.

        Override this in your custom model to customize the Q output head.

        Arguments:
            model_out (Tensor): embedding from the model layers

        Returns:
            (action_scores, logits, dist) if num_atoms == 1, otherwise
            (action_scores, z, support_logits_per_action, logits, dist)
        """

        return self.advantage_module(model_out)

    def get_state_value(self, model_out):
        """Returns the state value prediction for the given state embedding."""

        return self.value_module(model_out)

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

        epsilon_in = torch.normal(
            mean=torch.zeros([in_size]), std=torch.ones([in_size]))
        epsilon_out = torch.normal(
            mean=torch.zeros([out_size]), std=torch.ones([out_size]))
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
