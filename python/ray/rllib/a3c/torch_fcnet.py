from ray.rllib.a3c.torchpolicy import Policy
import torch
import torch.nn as nn
from torch.autograd import Variable
import torch.nn.functional as F


class Linear(Policy):

    def _init(self, inputs, num_outputs, options):
        hiddens = options.get("fcnet_hiddens", [16, 16])
        fcnet_activation = options.get("fcnet_activation", "tanh")
        activation = None
        if fcnet_activation == "tanh":
            activation = nn.Tanh
        elif fcnet_activation == "relu":
            activation = nn.ReLU

        layers = []
        last_layer_size = inputs
        for size in hiddens:
            layers.append(nn.Linear(last_layer_size, size))
            layers.append(activation())
            last_layer_size = size

        self.hidden_layers = nn.Sequential(*layers)

        self.logits = nn.Linear(last_layer_size, num_outputs)
        self.probs = nn.Softmax()
        self.value_branch = nn.Linear(last_layer_size, 1)
        self.setup_loss()

    def compute_action(self, x, *args):
        x = Variable(torch.from_numpy(x).float())
        logits, values, features = self(x)
        samples = self.probs(logits.unsqueeze(0)).multinomial().squeeze()
        return self.var_to_np(samples), self.var_to_np(values), features

    def compute_logits(self, x, *args):
        x = Variable(torch.from_numpy(x).float())
        res = self.hidden_layers(x)
        return self.var_to_np(self.logits(res))

    def value(self, x, *args):
        x = Variable(torch.from_numpy(x).float())
        res = self.hidden_layers(x)
        res = self.value_branch(res)
        return self.var_to_np(res)
