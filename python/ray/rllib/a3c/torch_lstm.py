from ray.rllib.a3c.torchpolicy import Policy
import torch
import torch.nn as nn
from torch.autograd import Variable
import torch.nn.functional as F
from ray.rllib.a3c.torchpolicy import convert_batch
import numpy as np



def normalized_columns_initializer(weights, std=1.0):
    out = torch.randn(weights.size())
    out *= std / torch.sqrt(out.pow(2).sum(1, keepdim=True))
    return out


def weights_init(m):
    classname = m.__class__.__name__
    if classname.find('Conv') != -1:
        weight_shape = list(m.weight.data.size())
        fan_in = np.prod(weight_shape[1:4])
        fan_out = np.prod(weight_shape[2:4]) * weight_shape[0]
        w_bound = np.sqrt(6. / (fan_in + fan_out))
        m.weight.data.uniform_(-w_bound, w_bound)
        m.bias.data.fill_(0)
    elif classname.find('Linear') != -1:
        weight_shape = list(m.weight.data.size())
        fan_in = weight_shape[1]
        fan_out = weight_shape[0]
        w_bound = np.sqrt(6. / (fan_in + fan_out))
        m.weight.data.uniform_(-w_bound, w_bound)
        m.bias.data.fill_(0)


class LSTM(Policy):

    def _init(self, input_shape, num_outputs, options):
        fcnet_activation = options.get("fcnet_activation", "tanh")
        activation = None
        if fcnet_activation == "tanh":
            activation = nn.Tanh
        elif fcnet_activation == "relu":
            activation = nn.ReLU
        elif fcnet_activation == "elu":
            activation = nn.ELU

        layers = []
        last_layer_size = input_shape[0] # Number of channels
        for i in range(4):
            layers.append(nn.Conv2d(last_layer_size, 32, 3, stride=2, padding=1))
            layers.append(activation())
            last_layer_size = 32

        self._convs = nn.Sequential(*layers)
        self.lstm = nn.LSTMCell(32 * 3 * 3, 256)

        self.logits = nn.Linear(256, num_outputs)
        self.value_branch = nn.Linear(256, 1)
        self.apply(weights_init)

        self.logits.weight.data = normalized_columns_initializer(
            self.logits.weight.data, 0.01)
        self.logits.bias.data.fill_(0)
        self.value_branch.weight.data = normalized_columns_initializer(
            self.value_branch.weight.data, 1.0)
        self.value_branch.bias.data.fill_(0)

        self.lstm.bias_ih.data.fill_(0)
        self.lstm.bias_hh.data.fill_(0)

        self.state_init = [np.zeros((1, 256), np.float32),
                           np.zeros((1, 256), np.float32)]

        self.probs = nn.Softmax()
        self.train()
        self.setup_loss()

    def _hiddens(self, inputs):
        """ Internal method - pass in Variables, not numpy arrays

        args:
            inputs: observations and features"""
        x, hiddens = inputs
        res = self._convs(x)
        res = res.view(-1, 32*3*3)
        return self.lstm(res, hiddens)


    def forward(self, inputs):
        """ Internal method - pass in Variables, not numpy arrays

        args:
            inputs: observations and features"""
        hx, cx = self._hiddens(inputs)
        return self.logits(hx), self.value_branch(hx), (hx, cx)

    def compute_action(self, x, hx, cx, *args):
        # TODO: convert features to variables
        x = Variable(torch.from_numpy(x).float().unsqueeze(0))
        hx = Variable(torch.from_numpy(hx).float())
        cx = Variable(torch.from_numpy(cx).float())
        logits, values, (hx, cx) = self((x, (hx, cx)))
        values = values.squeeze(0)
        samples = self.probs(logits).multinomial().squeeze()
        return self.var_to_np(samples), \
            self.var_to_np(values), \
            self.var_to_np(hx), \
            self.var_to_np(cx)


    def compute_logits(self, x, hx, cx,*args):
        x = Variable(torch.from_numpy(x).float().unsqueeze(0))
        hx = Variable(torch.from_numpy(hx).float())
        cx = Variable(torch.from_numpy(cx).float())
        hx, cx = self._hiddens((x, (hx, cx)))
        return self.var_to_np(self.logits(hx))

    def value(self, x, hx, cx, *args):
        x = Variable(torch.from_numpy(x).float().unsqueeze(0))
        hx = Variable(torch.from_numpy(hx).float())
        cx = Variable(torch.from_numpy(cx).float())
        hx, cx = self._hiddens((x, (hx, cx)))
        res = self.value_branch(hx)
        return self.var_to_np(res)

    def get_initial_features(self):
        return self.state_init

    def _backward(self, batch):
        states, acs, advs, rs, features = convert_batch(batch)
        values, ac_logprobs, entropy = self._evaluate((states, features), acs)
        pi_err = self._policy_entropy_loss(ac_logprobs, advs)
        value_err = (values - rs).pow(2).mean()

        self.optimizer.zero_grad()
        overall_err = value_err + pi_err - entropy * 0.1
        overall_err.backward()

    def _evaluate(self, inputs, actions):
        assert len(inputs) == 2, "Need to provide both state and features"

        logits, values, _ = self(inputs)
        log_probs = F.log_softmax(logits)
        probs = self.probs(logits)
        action_log_probs = log_probs.gather(1, actions.view(-1, 1))
        entropy = -(log_probs * probs).sum(-1).mean()
        return values, action_log_probs, entropy
