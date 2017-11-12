from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch
from torch.autograd import Variable
import torch.nn.functional as F

from ray.rllib.a3c.torchpolicy import TorchPolicy
from ray.rllib.models.pytorch.misc import var_to_np, convert_batch
from ray.rllib.models.catalog import ModelCatalog


class SharedTorchPolicy(TorchPolicy):
    """Assumes nonrecurrent."""

    def __init__(self, ob_space, ac_space, **kwargs):
        super(SharedTorchPolicy, self).__init__(
            ob_space, ac_space, **kwargs)

    def _setup_graph(self, ob_space, ac_space):
        _, self.logit_dim = ModelCatalog.get_action_dist(ac_space)
        self._model = ModelCatalog.get_torch_model(ob_space, self.logit_dim)
        self.optimizer = torch.optim.Adam(self._model.parameters(), lr=0.0001)

    def compute_action(self, ob, *args):
        """Should take in a SINGLE ob"""
        with self.lock:
            ob = Variable(torch.from_numpy(ob).float().unsqueeze(0))
            logits, values = self._model(ob)
            samples = self._model.probs(logits).multinomial().squeeze()
            values = values.squeeze(0)
            return var_to_np(samples), var_to_np(values)

    def compute_logits(self, ob, *args):
        with self.lock:
            ob = Variable(torch.from_numpy(ob).float().unsqueeze(0))
            res = self._model.hidden_layers(ob)
            return var_to_np(self._model.logits(res))

    def value(self, ob, *args):
        with self.lock:
            ob = Variable(torch.from_numpy(ob).float().unsqueeze(0))
            res = self._model.hidden_layers(ob)
            res = self._model.value_branch(res)
            res = res.squeeze(0)
            return var_to_np(res)

    def _evaluate(self, obs, actions):
        """Passes in multiple obs."""
        logits, values = self._model(obs)
        log_probs = F.log_softmax(logits)
        probs = self._model.probs(logits)
        action_log_probs = log_probs.gather(1, actions.view(-1, 1))
        entropy = -(log_probs * probs).sum(-1).sum()
        return values, action_log_probs, entropy

    def _backward(self, batch):
        """Loss is encoded in here. Defining a new loss function
        would start by rewriting this function"""

        states, acs, advs, rs, _ = convert_batch(batch)
        values, ac_logprobs, entropy = self._evaluate(states, acs)
        pi_err = -(advs * ac_logprobs).sum()
        value_err = 0.5 * (values - rs).pow(2).sum()

        self.optimizer.zero_grad()
        overall_err = 0.5 * value_err + pi_err - entropy * 0.01
        overall_err.backward()
        torch.nn.utils.clip_grad_norm(self._model.parameters(), 40)

    def get_initial_features(self):
        return [None]
