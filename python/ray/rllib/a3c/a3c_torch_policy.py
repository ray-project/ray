from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from threading import Lock

import torch
import torch.nn.functional as F

from ray.rllib.models.pytorch.misc import var_to_np, convert_batch
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.process_rollout import compute_advantages
from ray.rllib.utils.policy_graph import PolicyGraph


class SharedTorchPolicy(PolicyGraph):
    """A simple, non-recurrent PyTorch policy example."""

    def __init__(self, obs_space, action_space, registry, config):
        self.registry = registry
        self.local_steps = 0
        self.config = config
        self.summarize = config.get("summarize")
        self.setup_graph(obs_space, action_space)
        torch.set_num_threads(2)
        self.lock = Lock()

    def setup_graph(self, obs_space, action_space):
        _, self.logit_dim = ModelCatalog.get_action_dist(action_space)
        self._model = ModelCatalog.get_torch_model(
            self.registry, obs_space.shape, self.logit_dim,
            self.config["model"])
        self.optimizer = torch.optim.Adam(
            self._model.parameters(), lr=self.config["lr"])

    def compute_single_action(self, obs, state, is_training=False):
        assert not state, "RNN not supported"
        with self.lock:
            ob = torch.from_numpy(obs).float().unsqueeze(0)
            logits, values = self._model(ob)
            samples = F.softmax(logits, dim=1).multinomial(1).squeeze()
            values = values.squeeze()
            return var_to_np(samples), [], {"vf_preds": var_to_np(values)}

    def compute_gradients(self, samples):
        with self.lock:
            self.backward(samples)
            # Note that return values are just references;
            # calling zero_grad will modify the values
            return [p.grad.data.numpy() for p in self._model.parameters()], {}

    def apply_gradients(self, grads):
        self.optimizer.zero_grad()
        for g, p in zip(grads, self._model.parameters()):
            p.grad = torch.from_numpy(g)
        self.optimizer.step()
        return {}

    def get_weights(self):
        # !! This only returns references to the data.
        return self._model.state_dict()

    def set_weights(self, weights):
        with self.lock:
            self._model.load_state_dict(weights)

    def value(self, obs):
        with self.lock:
            obs = torch.from_numpy(obs).float().unsqueeze(0)
            res = self._model.hidden_layers(obs)
            res = self._model.value_branch(res)
            res = res.squeeze()
            return var_to_np(res)

    def forward(self, obs_batch, actions):
        logits, values = self._model(obs_batch)
        log_probs = F.log_softmax(logits, dim=1)
        probs = F.softmax(logits, dim=1)
        action_log_probs = log_probs.gather(1, actions.view(-1, 1))
        entropy = -(log_probs * probs).sum(-1).sum()
        return values, action_log_probs, entropy

    def backward(self, sample_batch):
        """Loss is encoded here.

        Defining a new loss function would start by rewriting this function.
        """

        states, actions, advs, rs = convert_batch(sample_batch)
        values, action_log_probs, entropy = self.forward(states, actions)
        pi_err = -advs.dot(action_log_probs.reshape(-1))
        value_err = F.mse_loss(values.reshape(-1), rs)

        self.optimizer.zero_grad()

        overall_err = sum([
            pi_err,
            self.config["vf_loss_coeff"] * value_err,
            self.config["entropy_coeff"] * entropy,
        ])

        overall_err.backward()
        torch.nn.utils.clip_grad_norm_(self._model.parameters(),
                                       self.config["grad_clip"])

    def postprocess_trajectory(self, sample_batch, other_agent_batches=None):
        completed = sample_batch["dones"][-1]
        if completed:
            last_r = 0.0
        else:
            last_r = self.value(sample_batch["new_obs"][-1])
        return compute_advantages(
            sample_batch, last_r, self.config["gamma"], self.config["lambda"])
