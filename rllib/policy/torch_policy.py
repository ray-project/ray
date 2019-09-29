from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import os

try:
    import torch
except ImportError:
    pass  # soft dep

from ray.rllib.policy.policy import Policy, LEARNER_STATS_KEY
from ray.rllib.utils.annotations import override
from ray.rllib.utils.tracking_dict import UsageTrackingDict


class TorchPolicy(Policy):
    """Template for a PyTorch policy and loss to use with RLlib.

    This is similar to TFPolicy, but for PyTorch.

    Attributes:
        observation_space (gym.Space): observation space of the policy.
        action_space (gym.Space): action space of the policy.
        config (dict): config of the policy
        model (TorchModel): Torch model instance
        dist_class (type): Torch action distribution class
    """

    def __init__(self, observation_space, action_space, model, loss,
                 action_distribution_class):
        """Build a policy from policy and loss torch modules.

        Note that model will be placed on GPU device if CUDA_VISIBLE_DEVICES
        is set. Only single GPU is supported for now.

        Arguments:
            observation_space (gym.Space): observation space of the policy.
            action_space (gym.Space): action space of the policy.
            model (nn.Module): PyTorch policy module. Given observations as
                input, this module must return a list of outputs where the
                first item is action logits, and the rest can be any value.
            loss (func): Function that takes (policy, model, dist_class,
                train_batch) and returns a single scalar loss.
            action_distribution_class (ActionDistribution): Class for action
                distribution.
        """
        self.observation_space = observation_space
        self.action_space = action_space
        self.device = (torch.device("cuda")
                       if bool(os.environ.get("CUDA_VISIBLE_DEVICES", None))
                       else torch.device("cpu"))
        self.model = model.to(self.device)
        self._loss = loss
        self._optimizer = self.optimizer()
        self.dist_class = action_distribution_class

    @override(Policy)
    def compute_actions(self,
                        obs_batch,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        **kwargs):
        with torch.no_grad():
            input_dict = self._lazy_tensor_dict({
                "obs": obs_batch,
            })
            if prev_action_batch:
                input_dict["prev_actions"] = prev_action_batch
            if prev_reward_batch:
                input_dict["prev_rewards"] = prev_reward_batch
            model_out = self.model(input_dict, state_batches, [1])
            logits, state = model_out
            action_dist = self.dist_class(logits, self.model)
            actions = action_dist.sample()
            return (actions.cpu().numpy(), [h.cpu().numpy() for h in state],
                    self.extra_action_out(input_dict, state_batches,
                                          self.model))

    @override(Policy)
    def learn_on_batch(self, postprocessed_batch):
        train_batch = self._lazy_tensor_dict(postprocessed_batch)

        loss_out = self._loss(self, self.model, self.dist_class, train_batch)
        self._optimizer.zero_grad()
        loss_out.backward()

        grad_process_info = self.extra_grad_process()
        self._optimizer.step()

        grad_info = self.extra_grad_info(train_batch)
        grad_info.update(grad_process_info)
        return {LEARNER_STATS_KEY: grad_info}

    @override(Policy)
    def compute_gradients(self, postprocessed_batch):
        train_batch = self._lazy_tensor_dict(postprocessed_batch)

        loss_out = self._loss(self, self.model, self.dist_class, train_batch)
        self._optimizer.zero_grad()
        loss_out.backward()

        grad_process_info = self.extra_grad_process()

        # Note that return values are just references;
        # calling zero_grad will modify the values
        grads = []
        for p in self.model.parameters():
            if p.grad is not None:
                grads.append(p.grad.data.cpu().numpy())
            else:
                grads.append(None)

        grad_info = self.extra_grad_info(train_batch)
        grad_info.update(grad_process_info)
        return grads, {LEARNER_STATS_KEY: grad_info}

    @override(Policy)
    def apply_gradients(self, gradients):
        for g, p in zip(gradients, self.model.parameters()):
            if g is not None:
                p.grad = torch.from_numpy(g).to(self.device)
        self._optimizer.step()

    @override(Policy)
    def get_weights(self):
        return {k: v.cpu() for k, v in self.model.state_dict().items()}

    @override(Policy)
    def set_weights(self, weights):
        self.model.load_state_dict(weights)

    @override(Policy)
    def get_initial_state(self):
        return [s.numpy() for s in self.model.get_initial_state()]

    def extra_grad_process(self):
        """Allow subclass to do extra processing on gradients and
           return processing info."""
        return {}

    def extra_action_out(self, input_dict, state_batches, model):
        """Returns dict of extra info to include in experience batch.

        Arguments:
            input_dict (dict): Dict of model input tensors.
            state_batches (list): List of state tensors.
            model (TorchModelV2): Reference to the model."""
        return {}

    def extra_grad_info(self, train_batch):
        """Return dict of extra grad info."""

        return {}

    def optimizer(self):
        """Custom PyTorch optimizer to use."""
        if hasattr(self, "config"):
            return torch.optim.Adam(
                self.model.parameters(), lr=self.config["lr"])
        else:
            return torch.optim.Adam(self.model.parameters())

    def _lazy_tensor_dict(self, postprocessed_batch):
        train_batch = UsageTrackingDict(postprocessed_batch)

        def convert(arr):
            tensor = torch.from_numpy(np.asarray(arr))
            if tensor.dtype == torch.double:
                tensor = tensor.float()
            return tensor.to(self.device)

        train_batch.set_get_interceptor(convert)
        return train_batch
