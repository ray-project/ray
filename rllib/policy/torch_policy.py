import numpy as np

from ray.rllib.policy.policy import Policy, LEARNER_STATS_KEY
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import try_import_torch
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.tracking_dict import UsageTrackingDict
from ray.rllib.utils.schedules import ConstantSchedule, PiecewiseSchedule

torch, _ = try_import_torch()


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
    def __init__(self, observation_space, action_space, config, model, loss,
                 action_distribution_class):
        """Build a policy from policy and loss torch modules.

        Note that model will be placed on GPU device if CUDA_VISIBLE_DEVICES
        is set. Only single GPU is supported for now.

        Arguments:
            observation_space (gym.Space): observation space of the policy.
            action_space (gym.Space): action space of the policy.
            config (dict): The Policy config dict.
            model (nn.Module): PyTorch policy module. Given observations as
                input, this module must return a list of outputs where the
                first item is action logits, and the rest can be any value.
            loss (func): Function that takes (policy, model, dist_class,
                train_batch) and returns a single scalar loss.
            action_distribution_class (ActionDistribution): Class for action
                distribution.
        """
        super(TorchPolicy, self).__init__(
            observation_space, action_space, config
        )
        self.device = (torch.device("cuda")
                       if torch.cuda.is_available() else torch.device("cpu"))
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
                SampleBatch.CUR_OBS: obs_batch,
            })
            if prev_action_batch:
                input_dict[SampleBatch.PREV_ACTIONS] = prev_action_batch
            if prev_reward_batch:
                input_dict[SampleBatch.PREV_REWARDS] = prev_reward_batch
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
    def num_state_tensors(self):
        return len(self.model.get_initial_state())

    @override(Policy)
    def get_initial_state(self):
        return [s.numpy() for s in self.model.get_initial_state()]

    def extra_grad_process(self):
        """Allow subclass to do extra processing on gradients and
           return processing info."""
        return {}

    def extra_action_out(self, input_dict, state_batches, model,
                         action_dist=None):
        """Returns dict of extra info to include in experience batch.

        Arguments:
            input_dict (dict): Dict of model input tensors.
            state_batches (list): List of state tensors.
            model (TorchModelV2): Reference to the model.
            action_dist (Distribution): Torch Distribution object to get
                log-probs (e.g. for already sampled actions).
        """
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

    @override(Policy)
    def export_model(self, export_dir):
        """TODO: implement for torch.
        """
        raise NotImplementedError

    @override(Policy)
    def export_checkpoint(self, export_dir):
        """TODO: implement for torch.
        """
        raise NotImplementedError


@DeveloperAPI
class LearningRateSchedule(object):
    """Mixin for TFPolicy that adds a learning rate schedule."""

    @DeveloperAPI
    def __init__(self, lr, lr_schedule):
        self.cur_lr = lr
        if lr_schedule is None:
            self.lr_schedule = ConstantSchedule(lr)
        else:
            self.lr_schedule = PiecewiseSchedule(
                lr_schedule, outside_value=lr_schedule[-1][-1]
            )

    @override(Policy)
    def on_global_var_update(self, global_vars):
        super(LearningRateSchedule, self).on_global_var_update(global_vars)
        self.cur_lr = self.lr_schedule.value(global_vars["timestep"])

    @override(TorchPolicy)
    def optimizer(self):
        for p in self._optimizer.param_groups:
            p["lr"] = self.cur_lr
        return self._optimizer


@DeveloperAPI
class EntropyCoeffSchedule(object):
    """Mixin for TorchPolicy that adds entropy coeff decay."""

    @DeveloperAPI
    def __init__(self, entropy_coeff, entropy_coeff_schedule):
        self.entropy_coeff = entropy_coeff

        if entropy_coeff_schedule is None:
            self.entropy_coeff_schedule = ConstantSchedule(entropy_coeff)
        else:
            # Allows for custom schedule similar to lr_schedule format
            if isinstance(entropy_coeff_schedule, list):
                self.entropy_coeff_schedule = PiecewiseSchedule(
                    entropy_coeff_schedule,
                    outside_value=entropy_coeff_schedule[-1][-1])
            else:
                # Implements previous version but enforces outside_value
                self.entropy_coeff_schedule = PiecewiseSchedule(
                    [[0, entropy_coeff], [entropy_coeff_schedule, 0.0]],
                    outside_value=0.0)

    @override(Policy)
    def on_global_var_update(self, global_vars):
        super(EntropyCoeffSchedule, self).on_global_var_update(global_vars)
        self.entropy_coeff = self.entropy_coeff_schedule.value(
            global_vars["timestep"]
        )
