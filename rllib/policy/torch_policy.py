import numpy as np
import time

from ray.rllib.policy.policy import Policy, LEARNER_STATS_KEY, ACTION_PROB, \
    ACTION_LOGP
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.schedules import ConstantSchedule, PiecewiseSchedule
from ray.rllib.utils.torch_ops import convert_to_non_torch_type
from ray.rllib.utils.tracking_dict import UsageTrackingDict

torch, _ = try_import_torch()


class TorchPolicy(Policy):
    """Template for a PyTorch policy and loss to use with RLlib.

    This is similar to TFPolicy, but for PyTorch.

    Attributes:
        observation_space (gym.Space): observation space of the policy.
        action_space (gym.Space): action space of the policy.
        config (dict): config of the policy.
        model (TorchModel): Torch model instance.
        dist_class (type): Torch action distribution class.
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
        self.framework = "torch"
        super().__init__(observation_space, action_space, config)
        self.device = (torch.device("cuda")
                       if torch.cuda.is_available() else torch.device("cpu"))
        self.model = model.to(self.device)
        self.unwrapped_model = model  # used to support DistributedDataParallel
        self._loss = loss
        self._optimizer = self.optimizer()
        self.dist_class = action_distribution_class

        # If set, means we are using distributed allreduce during learning.
        self.distributed_world_size = None

    @override(Policy)
    def compute_actions(self,
                        obs_batch,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        explore=None,
                        timestep=None,
                        **kwargs):

        explore = explore if explore is not None else self.config["explore"]
        timestep = timestep if timestep is not None else self.global_timestep

        with torch.no_grad():
            input_dict = self._lazy_tensor_dict({
                SampleBatch.CUR_OBS: obs_batch,
            })
            if prev_action_batch:
                input_dict[SampleBatch.PREV_ACTIONS] = prev_action_batch
            if prev_reward_batch:
                input_dict[SampleBatch.PREV_REWARDS] = prev_reward_batch
            state_batches = [self._convert_to_tensor(s) for s in state_batches]

            # Call the exploration before_compute_actions hook.
            self.exploration.before_compute_actions(timestep=timestep)

            model_out = self.model(input_dict, state_batches,
                                   self._convert_to_tensor([1]))
            logits, state = model_out
            action_dist = None
            actions, logp = \
                self.exploration.get_exploration_action(
                    logits, self.dist_class, self.model,
                    timestep, explore)
            input_dict[SampleBatch.ACTIONS] = actions

            extra_action_out = self.extra_action_out(input_dict, state_batches,
                                                     self.model, action_dist)
            if logp is not None:
                logp = convert_to_non_torch_type(logp)
                extra_action_out.update({
                    ACTION_PROB: np.exp(logp),
                    ACTION_LOGP: logp
                })
            return convert_to_non_torch_type((actions, state,
                                              extra_action_out))

    @override(Policy)
    def compute_log_likelihoods(self,
                                actions,
                                obs_batch,
                                state_batches=None,
                                prev_action_batch=None,
                                prev_reward_batch=None):
        with torch.no_grad():
            input_dict = self._lazy_tensor_dict({
                SampleBatch.CUR_OBS: obs_batch,
                SampleBatch.ACTIONS: actions
            })
            if prev_action_batch:
                input_dict[SampleBatch.PREV_ACTIONS] = prev_action_batch
            if prev_reward_batch:
                input_dict[SampleBatch.PREV_REWARDS] = prev_reward_batch

            parameters, _ = self.model(input_dict, state_batches, [1])
            action_dist = self.dist_class(parameters, self.model)
            log_likelihoods = action_dist.logp(input_dict[SampleBatch.ACTIONS])
            return log_likelihoods

    @override(Policy)
    def learn_on_batch(self, postprocessed_batch):
        train_batch = self._lazy_tensor_dict(postprocessed_batch)

        loss_out = self._loss(self, self.model, self.dist_class, train_batch)
        self._optimizer.zero_grad()
        loss_out.backward()

        info = {}
        info.update(self.extra_grad_process())

        if self.distributed_world_size:
            grads = []
            for p in self.model.parameters():
                if p.grad is not None:
                    grads.append(p.grad)
            start = time.time()
            if torch.cuda.is_available():
                # Sadly, allreduce_coalesced does not work with CUDA yet.
                for g in grads:
                    torch.distributed.all_reduce(
                        g, op=torch.distributed.ReduceOp.SUM)
            else:
                torch.distributed.all_reduce_coalesced(
                    grads, op=torch.distributed.ReduceOp.SUM)
            for p in self.model.parameters():
                if p.grad is not None:
                    p.grad /= self.distributed_world_size
            info["allreduce_latency"] = time.time() - start

        self._optimizer.step()
        info.update(self.extra_grad_info(train_batch))

        return {LEARNER_STATS_KEY: info}

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
    def is_recurrent(self):
        return len(self.model.get_initial_state()) > 0

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

    def extra_action_out(self,
                         input_dict,
                         state_batches,
                         model,
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
        train_batch.set_get_interceptor(self._convert_to_tensor)
        return train_batch

    def _convert_to_tensor(self, arr):
        if torch.is_tensor(arr):
            return arr.to(self.device)
        tensor = torch.from_numpy(np.asarray(arr))
        if tensor.dtype == torch.double:
            tensor = tensor.float()
        return tensor.to(self.device)

    @override(Policy)
    def export_model(self, export_dir):
        """TODO(sven): implement for torch.
        """
        raise NotImplementedError

    @override(Policy)
    def export_checkpoint(self, export_dir):
        """TODO(sven): implement for torch.
        """
        raise NotImplementedError

    @override(Policy)
    def import_model_from_h5(self, import_file):
        """Imports weights into torch model."""
        return self.model.import_from_h5(import_file)


@DeveloperAPI
class LearningRateSchedule:
    """Mixin for TFPolicy that adds a learning rate schedule."""

    @DeveloperAPI
    def __init__(self, lr, lr_schedule):
        self.cur_lr = lr
        if lr_schedule is None:
            self.lr_schedule = ConstantSchedule(lr, framework=None)
        else:
            self.lr_schedule = PiecewiseSchedule(
                lr_schedule, outside_value=lr_schedule[-1][-1], framework=None)

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
class EntropyCoeffSchedule:
    """Mixin for TorchPolicy that adds entropy coeff decay."""

    @DeveloperAPI
    def __init__(self, entropy_coeff, entropy_coeff_schedule):
        self.entropy_coeff = entropy_coeff

        if entropy_coeff_schedule is None:
            self.entropy_coeff_schedule = ConstantSchedule(
                entropy_coeff, framework=None)
        else:
            # Allows for custom schedule similar to lr_schedule format
            if isinstance(entropy_coeff_schedule, list):
                self.entropy_coeff_schedule = PiecewiseSchedule(
                    entropy_coeff_schedule,
                    outside_value=entropy_coeff_schedule[-1][-1],
                    framework=None)
            else:
                # Implements previous version but enforces outside_value
                self.entropy_coeff_schedule = PiecewiseSchedule(
                    [[0, entropy_coeff], [entropy_coeff_schedule, 0.0]],
                    outside_value=0.0,
                    framework=None)

    @override(Policy)
    def on_global_var_update(self, global_vars):
        super(EntropyCoeffSchedule, self).on_global_var_update(global_vars)
        self.entropy_coeff = self.entropy_coeff_schedule.value(
            global_vars["timestep"])
