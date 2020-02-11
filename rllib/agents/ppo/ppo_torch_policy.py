import logging
import guppy

import ray
from ray.rllib.agents.impala.vtrace_policy import BEHAVIOUR_LOGITS
from ray.rllib.agents.a3c.a3c_torch_policy import apply_grad_clipping
from ray.rllib.agents.ppo.ppo_tf_policy import postprocess_ppo_gae, \
   setup_config
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.policy import ACTION_LOGP
from ray.rllib.policy.torch_policy import EntropyCoeffSchedule, \
   LearningRateSchedule
from ray.rllib.policy.torch_policy_template import build_torch_policy
# from ray.rllib.utils.explained_variance import explained_variance
from ray.rllib.utils.torch_ops import sequence_mask
from ray.rllib.utils import try_import_torch

torch, nn = try_import_torch()
h = guppy.hpy()
logger = logging.getLogger(__name__)


class PPOLoss:
    def __init__(self,
                 dist_class,
                 model,
                 value_targets,
                 advantages,
                 actions,
                 prev_logits,
                 prev_actions_logp,
                 vf_preds,
                 curr_action_dist,
                 value_fn,
                 cur_kl_coeff,
                 valid_mask,
                 entropy_coeff=0,
                 clip_param=0.1,
                 vf_clip_param=0.1,
                 vf_loss_coeff=1.0,
                 use_gae=True):
        pass
        """Constructs the loss for Proximal Policy Objective.

        Arguments:
            dist_class: action distribution class for logits.
            value_targets (Placeholder): Placeholder for target values; used
                for GAE.
            actions (Placeholder): Placeholder for actions taken
                from previous model evaluation.
            advantages (Placeholder): Placeholder for calculated advantages
                from previous model evaluation.
            prev_logits (Placeholder): Placeholder for logits output from
                previous model evaluation.
            prev_actions_logp (Placeholder): Placeholder for prob output from
                previous model evaluation.
            vf_preds (Placeholder): Placeholder for value function output
                from previous model evaluation.
            curr_action_dist (ActionDistribution): ActionDistribution
                of the current model.
            value_fn (Tensor): Current value function output Tensor.
            cur_kl_coeff (Variable): Variable holding the current PPO KL
                coefficient.
            valid_mask (Tensor): A bool mask of valid input elements (#2992).
            entropy_coeff (float): Coefficient of the entropy regularizer.
            clip_param (float): Clip parameter
            vf_clip_param (float): Clip parameter for the value function
            vf_loss_coeff (float): Coefficient of the value function loss
            use_gae (bool): If true, use the Generalized Advantage Estimator.
        """


"""
        def reduce_mean_valid(t):
            return torch.mean(t * valid_mask)

        prev_dist = dist_class(prev_logits, model)
        # Make loss functions.
        logp_ratio = torch.exp(
            curr_action_dist.logp(actions) - prev_actions_logp)
        action_kl = prev_dist.kl(curr_action_dist)
        self.mean_kl = reduce_mean_valid(action_kl)

        curr_entropy = curr_action_dist.entropy()
        self.mean_entropy = reduce_mean_valid(curr_entropy)

        surrogate_loss = torch.min(
            advantages * logp_ratio,
            advantages * torch.clamp(logp_ratio, 1 - clip_param,
                                     1 + clip_param))
        self.mean_policy_loss = reduce_mean_valid(-surrogate_loss)

        if use_gae:
            vf_loss1 = torch.pow(value_fn - value_targets, 2.0)
            vf_clipped = vf_preds + torch.clamp(value_fn - vf_preds,
                                                -vf_clip_param, vf_clip_param)
            vf_loss2 = torch.pow(vf_clipped - value_targets, 2.0)
            vf_loss = torch.max(vf_loss1, vf_loss2)
            self.mean_vf_loss = reduce_mean_valid(vf_loss)
            loss = reduce_mean_valid(
                -surrogate_loss + cur_kl_coeff * action_kl +
                vf_loss_coeff * vf_loss - entropy_coeff * curr_entropy)
        else:
            self.mean_vf_loss = 0.0
            loss = reduce_mean_valid(-surrogate_loss +
                                     cur_kl_coeff * action_kl -
                                     entropy_coeff * curr_entropy)
        self.loss = loss
"""


def ppo_surrogate_loss(policy, model, dist_class, train_batch):
    # h.heap()
    logits, state = model.from_batch(train_batch)
    action_dist = dist_class(logits, model)

    if state:
        max_seq_len = torch.max(train_batch["seq_lens"])
        mask = sequence_mask(train_batch["seq_lens"], max_seq_len)
        mask = torch.reshape(mask, [-1])
    else:
        mask = torch.ones_like(
            train_batch[Postprocessing.ADVANTAGES], dtype=torch.bool)

    def reduce_mean_valid(t):
        return torch.mean(t * mask)

    prev_dist = dist_class(train_batch[BEHAVIOUR_LOGITS], model)
    # Make loss functions.
    logp_ratio = torch.exp(
        action_dist.logp(train_batch[SampleBatch.ACTIONS]) -
        train_batch[ACTION_LOGP])
    action_kl = prev_dist.kl(action_dist)
    # mean_kl = reduce_mean_valid(action_kl)

    curr_entropy = action_dist.entropy()
    # mean_entropy = reduce_mean_valid(curr_entropy)

    surrogate_loss = torch.min(
        train_batch[Postprocessing.ADVANTAGES] * logp_ratio,
        train_batch[Postprocessing.ADVANTAGES] * torch.clamp(
            logp_ratio, 1 - policy.config["clip_param"],
            1 + policy.config["clip_param"]))
    # mean_policy_loss = reduce_mean_valid(-surrogate_loss)

    if policy.config["use_gae"]:
        value_fn = model.value_function()
        vf_loss1 = torch.pow(
            value_fn - train_batch[Postprocessing.VALUE_TARGETS], 2.0)
        vf_clipped = train_batch[SampleBatch.VF_PREDS] + torch.clamp(
            value_fn - train_batch[SampleBatch.VF_PREDS],
            -policy.config["vf_clip_param"], policy.config["vf_clip_param"])
        vf_loss2 = torch.pow(
            vf_clipped - train_batch[Postprocessing.VALUE_TARGETS], 2.0)
        vf_loss = torch.max(vf_loss1, vf_loss2)
        # mean_vf_loss = reduce_mean_valid(vf_loss)
        loss = reduce_mean_valid(-surrogate_loss +
                                 policy.kl_coeff * action_kl +
                                 policy.config["vf_loss_coeff"] * vf_loss -
                                 policy.entropy_coeff * curr_entropy)
    else:
        # mean_vf_loss = 0.0
        loss = reduce_mean_valid(-surrogate_loss +
                                 policy.kl_coeff * action_kl -
                                 policy.entropy_coeff * curr_entropy)
    return loss
    """policy.loss_obj = PPOLoss(
        dist_class,
        model,
        train_batch[Postprocessing.VALUE_TARGETS],
        train_batch[Postprocessing.ADVANTAGES],
        train_batch[SampleBatch.ACTIONS],
        train_batch[BEHAVIOUR_LOGITS],
        train_batch[ACTION_LOGP],
        train_batch[SampleBatch.VF_PREDS],
        action_dist,
        model.value_function(),
        policy.kl_coeff,
        mask,
        entropy_coeff=policy.entropy_coeff,
        clip_param=policy.config["clip_param"],
        vf_clip_param=policy.config["vf_clip_param"],
        vf_loss_coeff=policy.config["vf_loss_coeff"],
        use_gae=policy.config["use_gae"],
    )

    return policy.loss_obj.loss
    """


def kl_and_loss_stats(policy, train_batch):
    # return {
    #    "cur_kl_coeff": policy.kl_coeff,
    #    "cur_lr": policy.cur_lr,
    #    "total_loss": policy.loss_obj.loss.cpu().detach().numpy(),
    #   "policy_loss": policy.loss_obj.mean_policy_loss.cpu().detach().numpy(),
    #    "vf_loss": policy.loss_obj.mean_vf_loss.cpu().detach().numpy(),
    #    "vf_explained_var": explained_variance(
    #        train_batch[Postprocessing.VALUE_TARGETS],
    #        policy.model.value_function(),
    #        framework="torch").cpu().detach().numpy(),
    #    "kl": policy.loss_obj.mean_kl.cpu().detach().numpy(),
    #    "entropy": policy.loss_obj.mean_entropy.cpu().detach().numpy(),
    #    "entropy_coeff": policy.entropy_coeff,
    # }
    return {}


def vf_preds_and_logits_fetches(policy, input_dict, state_batches, model,
                                action_dist):
    """Adds value function and logits outputs to experience train_batches."""
    return {
        SampleBatch.VF_PREDS: policy.model.value_function().cpu().numpy(),
        BEHAVIOUR_LOGITS: policy.model.last_output().cpu().numpy(),
        ACTION_LOGP: action_dist.logp(
            input_dict[SampleBatch.ACTIONS]).cpu().numpy(),
    }


class KLCoeffMixin:
    def __init__(self, config):
        # KL Coefficient.
        self.kl_coeff = config["kl_coeff"]
        self.kl_target = config["kl_target"]

    def update_kl(self, sampled_kl):
        if sampled_kl > 2.0 * self.kl_target:
            self.kl_coeff *= 1.5
        elif sampled_kl < 0.5 * self.kl_target:
            self.kl_coeff *= 0.5
        return self.kl_coeff


class ValueNetworkMixin:
    def __init__(self, obs_space, action_space, config):
        if config["use_gae"]:

            def value(ob, prev_action, prev_reward, *state):
                model_out, _ = self.model({
                    SampleBatch.CUR_OBS: torch.Tensor([ob]).to(self.device),
                    SampleBatch.PREV_ACTIONS: torch.Tensor([prev_action]).to(
                        self.device),
                    SampleBatch.PREV_REWARDS: torch.Tensor([prev_reward]).to(
                        self.device),
                    "is_training": False,
                }, [torch.Tensor([s]).to(self.device) for s in state],
                                          torch.Tensor([1]).to(self.device))
                return self.model.value_function()[0]

        else:

            def value(ob, prev_action, prev_reward, *state):
                return 0.0

        self._value = value


def setup_mixins(policy, obs_space, action_space, config):
    ValueNetworkMixin.__init__(policy, obs_space, action_space, config)
    KLCoeffMixin.__init__(policy, config)
    EntropyCoeffSchedule.__init__(policy, config["entropy_coeff"],
                                  config["entropy_coeff_schedule"])
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])


PPOTorchPolicy = build_torch_policy(
    name="PPOTorchPolicy",
    get_default_config=lambda: ray.rllib.agents.ppo.ppo.DEFAULT_CONFIG,
    loss_fn=ppo_surrogate_loss,
    stats_fn=kl_and_loss_stats,
    extra_action_out_fn=vf_preds_and_logits_fetches,
    postprocess_fn=postprocess_ppo_gae,
    extra_grad_process_fn=apply_grad_clipping,
    before_init=setup_config,
    after_init=setup_mixins,
    mixins=[KLCoeffMixin, ValueNetworkMixin])
