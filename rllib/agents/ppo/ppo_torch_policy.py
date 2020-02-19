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
from ray.rllib.utils.explained_variance import explained_variance
# from ray.rllib.utils.torch_ops import sequence_mask
from ray.rllib.utils import try_import_torch

torch, nn = try_import_torch()
h = guppy.hpy()
logger = logging.getLogger(__name__)


def ppo_surrogate_loss(policy, model, dist_class, train_batch):
    # h.heap()
    logits, state = model.from_batch(train_batch)
    action_dist = dist_class(logits, model)

    # Make loss functions.
    loss = action_dist.logp(train_batch[SampleBatch.ACTIONS])

    policy.loss = loss
    policy.mean_policy_loss = torch.mean(loss)
    policy.mean_vf_loss = torch.mean(loss)
    policy.mean_kl = torch.mean(loss)
    policy.mean_entropy = torch.mean(loss)
    return policy.mean_policy_loss


def kl_and_loss_stats(policy, train_batch):
    import torch
    import gc
    print("START")
    count = 0
    for obj in gc.get_objects():
        try:
            if torch.is_tensor(obj) or (
                    hasattr(obj, 'data') and torch.is_tensor(obj.data)):
                print(type(obj), obj.size())
                count += 1
        except:
            pass
    print("END: count={}".format(count))
    return {
        "cur_kl_coeff": policy.kl_coeff,
        "cur_lr": policy.cur_lr,
        #"total_loss": policy.loss.item(), #detach().cpu().numpy(),
        "policy_loss": policy.mean_policy_loss.item(),
        "vf_loss": policy.mean_vf_loss.item(),
        "vf_explained_var": explained_variance(
            train_batch[Postprocessing.VALUE_TARGETS],
            policy.model.value_function(),
            framework="torch").item(),
        "kl": policy.mean_kl.item(),
        "entropy": policy.mean_entropy.item(),
        "entropy_coeff": policy.entropy_coeff,
    }
    #return {}


def vf_preds_and_logits_fetches(policy, input_dict, state_batches, model,
                                action_dist):
    """Adds value function and logits outputs to experience train_batches."""
    return {
        SampleBatch.VF_PREDS: policy.model.value_function().detach().cpu().numpy(),
        #BEHAVIOUR_LOGITS: policy.model.last_output().cpu().numpy(),
        #ACTION_LOGP: action_dist.logp(
        #    input_dict[SampleBatch.ACTIONS]).cpu().numpy(),
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
