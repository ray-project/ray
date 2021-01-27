import logging

import ray
from ray.rllib.evaluation.postprocessing import compute_gae_for_sample_batch, \
    Postprocessing
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.agents.ppo.ppo_tf_policy import setup_config
from ray.rllib.agents.ppo.ppo_torch_policy import vf_preds_fetches, \
    ValueNetworkMixin
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_ops import apply_grad_clipping

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


def PPOLoss(dist_class,
            actions,
            curr_logits,
            behaviour_logits,
            advantages,
            value_fn,
            value_targets,
            vf_preds,
            cur_kl_coeff,
            entropy_coeff,
            clip_param,
            vf_clip_param,
            vf_loss_coeff,
            clip_loss=False):
    def surrogate_loss(actions, curr_dist, prev_dist, advantages, clip_param,
                       clip_loss):
        pi_new_logp = curr_dist.logp(actions)
        pi_old_logp = prev_dist.logp(actions)

        logp_ratio = torch.exp(pi_new_logp - pi_old_logp)
        if clip_loss:
            return torch.min(
                advantages * logp_ratio,
                advantages * torch.clamp(logp_ratio, 1 - clip_param,
                                         1 + clip_param))
        return advantages * logp_ratio

    def kl_loss(curr_dist, prev_dist):
        return prev_dist.kl(curr_dist)

    def entropy_loss(dist):
        return dist.entropy()

    def vf_loss(value_fn, value_targets, vf_preds, vf_clip_param=0.1):
        # GAE Value Function Loss
        vf_loss1 = torch.pow(value_fn - value_targets, 2.0)
        vf_clipped = vf_preds + torch.clamp(value_fn - vf_preds,
                                            -vf_clip_param, vf_clip_param)
        vf_loss2 = torch.pow(vf_clipped - value_targets, 2.0)
        vf_loss = torch.max(vf_loss1, vf_loss2)
        return vf_loss

    pi_new_dist = dist_class(curr_logits, None)
    pi_old_dist = dist_class(behaviour_logits, None)

    surr_loss = torch.mean(
        surrogate_loss(actions, pi_new_dist, pi_old_dist, advantages,
                       clip_param, clip_loss))
    kl_loss = torch.mean(kl_loss(pi_new_dist, pi_old_dist))
    vf_loss = torch.mean(
        vf_loss(value_fn, value_targets, vf_preds, vf_clip_param))
    entropy_loss = torch.mean(entropy_loss(pi_new_dist))

    total_loss = -surr_loss + cur_kl_coeff * kl_loss
    total_loss += vf_loss_coeff * vf_loss
    total_loss -= entropy_coeff * entropy_loss
    return total_loss, surr_loss, kl_loss, vf_loss, entropy_loss


# This is the computation graph for workers (inner adaptation steps)
class WorkerLoss(object):
    def __init__(self,
                 model,
                 dist_class,
                 actions,
                 curr_logits,
                 behaviour_logits,
                 advantages,
                 value_fn,
                 value_targets,
                 vf_preds,
                 cur_kl_coeff,
                 entropy_coeff,
                 clip_param,
                 vf_clip_param,
                 vf_loss_coeff,
                 clip_loss=False):
        self.loss, surr_loss, kl_loss, vf_loss, ent_loss = PPOLoss(
            dist_class=dist_class,
            actions=actions,
            curr_logits=curr_logits,
            behaviour_logits=behaviour_logits,
            advantages=advantages,
            value_fn=value_fn,
            value_targets=value_targets,
            vf_preds=vf_preds,
            cur_kl_coeff=cur_kl_coeff,
            entropy_coeff=entropy_coeff,
            clip_param=clip_param,
            vf_clip_param=vf_clip_param,
            vf_loss_coeff=vf_loss_coeff,
            clip_loss=clip_loss)


# This is the Meta-Update computation graph for main (meta-update step)
class MAMLLoss(object):
    def __init__(self,
                 model,
                 config,
                 dist_class,
                 value_targets,
                 advantages,
                 actions,
                 behaviour_logits,
                 vf_preds,
                 cur_kl_coeff,
                 policy_vars,
                 obs,
                 num_tasks,
                 split,
                 meta_opt,
                 inner_adaptation_steps=1,
                 entropy_coeff=0,
                 clip_param=0.3,
                 vf_clip_param=0.1,
                 vf_loss_coeff=1.0,
                 use_gae=True):

        import higher
        self.config = config
        self.num_tasks = num_tasks
        self.inner_adaptation_steps = inner_adaptation_steps
        self.clip_param = clip_param
        self.dist_class = dist_class
        self.cur_kl_coeff = cur_kl_coeff
        self.model = model
        self.vf_clip_param = vf_clip_param
        self.vf_loss_coeff = vf_loss_coeff
        self.entropy_coeff = entropy_coeff

        # Split episode tensors into [inner_adaptation_steps+1, num_tasks, -1]
        self.obs = self.split_placeholders(obs, split)
        self.actions = self.split_placeholders(actions, split)
        self.behaviour_logits = self.split_placeholders(
            behaviour_logits, split)
        self.advantages = self.split_placeholders(advantages, split)
        self.value_targets = self.split_placeholders(value_targets, split)
        self.vf_preds = self.split_placeholders(vf_preds, split)

        inner_opt = torch.optim.SGD(model.parameters(), lr=config["inner_lr"])
        surr_losses = []
        val_losses = []
        kl_losses = []
        entropy_losses = []
        meta_losses = []
        kls = []

        meta_opt.zero_grad()
        for i in range(self.num_tasks):
            with higher.innerloop_ctx(
                    model, inner_opt, copy_initial_weights=False) as (fnet,
                                                                      diffopt):
                inner_kls = []
                for step in range(self.inner_adaptation_steps):
                    ppo_loss, _, inner_kl_loss, _, _ = self.compute_losses(
                        fnet, step, i)
                    diffopt.step(ppo_loss)
                    inner_kls.append(inner_kl_loss)
                    kls.append(inner_kl_loss.detach())

                # Meta Update
                ppo_loss, s_loss, kl_loss, v_loss, ent = self.compute_losses(
                    fnet, self.inner_adaptation_steps, i, clip_loss=True)

                inner_loss = torch.mean(
                    torch.stack([
                        a * b for a, b in zip(
                            self.cur_kl_coeff[
                                i * self.inner_adaptation_steps:(i + 1) *
                                self.inner_adaptation_steps], inner_kls)
                    ]))
                meta_loss = (ppo_loss + inner_loss) / self.num_tasks
                meta_loss.backward()

                surr_losses.append(s_loss.detach())
                kl_losses.append(kl_loss.detach())
                val_losses.append(v_loss.detach())
                entropy_losses.append(ent.detach())
                meta_losses.append(meta_loss.detach())

        meta_opt.step()

        # Stats Logging
        self.mean_policy_loss = torch.mean(torch.stack(surr_losses))
        self.mean_kl_loss = torch.mean(torch.stack(kl_losses))
        self.mean_vf_loss = torch.mean(torch.stack(val_losses))
        self.mean_entropy = torch.mean(torch.stack(entropy_losses))
        self.mean_inner_kl = kls
        self.loss = torch.sum(torch.stack(meta_losses))
        # Hacky, needed to bypass RLlib backend
        self.loss.requires_grad = True

    def compute_losses(self,
                       model,
                       inner_adapt_iter,
                       task_iter,
                       clip_loss=False):
        obs = self.obs[inner_adapt_iter][task_iter]
        obs_dict = {"obs": obs, "obs_flat": obs}
        curr_logits, _ = model.forward(obs_dict, None, None)
        value_fns = model.value_function()
        ppo_loss, surr_loss, kl_loss, val_loss, ent_loss = PPOLoss(
            dist_class=self.dist_class,
            actions=self.actions[inner_adapt_iter][task_iter],
            curr_logits=curr_logits,
            behaviour_logits=self.behaviour_logits[inner_adapt_iter][
                task_iter],
            advantages=self.advantages[inner_adapt_iter][task_iter],
            value_fn=value_fns,
            value_targets=self.value_targets[inner_adapt_iter][task_iter],
            vf_preds=self.vf_preds[inner_adapt_iter][task_iter],
            cur_kl_coeff=0.0,
            entropy_coeff=self.entropy_coeff,
            clip_param=self.clip_param,
            vf_clip_param=self.vf_clip_param,
            vf_loss_coeff=self.vf_loss_coeff,
            clip_loss=clip_loss)
        return ppo_loss, surr_loss, kl_loss, val_loss, ent_loss

    def split_placeholders(self, placeholder, split):
        inner_placeholder_list = torch.split(
            placeholder, torch.sum(split, dim=1).tolist(), dim=0)
        placeholder_list = []
        for index, split_placeholder in enumerate(inner_placeholder_list):
            placeholder_list.append(
                torch.split(split_placeholder, split[index].tolist(), dim=0))
        return placeholder_list


def maml_loss(policy, model, dist_class, train_batch):
    logits, state = model.from_batch(train_batch)
    policy.cur_lr = policy.config["lr"]

    if policy.config["worker_index"]:
        policy.loss_obj = WorkerLoss(
            model=model,
            dist_class=dist_class,
            actions=train_batch[SampleBatch.ACTIONS],
            curr_logits=logits,
            behaviour_logits=train_batch[SampleBatch.ACTION_DIST_INPUTS],
            advantages=train_batch[Postprocessing.ADVANTAGES],
            value_fn=model.value_function(),
            value_targets=train_batch[Postprocessing.VALUE_TARGETS],
            vf_preds=train_batch[SampleBatch.VF_PREDS],
            cur_kl_coeff=0.0,
            entropy_coeff=policy.config["entropy_coeff"],
            clip_param=policy.config["clip_param"],
            vf_clip_param=policy.config["vf_clip_param"],
            vf_loss_coeff=policy.config["vf_loss_coeff"],
            clip_loss=False)
    else:
        policy.var_list = model.named_parameters()

        # `split` may not exist yet (during test-loss call), use a dummy value.
        # Cannot use get here due to train_batch being a TrackingDict.
        split = train_batch["split"] if "split" in train_batch else \
            torch.tensor([[8, 8], [8, 8]])
        policy.loss_obj = MAMLLoss(
            model=model,
            dist_class=dist_class,
            value_targets=train_batch[Postprocessing.VALUE_TARGETS],
            advantages=train_batch[Postprocessing.ADVANTAGES],
            actions=train_batch[SampleBatch.ACTIONS],
            behaviour_logits=train_batch[SampleBatch.ACTION_DIST_INPUTS],
            vf_preds=train_batch[SampleBatch.VF_PREDS],
            cur_kl_coeff=policy.kl_coeff_val,
            policy_vars=policy.var_list,
            obs=train_batch[SampleBatch.CUR_OBS],
            num_tasks=policy.config["num_workers"],
            split=split,
            config=policy.config,
            inner_adaptation_steps=policy.config["inner_adaptation_steps"],
            entropy_coeff=policy.config["entropy_coeff"],
            clip_param=policy.config["clip_param"],
            vf_clip_param=policy.config["vf_clip_param"],
            vf_loss_coeff=policy.config["vf_loss_coeff"],
            use_gae=policy.config["use_gae"],
            meta_opt=policy.meta_opt)

    return policy.loss_obj.loss


def maml_stats(policy, train_batch):
    if policy.config["worker_index"]:
        return {"worker_loss": policy.loss_obj.loss}
    else:
        return {
            "cur_kl_coeff": policy.kl_coeff_val,
            "cur_lr": policy.cur_lr,
            "total_loss": policy.loss_obj.loss,
            "policy_loss": policy.loss_obj.mean_policy_loss,
            "vf_loss": policy.loss_obj.mean_vf_loss,
            "kl_loss": policy.loss_obj.mean_kl_loss,
            "inner_kl": policy.loss_obj.mean_inner_kl,
            "entropy": policy.loss_obj.mean_entropy,
        }


class KLCoeffMixin:
    def __init__(self, config):
        self.kl_coeff_val = [
            config["kl_coeff"]
        ] * config["inner_adaptation_steps"] * config["num_workers"]
        self.kl_target = self.config["kl_target"]

    def update_kls(self, sampled_kls):
        for i, kl in enumerate(sampled_kls):
            if kl < self.kl_target / 1.5:
                self.kl_coeff_val[i] *= 0.5
            elif kl > 1.5 * self.kl_target:
                self.kl_coeff_val[i] *= 2.0
        return self.kl_coeff_val


def maml_optimizer_fn(policy, config):
    """
    Workers use simple SGD for inner adaptation
    Meta-Policy uses Adam optimizer for meta-update
    """
    if not config["worker_index"]:
        policy.meta_opt = torch.optim.Adam(
            policy.model.parameters(), lr=config["lr"])
        return policy.meta_opt
    return torch.optim.SGD(policy.model.parameters(), lr=config["inner_lr"])


def setup_mixins(policy, obs_space, action_space, config):
    ValueNetworkMixin.__init__(policy, obs_space, action_space, config)
    KLCoeffMixin.__init__(policy, config)


MAMLTorchPolicy = build_policy_class(
    name="MAMLTorchPolicy",
    framework="torch",
    get_default_config=lambda: ray.rllib.agents.maml.maml.DEFAULT_CONFIG,
    loss_fn=maml_loss,
    stats_fn=maml_stats,
    optimizer_fn=maml_optimizer_fn,
    extra_action_out_fn=vf_preds_fetches,
    postprocess_fn=compute_gae_for_sample_batch,
    extra_grad_process_fn=apply_grad_clipping,
    before_init=setup_config,
    after_init=setup_mixins,
    mixins=[KLCoeffMixin])
