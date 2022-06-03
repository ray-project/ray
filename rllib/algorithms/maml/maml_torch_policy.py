import logging
from typing import Dict, List, Type, Union

import ray
from ray.rllib.agents.ppo.ppo_tf_policy import validate_config
from ray.rllib.evaluation.postprocessing import (
    Postprocessing,
    compute_gae_for_sample_batch,
)
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_mixins import ValueNetworkMixin
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import apply_grad_clipping
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()
logger = logging.getLogger(__name__)

try:
    import higher
except (ImportError, ModuleNotFoundError):
    raise ImportError(
        (
            "The MAML and MB-MPO algorithms require the `higher` module to be "
            "installed! However, there was no installation found. You can install it "
            "via `pip install higher`."
        )
    )


def PPOLoss(
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
    clip_loss=False,
):
    def surrogate_loss(
        actions, curr_dist, prev_dist, advantages, clip_param, clip_loss
    ):
        pi_new_logp = curr_dist.logp(actions)
        pi_old_logp = prev_dist.logp(actions)

        logp_ratio = torch.exp(pi_new_logp - pi_old_logp)
        if clip_loss:
            return torch.min(
                advantages * logp_ratio,
                advantages * torch.clamp(logp_ratio, 1 - clip_param, 1 + clip_param),
            )
        return advantages * logp_ratio

    def kl_loss(curr_dist, prev_dist):
        return prev_dist.kl(curr_dist)

    def entropy_loss(dist):
        return dist.entropy()

    def vf_loss(value_fn, value_targets, vf_preds, vf_clip_param=0.1):
        # GAE Value Function Loss
        vf_loss1 = torch.pow(value_fn - value_targets, 2.0)
        vf_clipped = vf_preds + torch.clamp(
            value_fn - vf_preds, -vf_clip_param, vf_clip_param
        )
        vf_loss2 = torch.pow(vf_clipped - value_targets, 2.0)
        vf_loss = torch.max(vf_loss1, vf_loss2)
        return vf_loss

    pi_new_dist = dist_class(curr_logits, None)
    pi_old_dist = dist_class(behaviour_logits, None)

    surr_loss = torch.mean(
        surrogate_loss(
            actions, pi_new_dist, pi_old_dist, advantages, clip_param, clip_loss
        )
    )
    kl_loss = torch.mean(kl_loss(pi_new_dist, pi_old_dist))
    vf_loss = torch.mean(vf_loss(value_fn, value_targets, vf_preds, vf_clip_param))
    entropy_loss = torch.mean(entropy_loss(pi_new_dist))

    total_loss = -surr_loss + cur_kl_coeff * kl_loss
    total_loss += vf_loss_coeff * vf_loss
    total_loss -= entropy_coeff * entropy_loss
    return total_loss, surr_loss, kl_loss, vf_loss, entropy_loss


# This is the computation graph for workers (inner adaptation steps)
class WorkerLoss(object):
    def __init__(
        self,
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
        clip_loss=False,
    ):
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
            clip_loss=clip_loss,
        )


# This is the Meta-Update computation graph for main (meta-update step)
class MAMLLoss(object):
    def __init__(
        self,
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
        use_gae=True,
    ):
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
        self.behaviour_logits = self.split_placeholders(behaviour_logits, split)
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
            with higher.innerloop_ctx(model, inner_opt, copy_initial_weights=False) as (
                fnet,
                diffopt,
            ):
                inner_kls = []
                for step in range(self.inner_adaptation_steps):
                    ppo_loss, _, inner_kl_loss, _, _ = self.compute_losses(
                        fnet, step, i
                    )
                    diffopt.step(ppo_loss)
                    inner_kls.append(inner_kl_loss)
                    kls.append(inner_kl_loss.detach())

                # Meta Update
                ppo_loss, s_loss, kl_loss, v_loss, ent = self.compute_losses(
                    fnet, self.inner_adaptation_steps - 1, i, clip_loss=True
                )

                inner_loss = torch.mean(
                    torch.stack(
                        [
                            a * b
                            for a, b in zip(
                                self.cur_kl_coeff[
                                    i
                                    * self.inner_adaptation_steps : (i + 1)
                                    * self.inner_adaptation_steps
                                ],
                                inner_kls,
                            )
                        ]
                    )
                )
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

    def compute_losses(self, model, inner_adapt_iter, task_iter, clip_loss=False):
        obs = self.obs[inner_adapt_iter][task_iter]
        obs_dict = {"obs": obs, "obs_flat": obs}
        curr_logits, _ = model.forward(obs_dict, None, None)
        value_fns = model.value_function()
        ppo_loss, surr_loss, kl_loss, val_loss, ent_loss = PPOLoss(
            dist_class=self.dist_class,
            actions=self.actions[inner_adapt_iter][task_iter],
            curr_logits=curr_logits,
            behaviour_logits=self.behaviour_logits[inner_adapt_iter][task_iter],
            advantages=self.advantages[inner_adapt_iter][task_iter],
            value_fn=value_fns,
            value_targets=self.value_targets[inner_adapt_iter][task_iter],
            vf_preds=self.vf_preds[inner_adapt_iter][task_iter],
            cur_kl_coeff=0.0,
            entropy_coeff=self.entropy_coeff,
            clip_param=self.clip_param,
            vf_clip_param=self.vf_clip_param,
            vf_loss_coeff=self.vf_loss_coeff,
            clip_loss=clip_loss,
        )
        return ppo_loss, surr_loss, kl_loss, val_loss, ent_loss

    def split_placeholders(self, placeholder, split):
        inner_placeholder_list = torch.split(
            placeholder, torch.sum(split, dim=1).tolist(), dim=0
        )
        placeholder_list = []
        for index, split_placeholder in enumerate(inner_placeholder_list):
            placeholder_list.append(
                torch.split(split_placeholder, split[index].tolist(), dim=0)
            )
        return placeholder_list


class KLCoeffMixin:
    def __init__(self, config):
        self.kl_coeff_val = (
            [config["kl_coeff"]]
            * config["inner_adaptation_steps"]
            * config["num_workers"]
        )
        self.kl_target = self.config["kl_target"]

    def update_kls(self, sampled_kls):
        for i, kl in enumerate(sampled_kls):
            if kl < self.kl_target / 1.5:
                self.kl_coeff_val[i] *= 0.5
            elif kl > 1.5 * self.kl_target:
                self.kl_coeff_val[i] *= 2.0
        return self.kl_coeff_val


class MAMLTorchPolicy(ValueNetworkMixin, KLCoeffMixin, TorchPolicyV2):
    """PyTorch policy class used with MAMLTrainer."""

    def __init__(self, observation_space, action_space, config):
        config = dict(ray.rllib.algorithms.maml.maml.DEFAULT_CONFIG, **config)
        validate_config(config)

        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )

        KLCoeffMixin.__init__(self, config)
        ValueNetworkMixin.__init__(self, config)

        # TODO: Don't require users to call this manually.
        self._initialize_loss_from_dummy_batch()

    @override(TorchPolicyV2)
    def loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        """Constructs the loss function.

        Args:
            model: The Model to calculate the loss for.
            dist_class: The action distr. class.
            train_batch: The training data.

        Returns:
            The PPO loss tensor given the input batch.
        """
        logits, state = model(train_batch)
        self.cur_lr = self.config["lr"]

        if self.config["worker_index"]:
            self.loss_obj = WorkerLoss(
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
                entropy_coeff=self.config["entropy_coeff"],
                clip_param=self.config["clip_param"],
                vf_clip_param=self.config["vf_clip_param"],
                vf_loss_coeff=self.config["vf_loss_coeff"],
                clip_loss=False,
            )
        else:
            self.var_list = model.named_parameters()

            # `split` may not exist yet (during test-loss call), use a dummy value.
            # Cannot use get here due to train_batch being a TrackingDict.
            if "split" in train_batch:
                split = train_batch["split"]
            else:
                split_shape = (
                    self.config["inner_adaptation_steps"],
                    self.config["num_workers"],
                )
                split_const = int(
                    train_batch["obs"].shape[0] // (split_shape[0] * split_shape[1])
                )
                split = torch.ones(split_shape, dtype=int) * split_const
            self.loss_obj = MAMLLoss(
                model=model,
                dist_class=dist_class,
                value_targets=train_batch[Postprocessing.VALUE_TARGETS],
                advantages=train_batch[Postprocessing.ADVANTAGES],
                actions=train_batch[SampleBatch.ACTIONS],
                behaviour_logits=train_batch[SampleBatch.ACTION_DIST_INPUTS],
                vf_preds=train_batch[SampleBatch.VF_PREDS],
                cur_kl_coeff=self.kl_coeff_val,
                policy_vars=self.var_list,
                obs=train_batch[SampleBatch.CUR_OBS],
                num_tasks=self.config["num_workers"],
                split=split,
                config=self.config,
                inner_adaptation_steps=self.config["inner_adaptation_steps"],
                entropy_coeff=self.config["entropy_coeff"],
                clip_param=self.config["clip_param"],
                vf_clip_param=self.config["vf_clip_param"],
                vf_loss_coeff=self.config["vf_loss_coeff"],
                use_gae=self.config["use_gae"],
                meta_opt=self.meta_opt,
            )

        return self.loss_obj.loss

    @override(TorchPolicyV2)
    def optimizer(
        self,
    ) -> Union[List["torch.optim.Optimizer"], "torch.optim.Optimizer"]:
        """
        Workers use simple SGD for inner adaptation
        Meta-Policy uses Adam optimizer for meta-update
        """
        if not self.config["worker_index"]:
            self.meta_opt = torch.optim.Adam(
                self.model.parameters(), lr=self.config["lr"]
            )
            return self.meta_opt
        return torch.optim.SGD(self.model.parameters(), lr=self.config["inner_lr"])

    @override(TorchPolicyV2)
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        if self.config["worker_index"]:
            return convert_to_numpy({"worker_loss": self.loss_obj.loss})
        else:
            return convert_to_numpy(
                {
                    "cur_kl_coeff": self.kl_coeff_val,
                    "cur_lr": self.cur_lr,
                    "total_loss": self.loss_obj.loss,
                    "policy_loss": self.loss_obj.mean_policy_loss,
                    "vf_loss": self.loss_obj.mean_vf_loss,
                    "kl_loss": self.loss_obj.mean_kl_loss,
                    "inner_kl": self.loss_obj.mean_inner_kl,
                    "entropy": self.loss_obj.mean_entropy,
                }
            )

    @override(TorchPolicyV2)
    def extra_grad_process(
        self, optimizer: "torch.optim.Optimizer", loss: TensorType
    ) -> Dict[str, TensorType]:
        return apply_grad_clipping(self, optimizer, loss)

    @override(TorchPolicyV2)
    def postprocess_trajectory(
        self, sample_batch, other_agent_batches=None, episode=None
    ):
        # Do all post-processing always with no_grad().
        # Not using this here will introduce a memory leak
        # in torch (issue #6962).
        # TODO: no_grad still necessary?
        with torch.no_grad():
            return compute_gae_for_sample_batch(
                self, sample_batch, other_agent_batches, episode
            )
