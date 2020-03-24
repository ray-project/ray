from gym.spaces import Box, Discrete
import logging
import numpy as np

import ray
import ray.experimental.tf_utils
from ray.rllib.agents.sac.sac_tf_policy import build_sac_model, \
    postprocess_trajectory
from ray.rllib.agents.sac.sac_model import SACModel
from ray.rllib.agents.dqn.dqn_policy import postprocess_nstep_and_prio, \
    PRIO_WEIGHTS
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy import TorchPolicy
from ray.rllib.policy.torch_policy_template import build_torch_policy
from ray.rllib.models import ModelCatalog
from ray.rllib.models.torch.torch_action_dist import (TorchCategorical,
    TorchSquashedGaussian, TorchDiagGaussian)
from ray.rllib.utils import try_import_torch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.tf_ops import minimize_and_clip, make_tf_callable

torch, nn = try_import_torch()
F = None
if nn is not None:
    F = nn.functional

logger = logging.getLogger(__name__)


def get_dist_class(config, action_space):
    if isinstance(action_space, Discrete):
        action_dist_class = TorchCategorical
    else:
        action_dist_class = TorchSquashedGaussian if \
            config["normalize_actions"] else TorchDiagGaussian
    return action_dist_class


def get_log_likelihood(policy, model, actions, input_dict, obs_space,
                       action_space, config):
    model_out, _ = model({
        "obs": input_dict[SampleBatch.CUR_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)
    distribution_inputs = model.get_policy_output(model_out)
    action_dist_class = get_dist_class(policy.config, action_space)
    return action_dist_class(distribution_inputs, model).logp(actions)


def build_action_output(policy, model, input_dict, obs_space, action_space,
                        explore, config, timestep):
    model_out, _ = model({
        "obs": input_dict[SampleBatch.CUR_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)
    distribution_inputs = model.get_policy_output(model_out)
    action_dist_class = get_dist_class(policy.config, action_space)

    policy.output_actions, policy.sampled_action_logp = \
        policy.exploration.get_exploration_action(
            distribution_inputs, action_dist_class, model, timestep, explore)

    return policy.output_actions, policy.sampled_action_logp


def actor_critic_loss(policy, model, _, train_batch):
    model_out_t, _ = model({
        "obs": train_batch[SampleBatch.CUR_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)

    model_out_tp1, _ = model({
        "obs": train_batch[SampleBatch.NEXT_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)

    target_model_out_tp1, _ = policy.target_model({
        "obs": train_batch[SampleBatch.NEXT_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)

    # Discrete case.
    if model.discrete:
        # Get all action probs directly from pi and form their logp.
        log_pis_t = F.log_softmax(model.get_policy_output(model_out_t), dim=-1)
        policy_t = torch.exp(log_pis_t)
        log_pis_tp1 = F.log_softmax(
            model.get_policy_output(model_out_tp1), -1)
        policy_tp1 = torch.exp(log_pis_tp1)
        # Q-values.
        q_t = model.get_q_values(model_out_t)
        # Target Q-values.
        q_tp1 = policy.target_model.get_q_values(target_model_out_tp1)
        if policy.config["twin_q"]:
            twin_q_t = model.get_twin_q_values(model_out_t)
            twin_q_tp1 = policy.target_model.get_twin_q_values(
                target_model_out_tp1)
            q_tp1 = torch.min((q_tp1, twin_q_tp1), axis=0)
        q_tp1 -= model.alpha * log_pis_tp1

        # Actually selected Q-values (from the actions batch).
        one_hot = torch.one_hot(
            train_batch[SampleBatch.ACTIONS], depth=q_t.shape.as_list()[-1])
        q_t_selected = torch.sum(q_t * one_hot, dim=-1)
        if policy.config["twin_q"]:
            twin_q_t_selected = torch.sum(twin_q_t * one_hot, dim=-1)
        # Discrete case: "Best" means weighted by the policy (prob) outputs.
        q_tp1_best = torch.sum(torch.mul(policy_tp1, q_tp1), dim=-1)
        q_tp1_best_masked = \
            (1.0 - train_batch[SampleBatch.DONES].float()) * \
            q_tp1_best
    # Continuous actions case.
    else:
        # Sample simgle actions from distribution.
        action_dist_class = get_dist_class(policy.config, policy.action_space)
        action_dist_t = action_dist_class(
            model.get_policy_output(model_out_t), policy.model)
        policy_t = action_dist_t.sample()
        log_pis_t = torch.unsqueeze(action_dist_t.sampled_action_logp(), -1)
        action_dist_tp1 = action_dist_class(
            model.get_policy_output(model_out_tp1), policy.model)
        policy_tp1 = action_dist_tp1.sample()
        log_pis_tp1 = torch.unsqueeze(action_dist_tp1.sampled_action_logp(), -1)

        # Q-values for the actually selected actions.
        q_t = model.get_q_values(model_out_t, train_batch[SampleBatch.ACTIONS])
        if policy.config["twin_q"]:
            twin_q_t = model.get_twin_q_values(
                model_out_t, train_batch[SampleBatch.ACTIONS])

        # Q-values for current policy in given current state.
        q_t_det_policy = model.get_q_values(model_out_t, policy_t)
        if policy.config["twin_q"]:
            twin_q_t_det_policy = model.get_twin_q_values(
                model_out_t, policy_t)
            q_t_det_policy = torch.min(
                (q_t_det_policy, twin_q_t_det_policy), dim=0)

        # target q network evaluation
        q_tp1 = policy.target_model.get_q_values(target_model_out_tp1,
                                                 policy_tp1)
        if policy.config["twin_q"]:
            twin_q_tp1 = policy.target_model.get_twin_q_values(
                target_model_out_tp1, policy_tp1)

        q_t_selected = torch.squeeze(q_t, axis=len(q_t.shape) - 1)
        if policy.config["twin_q"]:
            twin_q_t_selected = torch.squeeze(
                twin_q_t, axis=len(q_t.shape) - 1)
            q_tp1 = torch.min((q_tp1, twin_q_tp1), dim=0)
        q_tp1 -= model.alpha * log_pis_tp1

        q_tp1_best = torch.squeeze(input=q_tp1, axis=len(q_tp1.shape) - 1)
        q_tp1_best_masked = (1.0 - train_batch[SampleBatch.DONES].float()) * \
            q_tp1_best

    assert policy.config["n_step"] == 1, "TODO(hartikainen) n_step > 1"

    # compute RHS of bellman equation
    q_t_selected_target = (train_batch[SampleBatch.REWARDS] +
        policy.config["gamma"]**policy.config["n_step"] * q_tp1_best_masked).detach()

    # Compute the TD-error (potentially clipped).
    base_td_error = torch.abs(q_t_selected - q_t_selected_target)
    if policy.config["twin_q"]:
        twin_td_error = torch.abs(twin_q_t_selected - q_t_selected_target)
        td_error = 0.5 * (base_td_error + twin_td_error)
    else:
        td_error = base_td_error

    critic_loss = [
        0.5 * torch.mean(torch.pow(q_t_selected_target - q_t_selected, 2.0))
    ]
    if policy.config["twin_q"]:
        critic_loss.append(0.5 * torch.mean(torch.pow(
            q_t_selected_target - twin_q_t_selected, 2.0)))

    # Auto-calculate the target entropy.
    if policy.config["target_entropy"] == "auto":
        if model.discrete:
            target_entropy = np.array(-policy.action_space.n, dtype=np.float32)
        else:
            target_entropy = -np.prod(policy.action_space.shape)
    else:
        target_entropy = policy.config["target_entropy"]

    # Alpha- and actor losses.
    # Note: In the papers, alpha is used directly, here we take the log.
    # Discrete case: Multiply the action probs as weights with the original
    # loss terms (no expectations needed).
    if model.discrete:
        alpha_loss = torch.mean(torch.sum(torch.mul(
            policy_t.detach(),
            -model.log_alpha * (log_pis_t + target_entropy).detach()),
            dim=-1))
        actor_loss = torch.mean(torch.sum(torch.mul(
            # NOTE: No stop_grad around policy output here
            # (compare with q_t_det_policy for continuous case).
            policy_t,
            model.alpha * log_pis_t - q_t.detach()),
            dim=-1))
    else:
        alpha_loss = -torch.mean(
            model.log_alpha * (log_pis_t + target_entropy).detach())
        actor_loss = torch.mean(model.alpha * log_pis_t - q_t_det_policy)

    # save for stats function
    policy.q_t = q_t
    policy.td_error = td_error
    policy.actor_loss = actor_loss
    policy.critic_loss = critic_loss
    policy.alpha_loss = alpha_loss
    policy.alpha_value = model.alpha
    policy.target_entropy = target_entropy

    # In a custom apply op we handle the losses separately, but return them
    # combined in one loss for now.
    combined_critic_loss = critic_loss[0] + \
        (critic_loss[1] if len(critic_loss) > 1 else 0.0)
    return actor_loss + combined_critic_loss + alpha_loss


def gradients(policy, optimizer, loss):
    if policy.config["grad_norm_clipping"]:
        actor_grads_and_vars = minimize_and_clip(
            optimizer,
            policy.actor_loss,
            var_list=policy.model.policy_variables(),
            clip_val=policy.config["grad_norm_clipping"])
        if policy.config["twin_q"]:
            q_variables = policy.model.q_variables()
            half_cutoff = len(q_variables) // 2
            critic_grads_and_vars = []
            critic_grads_and_vars += minimize_and_clip(
                optimizer,
                policy.critic_loss[0],
                var_list=q_variables[:half_cutoff],
                clip_val=policy.config["grad_norm_clipping"])
            critic_grads_and_vars += minimize_and_clip(
                optimizer,
                policy.critic_loss[1],
                var_list=q_variables[half_cutoff:],
                clip_val=policy.config["grad_norm_clipping"])
        else:
            critic_grads_and_vars = minimize_and_clip(
                optimizer,
                policy.critic_loss[0],
                var_list=policy.model.q_variables(),
                clip_val=policy.config["grad_norm_clipping"])
        alpha_grads_and_vars = minimize_and_clip(
            optimizer,
            policy.alpha_loss,
            var_list=[policy.model.log_alpha],
            clip_val=policy.config["grad_norm_clipping"])
    else:
        actor_grads_and_vars = policy._actor_optimizer.compute_gradients(
            policy.actor_loss, var_list=policy.model.policy_variables())
        if policy.config["twin_q"]:
            q_variables = policy.model.q_variables()
            half_cutoff = len(q_variables) // 2
            base_q_optimizer, twin_q_optimizer = policy._critic_optimizer
            critic_grads_and_vars = base_q_optimizer.compute_gradients(
                policy.critic_loss[0], var_list=q_variables[:half_cutoff]
            ) + twin_q_optimizer.compute_gradients(
                policy.critic_loss[1], var_list=q_variables[half_cutoff:])
        else:
            critic_grads_and_vars = policy._critic_optimizer[
                0].compute_gradients(
                    policy.critic_loss[0], var_list=policy.model.q_variables())
        alpha_grads_and_vars = policy._alpha_optimizer.compute_gradients(
            policy.alpha_loss, var_list=[policy.model.log_alpha])

    # save these for later use in build_apply_op
    policy._actor_grads_and_vars = [(g, v) for (g, v) in actor_grads_and_vars
                                    if g is not None]
    policy._critic_grads_and_vars = [(g, v) for (g, v) in critic_grads_and_vars
                                     if g is not None]
    policy._alpha_grads_and_vars = [(g, v) for (g, v) in alpha_grads_and_vars
                                    if g is not None]
    grads_and_vars = (
        policy._actor_grads_and_vars + policy._critic_grads_and_vars +
        policy._alpha_grads_and_vars)
    return grads_and_vars


def apply_gradients(policy, optimizer, grads_and_vars):
    policy._actor_optimizer.apply_gradients(policy._actor_grads_and_vars)
    cgrads = policy._critic_grads_and_vars
    half_cutoff = len(cgrads) // 2
    if policy.config["twin_q"]:
        policy._critic_optimizer[0].apply_gradients(cgrads[:half_cutoff]),
        policy._critic_optimizer[1].apply_gradients(cgrads[half_cutoff:])
    else:
        policy._critic_optimizer[0].apply_gradients(cgrads)

    policy._alpha_optimizer.apply_gradients(
        policy._alpha_grads_and_vars,
        global_step=policy.global_timestep)


def stats(policy, train_batch):
    return {
        "td_error": torch.mean(policy.td_error),
        "actor_loss": torch.mean(policy.actor_loss),
        "critic_loss": torch.mean(policy.critic_loss),
        "alpha_loss": torch.mean(policy.alpha_loss),
        "alpha_value": torch.mean(policy.alpha_value),
        "target_entropy": policy.target_entropy,
        "mean_q": torch.mean(policy.q_t),
        "max_q": torch.max(policy.q_t),
        "min_q": torch.min(policy.q_t),
    }


class ActorCriticOptimizerMixin:
    def __init__(self, config):
        # use separate optimizers for actor & critic
        self._actor_optimizer = torch.optim.Adam(
            lr=config["optimization"]["actor_learning_rate"])

        self._critic_optimizer = [torch.optim.Adam(
            lr=config["optimization"]["critic_learning_rate"])]
        if config["twin_q"]:
            self._critic_optimizer.append(torch.optim.Adam(
                lr=config["optimization"]["critic_learning_rate"]))
        self._alpha_optimizer = torch.optim.Adam(
            lr=config["optimization"]["entropy_learning_rate"])


class ComputeTDErrorMixin:
    def __init__(self):
        def compute_td_error(obs_t, act_t, rew_t, obs_tp1, done_mask,
                             importance_weights):
            input_dict = self._lazy_tensor_dict({SampleBatch.CUR_OBS: obs_t})
            input_dict[SampleBatch.ACTIONS] = act_t
            input_dict[SampleBatch.REWARDS] = rew_t
            input_dict[SampleBatch.NEXT_OBS] = obs_tp1
            input_dict[SampleBatch.DONES] = done_mask
            input_dict[PRIO_WEIGHTS] = importance_weights

            # Do forward pass on loss to update td errors attribute
            # (one TD-error value per item in batch to update PR weights).
            actor_critic_loss(self, self.model, None, input_dict)

            return self.td_error

        self.compute_td_error = compute_td_error


class TargetNetworkMixin:
    def __init__(self):
        # Hard initial update from Q-net(s) to target Q-net(s).
        self.update_target(tau=1.0)

    def update_target(self, tau=None):
        tau = tau or self.config.get("tau")
        # Update_target_fn will be called periodically to copy Q network to
        # target Q network, using (soft) tau-synching.
        # Full sync from Q-model to target Q-model.
        if tau == 1.0:
            self.target_model.load_state_dict(self.model.state_dict())
        # Partial (soft) sync using tau-synching.
        else:
            model_vars = self.model.trainable_variables()
            target_model_vars = self.target_model.trainable_variables()
            assert len(model_vars) == len(target_model_vars), \
                (model_vars, target_model_vars)
            for var, var_target in zip(model_vars, target_model_vars):
                var_target.data = tau * var.data + \
                    (1.0 - tau) * var_target.data
    

    @override(TorchPolicy)
    def variables(self):
        return self.model.variables() + self.target_model.variables()


def setup_early_mixins(policy, obs_space, action_space, config):
    ActorCriticOptimizerMixin.__init__(policy, config)


def setup_mid_mixins(policy, obs_space, action_space, config):
    ComputeTDErrorMixin.__init__(policy)


def setup_late_mixins(policy, obs_space, action_space, config):
    TargetNetworkMixin.__init__(policy)


SACTorchPolicy = build_torch_policy(
    name="SACTorchPolicy",
    get_default_config=lambda: ray.rllib.agents.sac.sac.DEFAULT_CONFIG,
    make_model=build_sac_model,
    postprocess_fn=postprocess_trajectory,
    action_sampler_fn=build_action_output,
    log_likelihood_fn=get_log_likelihood,
    loss_fn=actor_critic_loss,
    stats_fn=stats,
    gradients_fn=gradients,
    apply_gradients_fn=apply_gradients,
    extra_learn_fetches_fn=lambda policy: {"td_error": policy.td_error},
    mixins=[
        TargetNetworkMixin, ActorCriticOptimizerMixin, ComputeTDErrorMixin
    ],
    before_init=setup_early_mixins,
    before_loss_init=setup_mid_mixins,
    after_init=setup_late_mixins,
    obs_include_prev_action_reward=False)
