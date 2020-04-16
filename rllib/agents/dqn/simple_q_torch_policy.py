"""Basic example of a DQN policy without any optimizations."""

import logging

import ray
from ray.rllib.agents.dqn.simple_q_tf_policy import build_q_models, \
    get_distribution_inputs_and_class, compute_q_values
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.torch_policy_template import build_torch_policy
from ray.rllib.utils import try_import_torch
from ray.rllib.utils.torch_ops import huber_loss

torch, nn = try_import_torch()
F = None
if nn:
    F = nn.functional
logger = logging.getLogger(__name__)


class TargetNetworkMixin:
    def __init__(self, obs_space, action_space, config):
        def do_update():
            # Update_target_fn will be called periodically to copy Q network to
            # target Q network.
            assert len(self.q_func_vars) == len(self.target_q_func_vars), \
                (self.q_func_vars, self.target_q_func_vars)
            self.target_q_model.load_state_dict(self.q_model.state_dict())

        self.update_target = do_update


def build_q_model_and_distribution(policy, obs_space, action_space, config):
    return build_q_models(policy, obs_space, action_space, config), \
        TorchCategorical


def build_q_losses(policy, model, dist_class, train_batch):
    # q network evaluation
    q_t = compute_q_values(
        policy,
        policy.q_model,
        train_batch[SampleBatch.CUR_OBS],
        explore=False,
        is_training=True)

    # target q network evalution
    q_tp1 = compute_q_values(
        policy,
        policy.target_q_model,
        train_batch[SampleBatch.NEXT_OBS],
        explore=False,
        is_training=True)

    # q scores for actions which we know were selected in the given state.
    one_hot_selection = F.one_hot(train_batch[SampleBatch.ACTIONS],
                                  policy.action_space.n)
    q_t_selected = torch.sum(q_t * one_hot_selection, 1)

    # compute estimate of best possible value starting from state at t + 1
    dones = train_batch[SampleBatch.DONES].float()
    q_tp1_best_one_hot_selection = F.one_hot(
        torch.argmax(q_tp1, 1), policy.action_space.n)
    q_tp1_best = torch.sum(q_tp1 * q_tp1_best_one_hot_selection, 1)
    q_tp1_best_masked = (1.0 - dones) * q_tp1_best

    # compute RHS of bellman equation
    q_t_selected_target = (train_batch[SampleBatch.REWARDS] +
                           policy.config["gamma"] * q_tp1_best_masked)

    # Compute the error (Square/Huber).
    td_error = q_t_selected - q_t_selected_target.detach()
    loss = torch.mean(huber_loss(td_error))

    # save TD error as an attribute for outside access
    policy.td_error = td_error

    return loss


def extra_action_out_fn(policy, input_dict, state_batches, model, action_dist):
    """Adds q-values to action out dict."""
    return {"q_values": policy.q_values}


def setup_late_mixins(policy, obs_space, action_space, config):
    TargetNetworkMixin.__init__(policy, obs_space, action_space, config)


SimpleQTorchPolicy = build_torch_policy(
    name="SimpleQPolicy",
    loss_fn=build_q_losses,
    get_default_config=lambda: ray.rllib.agents.dqn.dqn.DEFAULT_CONFIG,
    extra_action_out_fn=extra_action_out_fn,
    after_init=setup_late_mixins,
    make_model_and_action_dist=build_q_model_and_distribution,
    mixins=[TargetNetworkMixin],
    action_distribution_fn=get_distribution_inputs_and_class,
    stats_fn=lambda policy, config: {"td_error": policy.td_error},
)
