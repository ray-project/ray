import ray
from ray.rllib.agents.marwil.marwil_tf_policy import postprocess_advantages
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_template import build_torch_policy
from ray.rllib.utils.explained_variance import explained_variance
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


class ValueNetworkMixin:
    def __init__(self):
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

        self._value = value


def marwil_loss(policy, model, dist_class, train_batch):
    model_out, _ = model.from_batch(train_batch)
    action_dist = dist_class(model_out, model)
    state_values = model.value_function()
    advantages = train_batch[Postprocessing.ADVANTAGES]
    actions = train_batch[SampleBatch.ACTIONS]

    # Value loss.
    policy.v_loss = 0.5 * torch.mean(torch.pow(state_values - advantages, 2.0))

    # Policy loss.
    # Advantage estimation.
    adv = advantages - state_values
    # Update averaged advantage norm.
    policy.ma_adv_norm.add_(
        1e-6 * (torch.mean(torch.pow(adv, 2.0)) - policy.ma_adv_norm))
    # #xponentially weighted advantages.
    exp_advs = torch.exp(policy.config["beta"] *
                         (adv / (1e-8 + torch.pow(policy.ma_adv_norm, 0.5))))
    # log\pi_\theta(a|s)
    logprobs = action_dist.logp(actions)
    policy.p_loss = -1.0 * torch.mean(exp_advs.detach() * logprobs)

    # Combine both losses.
    policy.total_loss = policy.p_loss + policy.config["vf_coeff"] * \
        policy.v_loss
    explained_var = explained_variance(
        advantages, state_values, framework="torch")
    policy.explained_variance = torch.mean(explained_var)

    return policy.total_loss


def stats(policy, train_batch):
    return {
        "policy_loss": policy.p_loss,
        "vf_loss": policy.v_loss,
        "total_loss": policy.total_loss,
        "vf_explained_var": policy.explained_variance,
    }


def setup_mixins(policy, obs_space, action_space, config):
    # Create a var.
    policy.ma_adv_norm = torch.tensor(
        [100.0], dtype=torch.float32, requires_grad=False)
    # Setup Value branch of our NN.
    ValueNetworkMixin.__init__(policy)


MARWILTorchPolicy = build_torch_policy(
    name="MARWILTorchPolicy",
    loss_fn=marwil_loss,
    get_default_config=lambda: ray.rllib.agents.marwil.marwil.DEFAULT_CONFIG,
    stats_fn=stats,
    postprocess_fn=postprocess_advantages,
    after_init=setup_mixins,
    mixins=[ValueNetworkMixin])
