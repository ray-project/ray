import logging

import ray
from ray.rllib.agents.dreamer.utils import FreezeParameters
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_ops import apply_grad_clipping

torch, nn = try_import_torch()
if torch:
    from torch import distributions as td

logger = logging.getLogger(__name__)


# This is the computation graph for workers (inner adaptation steps)
def compute_dreamer_loss(obs,
                         action,
                         reward,
                         model,
                         imagine_horizon,
                         discount=0.99,
                         lambda_=0.95,
                         kl_coeff=1.0,
                         free_nats=3.0,
                         log=False):
    """Constructs loss for the Dreamer objective

        Args:
            obs (TensorType): Observations (o_t)
            action (TensorType): Actions (a_(t-1))
            reward (TensorType): Rewards (r_(t-1))
            model (TorchModelV2): DreamerModel, encompassing all other models
            imagine_horizon (int): Imagine horizon for actor and critic loss
            discount (float): Discount
            lambda_ (float): Lambda, like in GAE
            kl_coeff (float): KL Coefficient for Divergence loss in model loss
            free_nats (float): Threshold for minimum divergence in model loss
            log (bool): If log, generate gifs
        """
    encoder_weights = list(model.encoder.parameters())
    decoder_weights = list(model.decoder.parameters())
    reward_weights = list(model.reward.parameters())
    dynamics_weights = list(model.dynamics.parameters())
    critic_weights = list(model.value.parameters())
    model_weights = list(encoder_weights + decoder_weights + reward_weights +
                         dynamics_weights)

    device = (torch.device("cuda")
              if torch.cuda.is_available() else torch.device("cpu"))

    # PlaNET Model Loss
    latent = model.encoder(obs)
    post, prior = model.dynamics.observe(latent, action)
    features = model.dynamics.get_feature(post)
    image_pred = model.decoder(features)
    reward_pred = model.reward(features)
    image_loss = -torch.mean(image_pred.log_prob(obs))
    reward_loss = -torch.mean(reward_pred.log_prob(reward))
    prior_dist = model.dynamics.get_dist(prior[0], prior[1])
    post_dist = model.dynamics.get_dist(post[0], post[1])
    div = torch.mean(
        torch.distributions.kl_divergence(post_dist, prior_dist).sum(dim=2))
    div = torch.clamp(div, min=free_nats)
    model_loss = kl_coeff * div + reward_loss + image_loss

    # Actor Loss
    # [imagine_horizon, batch_length*batch_size, feature_size]
    with torch.no_grad():
        actor_states = [v.detach() for v in post]
    with FreezeParameters(model_weights):
        imag_feat = model.imagine_ahead(actor_states, imagine_horizon)
    with FreezeParameters(model_weights + critic_weights):
        reward = model.reward(imag_feat).mean
        value = model.value(imag_feat).mean
    pcont = discount * torch.ones_like(reward)
    returns = lambda_return(reward[:-1], value[:-1], pcont[:-1], value[-1],
                            lambda_)
    discount_shape = pcont[:1].size()
    discount = torch.cumprod(
        torch.cat([torch.ones(*discount_shape).to(device), pcont[:-2]], dim=0),
        dim=0)
    actor_loss = -torch.mean(discount * returns)

    # Critic Loss
    with torch.no_grad():
        val_feat = imag_feat.detach()[:-1]
        target = returns.detach()
        val_discount = discount.detach()
    val_pred = model.value(val_feat)
    critic_loss = -torch.mean(val_discount * val_pred.log_prob(target))

    # Logging purposes
    prior_ent = torch.mean(prior_dist.entropy())
    post_ent = torch.mean(post_dist.entropy())

    log_gif = None
    if log:
        log_gif = log_summary(obs, action, latent, image_pred, model)

    return_dict = {
        "model_loss": model_loss,
        "reward_loss": reward_loss,
        "image_loss": image_loss,
        "divergence": div,
        "actor_loss": actor_loss,
        "critic_loss": critic_loss,
        "prior_ent": prior_ent,
        "post_ent": post_ent,
    }

    if log_gif is not None:
        return_dict["log_gif"] = log_gif
    return return_dict


# Similar to GAE-Lambda, calculate value targets
def lambda_return(reward, value, pcont, bootstrap, lambda_):
    def agg_fn(x, y):
        return y[0] + y[1] * lambda_ * x

    next_values = torch.cat([value[1:], bootstrap[None]], dim=0)
    inputs = reward + pcont * next_values * (1 - lambda_)

    last = bootstrap
    returns = []
    for i in reversed(range(len(inputs))):
        last = agg_fn(last, [inputs[i], pcont[i]])
        returns.append(last)

    returns = list(reversed(returns))
    returns = torch.stack(returns, dim=0)
    return returns


# Creates gif
def log_summary(obs, action, embed, image_pred, model):
    truth = obs[:6] + 0.5
    recon = image_pred.mean[:6]
    init, _ = model.dynamics.observe(embed[:6, :5], action[:6, :5])
    init = [itm[:, -1] for itm in init]
    prior = model.dynamics.imagine(action[:6, 5:], init)
    openl = model.decoder(model.dynamics.get_feature(prior)).mean

    mod = torch.cat([recon[:, :5] + 0.5, openl + 0.5], 1)
    error = (mod - truth + 1.0) / 2.0
    return torch.cat([truth, mod, error], 3)


def dreamer_loss(policy, model, dist_class, train_batch):
    log_gif = False
    if "log_gif" in train_batch:
        log_gif = True

    policy.stats_dict = compute_dreamer_loss(
        train_batch["obs"],
        train_batch["actions"],
        train_batch["rewards"],
        policy.model,
        policy.config["imagine_horizon"],
        policy.config["discount"],
        policy.config["lambda"],
        policy.config["kl_coeff"],
        policy.config["free_nats"],
        log_gif,
    )

    loss_dict = policy.stats_dict

    return (loss_dict["model_loss"], loss_dict["actor_loss"],
            loss_dict["critic_loss"])


def build_dreamer_model(policy, obs_space, action_space, config):

    policy.model = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        1,
        config["dreamer_model"],
        name="DreamerModel",
        framework="torch")

    policy.model_variables = policy.model.variables()

    return policy.model


def action_sampler_fn(policy, model, input_dict, state, explore, timestep):
    """Action sampler function has two phases. During the prefill phase,
    actions are sampled uniformly [-1, 1]. During training phase, actions
    are evaluated through DreamerPolicy and an additive gaussian is added
    to incentivize exploration.
    """
    obs = input_dict["obs"]

    # Custom Exploration
    if timestep <= policy.config["prefill_timesteps"]:
        logp = [0.0]
        # Random action in space [-1.0, 1.0]
        action = 2.0 * torch.rand(1, model.action_space.shape[0]) - 1.0
        state = model.get_initial_state()
    else:
        # Weird RLLib Handling, this happens when env rests
        if len(state[0].size()) == 3:
            # Very hacky, but works on all envs
            state = model.get_initial_state()
        action, logp, state = model.policy(obs, state, explore)
        action = td.Normal(action, policy.config["explore_noise"]).sample()
        action = torch.clamp(action, min=-1.0, max=1.0)

    policy.global_timestep += policy.config["action_repeat"]

    return action, logp, state


def dreamer_stats(policy, train_batch):
    return policy.stats_dict


def dreamer_optimizer_fn(policy, config):
    model = policy.model
    encoder_weights = list(model.encoder.parameters())
    decoder_weights = list(model.decoder.parameters())
    reward_weights = list(model.reward.parameters())
    dynamics_weights = list(model.dynamics.parameters())
    actor_weights = list(model.actor.parameters())
    critic_weights = list(model.value.parameters())
    model_opt = torch.optim.Adam(
        encoder_weights + decoder_weights + reward_weights + dynamics_weights,
        lr=config["td_model_lr"])
    actor_opt = torch.optim.Adam(actor_weights, lr=config["actor_lr"])
    critic_opt = torch.optim.Adam(critic_weights, lr=config["critic_lr"])

    return (model_opt, actor_opt, critic_opt)


DreamerTorchPolicy = build_policy_class(
    name="DreamerTorchPolicy",
    framework="torch",
    get_default_config=lambda: ray.rllib.agents.dreamer.dreamer.DEFAULT_CONFIG,
    action_sampler_fn=action_sampler_fn,
    loss_fn=dreamer_loss,
    stats_fn=dreamer_stats,
    make_model=build_dreamer_model,
    optimizer_fn=dreamer_optimizer_fn,
    extra_grad_process_fn=apply_grad_clipping)
