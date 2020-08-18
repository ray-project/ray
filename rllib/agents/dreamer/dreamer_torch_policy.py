import logging

import ray
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_template import build_torch_policy
from ray.rllib.agents.a3c.a3c_torch_policy import apply_grad_clipping
from ray.rllib.utils.framework import get_activation_fn
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.agents.dreamer.utils import FreezeParameters
torch, nn = try_import_torch()
if torch:
  from torch import distributions as td

logger = logging.getLogger(__name__)


# This is the computation graph for workers (inner adaptation steps)
class DreamerLoss(object):
    def __init__(self,
        obs,
        action,
        reward,
        model,
        imagine_horizon,
        discount=0.99,
        lambda_=0.95,
        kl_coeff=1.0,
        free_nats=3.0,
        log=False,
        optimizers=None):

        model_optimizer = optimizers[0]
        actor_optimizer = optimizers[1]
        critic_optimizer = optimizers[2]
        self.model = model

        encoder_weights = list(model.encoder.parameters())
        decoder_weights = list(model.decoder.parameters())
        reward_weights = list(model.reward.parameters())
        dynamics_weights = list(model.dynamics.parameters())
        actor_weights = list(model.actor.parameters())
        critic_weights = list(model.value.parameters())

        model_modules = encoder_weights + decoder_weights + reward_weights + dynamics_weights
        self.horizon = imagine_horizon
        self.lambda_ = lambda_
        self.kl_coeff = kl_coeff
        self.device = (torch.device("cuda")
              if torch.cuda.is_available() else torch.device("cpu"))
        self.free_nats = free_nats

        # PlaNET Model Loss
        #a(torch.mean(obs))
        #print(torch.mean(action))
        latent = model.encoder(obs)
        post, prior = model.dynamics.observe(latent, action)
        features = model.dynamics.get_feature(post)
        image_pred = model.decoder(features)
        reward_pred = model.reward(features)
        self.image_loss = -torch.mean(image_pred.log_prob(obs)) 
        self.reward_loss = -torch.mean(reward_pred.log_prob(reward))

        prior_dist = model.dynamics.get_dist(prior[0], prior[1]) 
        post_dist = model.dynamics.get_dist(post[0], post[1])
        
        div = torch.mean(torch.distributions.kl_divergence(post_dist, prior_dist).sum(dim=2))
        print(div)
        self.div = torch.clamp(div, min=free_nats)
        self.model_loss = kl_coeff * self.div + self.reward_loss + self.image_loss
        
        # Actor Loss
        # [imagine_horizon, batch_length*batch_size, feature_size]
        with torch.no_grad():
            actor_states = [v.detach() for v in post]
        with FreezeParameters(model_modules):
            imag_feat = model.imagine_ahead(actor_states, imagine_horizon)
        with FreezeParameters(model_modules + critic_weights):
            reward = model.reward(imag_feat).mean
            value = model.value(imag_feat).mean
        pcont =  discount*torch.ones_like(reward)
        returns = self.lambda_return(reward[:-1], value[:-1], pcont[:-1], value[-1], lambda_)
        discount_shape = pcont[:1].size()
        discount = torch.cumprod(torch.cat([torch.ones(*discount_shape).to(self.device), pcont[:-2]], dim=0),dim=0)
        self.actor_loss = -torch.mean(discount * returns)

        # Critic Loss
        with torch.no_grad():
            val_feat = imag_feat.detach()[:-1]
            target = returns.detach()
            val_discount = discount.detach()
        val_pred = model.value(val_feat)
        self.critic_loss = -torch.mean(val_discount * val_pred.log_prob(target))

        model_optimizer.zero_grad()
        actor_optimizer.zero_grad()
        critic_optimizer.zero_grad()

        self.model_loss.backward()
        self.actor_loss.backward()
        self.critic_loss.backward()

        nn.utils.clip_grad_norm_(model_modules, 100.0, norm_type=2)
        nn.utils.clip_grad_norm_(actor_weights, 100.0, norm_type=2)
        nn.utils.clip_grad_norm_(critic_weights, 100.0, norm_type=2)

        model_optimizer.step()
        actor_optimizer.step()
        critic_optimizer.step()

        # Logging purposes
        self.prior_ent = torch.mean(prior_dist.entropy()) 
        self.post_ent = torch.mean(post_dist.entropy())
        if log:
            self.log_summary(obs, action, latent, image_pred)


    # Similar to GAE-Lambda
    def lambda_return(self, reward, value, pcont, bootstrap, lambda_):
        agg_fn = lambda x,y: y[0] + y[1] * lambda_ * x
        next_values = torch.cat([value[1:], bootstrap[None]],dim=0)
        inputs = reward + pcont * next_values * (1 - lambda_)

        last = bootstrap
        returns = []
        for i in reversed(range(len(inputs))):
            last = agg_fn(last, [inputs[i], pcont[i]])
            returns.append(last)

        returns = list(reversed(returns))
        returns = torch.stack(returns, dim=0)
        return returns

    def log_summary(self, obs, action, embed, image_pred):
        truth = obs[:6] + 0.5
        recon = image_pred.mean[:6]
        init, _ = self.model.dynamics.observe(embed[:6, :5], action[:6, :5])
        init = [itm[:,-1] for itm in init]
        prior = self.model.dynamics.imagine(action[:6, 5:], init)
        openl = self.model.decoder(self.model.dynamics.get_feature(prior)).mean

        mod = torch.cat([recon[:,:5] + 0.5, openl+0.5], 1)
        error = (mod - truth + 1.0) / 2.0
        self.gif = torch.cat([truth, mod, error], 3)

       
def dreamer_loss(policy, model, dist_class, train_batch):
    policy.cur_lr = policy.config["lr"]
    policy.log_gif = False
    if "log" in train_batch:
        policy.log_gif = True
    policy.loss_obj = DreamerLoss(train_batch['obs'],
                                  train_batch['actions'],
                                  train_batch['rewards'],
                                  policy.model,
                                  policy.config["imagine_horizon"],
                                  policy.config["discount"],
                                  policy.config["lambda"],
                                  policy.config["kl_coeff"],
                                  policy.config["free_nats"],
                                  policy.log_gif,
                                  policy._optimizers,
                                  )

    return tuple([policy.loss_obj.model_loss, policy.loss_obj.actor_loss, policy.loss_obj.critic_loss])

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
    obs = input_dict["obs"]

    # Exploration here
    if timestep <= policy.config["prefill_timesteps"]:
        logp = [0.0]
        # Random action in space [-1.0, 1.0]
        action = 2.0 * torch.rand(1, model.action_space.shape[0]) - 1.0
        state = model.get_initial_state()
    else:
        # Weird RLLib Handling, this happens when env rests
        if len(state[0].size())==3:
            state = model.get_initial_state()
        action, logp, state = model.policy(obs, state, explore)
        # Add random gaussian (for better training dataset)
        action = td.Normal(action, policy.config["explore_noise"]).sample()
        action = torch.clamp(action, min=-1.0, max=1.0)

    policy.global_timestep += policy.config["action_repeat"]

    return action, logp, state

def dreamer_stats(policy, train_batch):
    stats_dict = {"model_loss": policy.loss_obj.model_loss,
            "reward_loss": policy.loss_obj.reward_loss,
            "image_loss": policy.loss_obj.image_loss,
            "divergence": policy.loss_obj.div,
            "actor_loss": policy.loss_obj.actor_loss,
            "value_loss": policy.loss_obj.critic_loss,
            "prior_ent": policy.loss_obj.prior_ent,
            "post_ent": policy.loss_obj.post_ent,
            }

    if "log" in train_batch:
        stats_dict["log"] = policy.loss_obj.gif

    return stats_dict

def dreamer_optimizer_fn(policy, config):
    model = policy.model
    encoder_weights = list(model.encoder.parameters())
    decoder_weights = list(model.decoder.parameters())
    reward_weights = list(model.reward.parameters())
    dynamics_weights = list(model.dynamics.parameters())
    actor_weights = list(model.actor.parameters())
    critic_weights = list(model.value.parameters())
    model_opt = torch.optim.Adam(encoder_weights + decoder_weights + reward_weights + dynamics_weights, lr=config["model_lr"])
    actor_opt = torch.optim.Adam(actor_weights, lr=config["actor_lr"])
    critic_opt = torch.optim.Adam(critic_weights, lr=config["critic_lr"])

    return tuple([model_opt, actor_opt, critic_opt])

DreamerTorchPolicy = build_torch_policy(
    name="DreamerTorchPolicy",
    get_default_config=lambda: ray.rllib.agents.dreamer.dreamer.DEFAULT_CONFIG,
    action_sampler_fn=action_sampler_fn,
    loss_fn=dreamer_loss,
    stats_fn=dreamer_stats,
    make_model=build_dreamer_model,
    optimizer_fn=dreamer_optimizer_fn,
    extra_grad_process_fn=apply_grad_clipping)
