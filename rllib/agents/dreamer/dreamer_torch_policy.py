import logging

import ray
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_template import build_torch_policy
from ray.rllib.agents.a3c.a3c_torch_policy import apply_grad_clipping
from ray.rllib.utils.framework import get_activation_fn
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.models.catalog import ModelCatalog

torch, nn = try_import_torch()

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
        free_nats=3.0):

        torch.autograd.set_detect_anomaly(True)
        self.horizon = imagine_horizon
        self.lambda_ = lambda_
        self.kl_coeff = kl_coeff
        self.free_nats = free_nats
        self.device = (torch.device("cuda")
              if torch.cuda.is_available() else torch.device("cpu"))

        # PlaNET Model Loss
        latent = model.encoder(obs)
        #print(latent.requires_grad)
        #print(torch.mean(latent))
        post, prior = model.dynamics.observe(latent, action)
        features = model.dynamics.get_feature(post)
        image_pred = model.decoder(features)
        reward_pred = model.reward(features)

        self.image_loss = -torch.mean(image_pred.log_prob(obs)) 
        self.reward_loss = -torch.mean(reward_pred.log_prob(reward))

        prior_dist = model.dynamics.get_dist(prior[0], prior[1]) 
        post_dist = model.dynamics.get_dist(post[0], post[1])
        self.div = torch.mean(torch.distributions.kl_divergence(post_dist, prior_dist))
        #print(torch.mean(post[0]), torch.mean(post[1]))
        #print(torch.mean(prior[0]), torch.mean(prior[1]))
        #print(self.div)
        #self.div = torch.clamp(self.div, min=free_nats)
        self.model_loss = kl_coeff * self.div + self.reward_loss + self.image_loss
        self.loss = self.model_loss

        
        # Actor Loss
        # [imagine_horizon, batch_length*batch_size, feature_size]
        imag_feat = model.imagine_ahead(post, imagine_horizon)
        reward = model.reward(imag_feat).mean
        value = model.value(imag_feat).mean

        pcont =  discount*torch.ones(*reward.size()).to(self.device)
        returns = self.lambda_return(reward[:-1], value[:-1], pcont[:-1], value[-1], lambda_)
        discount_shape = pcont[:1].size()
        discount = torch.cumprod(torch.cat([torch.ones(*discount_shape).to(self.device), pcont[:-2]], dim=0),dim=0).detach()

        self.actor_loss = -torch.mean(discount * returns)

        # Critic Loss
        value_pred = model.value(imag_feat[:-1])
        target = returns.detach()
        self.critic_loss = -torch.mean(discount * value_pred.log_prob(target))

        # Logging purposes
        self.prior_ent = torch.mean(prior_dist.entropy()) 
        self.post_ent = torch.mean(post_dist.entropy())

    # Similar to GAE
    def lambda_return(self, reward, value, pcont, bootstrap, lambda_):
        agg_fn = lambda x,y: y[0] + y[1] * lambda_ * x
        next_values = torch.cat([value[1:], bootstrap[None]],dim=0)
        inputs = reward + pcont * next_values * (1 - lambda_)

        last = bootstrap
        returns = [[]]
        for i in reversed(range(self.horizon-1)):
            last = agg_fn(last, [inputs[i], pcont[i]])
            [o.append(l) for o,l in zip(returns, [last])]

        returns = [list(reversed(x)) for x in returns]
        returns = [torch.stack(x, dim=0) for x in returns]
        return returns[0]

       
def dreamer_loss(policy, model, dist_class, train_batch):
    policy.cur_lr = policy.config["lr"]
    policy.loss_obj = DreamerLoss(train_batch['obs'],
                                  train_batch['actions'],
                                  train_batch['rewards'],
                                  policy.model,
                                  policy.config["imagine_horizon"],
                                  policy.config["discount"],
                                  policy.config["lambda"],
                                  policy.config["kl_coeff"],
                                  policy.config["free_nats"])

    return tuple([policy.loss_obj.actor_loss, policy.loss_obj.critic_loss, policy.loss_obj.model_loss])

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
    if timestep < policy.config["prefill_timesteps"]:
        logp = [0.0]
        # Random action in space [-1.0, 1.0]
        action = 2.0*torch.rand(1, model.action_space.shape[0])-1.0
        state = model.get_initial_state()
    else:
        action, logp, state = model.policy(obs, state, explore)

    policy.global_timestep += policy.config["action_repeat"]

    return action, logp, state

def dreamer_stats(policy, train_batch):
    return {"model_loss": policy.loss_obj.model_loss,
            "reward_loss": policy.loss_obj.reward_loss,
            "image_loss": policy.loss_obj.image_loss,
            "divergence": policy.loss_obj.div,
            "actor_loss": policy.loss_obj.actor_loss,
            "value_loss": policy.loss_obj.critic_loss,
            "prior_ent": policy.loss_obj.prior_ent,
            "post_ent": policy.loss_obj.post_ent,
            }

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
    return tuple([actor_opt, critic_opt, model_opt])

DreamerTorchPolicy = build_torch_policy(
    name="DreamerTorchPolicy",
    get_default_config=lambda: ray.rllib.agents.dreamer.dreamer.DEFAULT_CONFIG,
    action_sampler_fn=action_sampler_fn,
    loss_fn=dreamer_loss,
    stats_fn=dreamer_stats,
    make_model=build_dreamer_model,
    optimizer_fn=dreamer_optimizer_fn,
    extra_grad_process_fn=apply_grad_clipping)
