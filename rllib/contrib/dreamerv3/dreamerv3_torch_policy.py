from pprint import pprint
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import torch
from torch import nn
import torch.distributions as td
import torch.nn.functional as F
from ray.rllib.evaluation.episode import Episode
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils import try_import_torch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.torch_utils import apply_grad_clipping
from ray.rllib.utils.typing import AgentID, TensorType

import dreamerv3
from dreamerv3.utils import FreezeParameters, symexp, symlog, batchify_states



EPS = 1e-8


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


class DreamerV3TorchPolicy(TorchPolicyV2):
    def __init__(self, observation_space, action_space, config):

        config = dict(dreamerv3.dreamerv3.DreamerV3Config().to_dict(), **config)

        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )

        # TODO: Don't require users to call this manually.
        self._initialize_loss_from_dummy_batch()

    @override(TorchPolicyV2)
    def loss(
            self, model: ModelV2, dist_class: ActionDistribution, train_batch: SampleBatch
    ) -> Union[TensorType, List[TensorType]]:
        log_gif = False
        if "log_gif" in train_batch:
            log_gif = True

        # This is the computation graph for workers (inner adaptation steps)
        encoder_weights = list(self.model.encoder.parameters())
        decoder_weights = list(self.model.decoder.parameters())
        reward_weights = list(self.model.reward.parameters())
        dynamics_weights = list(self.model.dynamics.parameters())
        critic_weights = list(self.model.value.parameters())
        model_weights = list(
            encoder_weights + decoder_weights + reward_weights + dynamics_weights
        )
        device = (
            torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
        )

        latent = self.model.encoder(train_batch["obs"])
        post, prior = self.model.dynamics.observe(latent, train_batch["actions"])
        features = self.model.dynamics.get_feature(post)

        image_pred = self.model.decoder(features)
        reward_pred = self.model.reward(features)
        continue_pred = self.model.continue_(features)

        obs_batch = train_batch["obs"].unsqueeze(1)
        rew_batch = train_batch["rewards"].unsqueeze(1)
        rew_batch = symlog(self.model.twohot.encode(rew_batch))
        reward_loss = -torch.tensordot(
            reward_pred, rew_batch,
            ([-1], [-1])
        ).mean()
        done_batch = train_batch["dones"].unsqueeze(1)

        # PlaNET Model Loss
        image_loss = -torch.mean(image_pred.log_prob(obs_batch))
        # reward_loss = -reward_loss # TODO is this correct?
        continue_loss = -F.binary_cross_entropy(continue_pred, done_batch.squeeze(1))
        pred_loss = image_loss + reward_loss + continue_loss

        pred_loss_info = dict(
            image_loss=image_loss,
            reward_loss=reward_loss,
            continue_loss=continue_loss
        )

        # TODO: do we have to forward twice? instead detach?
        with torch.no_grad():
            # sg_latent = self.model.encoder(train_batch["obs"]) # TODO: check
            sg_post, sg_prior = self.model.dynamics.observe(
                latent, train_batch["actions"]
            )

        prior_dist = self.model.dynamics.get_dist(prior[0], prior[1])
        sg_post_dist = self.model.dynamics.get_dist(sg_post[0], sg_post[1])
        dyn_div = torch.distributions.kl_divergence(sg_post_dist, prior_dist).sum(dim=2)
        dyn_div = torch.mean(dyn_div)

        dyn_loss = torch.max(torch.cuda.FloatTensor([1.]), dyn_div)  # TDOO: sg
        dyn_loss = torch.clamp(dyn_loss, min=(self.config["free_nats"]))

        sg_prior_dist = self.model.dynamics.get_dist(sg_prior[0], sg_prior[1])
        post_dist = self.model.dynamics.get_dist(post[0], post[1])
        rep_div = torch.distributions.kl_divergence(post_dist, sg_prior_dist).sum(dim=2)
        rep_div = torch.mean(rep_div)

        rep_loss = torch.max(torch.cuda.FloatTensor([1.]), rep_div)  # TDOO: sg
        rep_loss = torch.clamp(rep_loss, min=(self.config["free_nats"]))

        model_loss = (
                self.config['pred_beta'] * pred_loss
                + self.config['dyn_beta'] * dyn_loss
                + self.config['rep_beta'] * rep_loss
        )

        model_loss_info = dict(
            image_pred=image_pred,
            **pred_loss_info,
            dyn_div=dyn_div,
            rep_div=rep_div,
            prior_dist=prior_dist,
            post_dist=post_dist
        )

        # Actor Loss
        # [imagine_horizon, batch_length*batch_size, feature_size]
        with torch.no_grad():
            actor_states = [v.detach() for v in post]
        with FreezeParameters(model_weights):
            imag_feat = self.model.imagine_ahead(
                actor_states, self.config["imagine_horizon"]
            )
        with FreezeParameters(model_weights + critic_weights):
            reward = self.model.reward(imag_feat)
            reward = symexp(reward)
            reward = torch.tensordot(
                reward, self.model.twohot.bins,
                ([-1], [-1])
            )
            value = self.model.value(imag_feat)
            value = symexp(value)
            value = torch.tensordot(
                value, self.model.twohot.bins,
                ([-1], [-1])
            )

        pcont = self.config["gamma"] * torch.ones_like(reward)

        # Similar to GAE-Lambda, calculate value targets
        next_values = torch.cat([value[:-1][1:], value[-1][None]], dim=0)
        inputs = reward[:-1] + pcont[:-1] * next_values * (1 - self.config["lambda"])

        def agg_fn(x, y, c):
            return y[0] + y[1] * self.config["lambda"] * x * c

        last = value[-1]
        returns = []
        # TODO add continue pred
        for i in reversed(range(len(inputs))):
            last = agg_fn(last, [inputs[i], pcont[:-1][i]], continue_pred.round())
            returns.append(last)

        returns = list(reversed(returns))
        returns = torch.stack(returns, dim=0)
        discount_shape = pcont[:1].size()
        discount = torch.cumprod(
            torch.cat([torch.ones(*discount_shape).to(device), pcont[:-2]], dim=0),
            dim=0,
        )
        lambda_returns = (discount * returns)
        per_5 = torch.quantile(lambda_returns, 0.5, dim=-1, keepdim=True)
        per_95 = torch.quantile(lambda_returns, 0.95, dim=-1, keepdim=True)
        s = per_95 - per_5 + EPS
        scale = torch.max(torch.FloatTensor([1.]).to(device), s)
        scaled_returns = (lambda_returns / scale).mean()
        ent = self.model.actor.action_dist.entropy().mean()
        ent_reg = self.config['ent_coef'] * ent
        actor_loss = scaled_returns - ent_reg

        # Critic Loss
        with torch.no_grad():
            val_feat = imag_feat.detach()[:-1]
            target = returns.detach()
            val_discount = discount.detach()
            target = symlog(val_discount * target)
            target = self.model.twohot.encode(target.view(-1, 1))

        val_pred = self.model.value(val_feat)
        critic_loss = -torch.tensordot(
            target, val_pred,
            ([-1], [-1])
        ).mean()

        # Logging purposes
        prior_ent = torch.mean(model_loss_info.pop('prior_dist').entropy())
        post_ent = torch.mean(model_loss_info.pop('post_dist').entropy())
        gif = None
        if log_gif:
            gif = log_summary(
                train_batch["obs"],
                train_batch["actions"],
                latent,
                model_loss_info.pop("image_pred"),
                self.model,
            )
        return_dict = {
            "model_loss": model_loss,
            **model_loss_info,
            "actor_loss": actor_loss,
            "actor_entropy": ent,
            "critic_loss": critic_loss,
            "prior_ent": prior_ent,
            "post_ent": post_ent,
        }
        # pprint(return_dict)
        if gif is not None:
            return_dict["log_gif"] = gif
        self.stats_dict = return_dict

        loss_dict = self.stats_dict

        return (
            loss_dict["model_loss"],
            loss_dict["actor_loss"],
            loss_dict["critic_loss"],
        )

    @override(TorchPolicyV2)
    def postprocess_trajectory(
            self,
            sample_batch: SampleBatch,
            other_agent_batches: Optional[
                Dict[AgentID, Tuple["Policy", SampleBatch]]
            ] = None,
            episode: Optional["Episode"] = None,
    ) -> SampleBatch:
        """Batch format should be in the form of (s_t, a_(t-1), r_t)
        When t=0, the resetted obs is paired with action and reward of 0.
        """
        obs = sample_batch[SampleBatch.OBS]
        new_obs = sample_batch[SampleBatch.NEXT_OBS]
        action = sample_batch[SampleBatch.ACTIONS]
        reward = sample_batch[SampleBatch.REWARDS]
        eps_ids = sample_batch[SampleBatch.EPS_ID]
        dones = sample_batch[SampleBatch.DONES].astype(float)

        act_shape = action.shape
        act_reset = np.array([0.0] * act_shape[-1])[None]
        rew_reset = np.array(0.0)[None]
        obs_end = np.array(new_obs[act_shape[0] - 1])[None]
        done_reset = np.array(0.0)[None]

        batch_obs = np.concatenate([obs, obs_end], axis=0)
        batch_action = np.concatenate([act_reset, action], axis=0)
        batch_rew = np.concatenate([rew_reset, reward], axis=0)
        batch_eps_ids = np.concatenate([eps_ids, eps_ids[-1:]], axis=0)
        batch_done = np.concatenate([done_reset, dones], axis=0)

        new_batch = {
            SampleBatch.OBS: batch_obs,
            SampleBatch.REWARDS: batch_rew,
            SampleBatch.ACTIONS: batch_action,
            SampleBatch.EPS_ID: batch_eps_ids,
            SampleBatch.DONES: batch_done,
        }

        return SampleBatch(new_batch)

    def stats_fn(self, train_batch):
        return self.stats_dict

    @override(TorchPolicyV2)
    def optimizer(self):
        model = self.model
        encoder_weights = list(model.encoder.parameters())
        decoder_weights = list(model.decoder.parameters())
        reward_weights = list(model.reward.parameters())
        dynamics_weights = list(model.dynamics.parameters())
        actor_weights = list(model.actor.parameters())
        critic_weights = list(model.value.parameters())
        model_opt = torch.optim.Adam(
            encoder_weights + decoder_weights + reward_weights + dynamics_weights,
            lr=self.config["td_model_lr"], eps=1e-8
        )
        actor_opt = torch.optim.Adam(actor_weights, lr=self.config["actor_lr"], eps=1e-5)
        critic_opt = torch.optim.Adam(critic_weights, lr=self.config["critic_lr"], eps=1e-5)

        return (model_opt, actor_opt, critic_opt)

    def action_sampler_fn(policy, model, obs_batch, state_batches, explore, timestep):
        """Action sampler function has two phases. During the prefill phase,
        actions are sampled uniformly [-1, 1]. During training phase, actions
        are evaluated through DreamerPolicy and an additive gaussian is added
        to incentivize exploration.
        """
        obs = obs_batch["obs"]
        bsize = obs.shape[0]

        # Custom Exploration
        if timestep <= policy.config["prefill_timesteps"]:
            logp = None
            # Random action in space [-1.0, 1.0]
            eps = torch.rand(bsize, model.action_space.shape[0], device=obs.device)
            action = 2.0 * eps - 1.0
            state_batches = model.get_initial_state()
            # batchify the intial states to match the batch size of the obs tensor
            state_batches = batchify_states(state_batches, bsize, device=obs.device)
        else:
            # Weird RLlib Handling, this happens when env rests
            if len(state_batches[0].size()) == 3:
                # Very hacky, but works on all envs
                state_batches = model.get_initial_state().to(device=obs.device)
                # batchify the intial states to match the batch size of the obs tensor
                state_batches = batchify_states(state_batches, bsize, device=obs.device)
            action, logp, state_batches = model.policy(obs, state_batches, explore)
            action = td.Normal(action, policy.config["explore_noise"]).sample()
            action = torch.clamp(action, min=-1.0, max=1.0)

        # policy.global_timestep += policy.config["env_config"]["frame_skip"]
        policy.global_timestep += 1

        return action, logp, state_batches

    def make_model(self):

        # model = ModelCatalog.get_model_v2(
        #     self.observation_space,
        #     self.action_space,
        #     1,
        #     self.config["dreamer_model"],
        #     name="DreamerV3Model",
        #     framework="torch",
        # )
        from dreamerv3 import DreamerV3Config

        model = dreamerv3.dreamerv3_model.DreamerV3Model(
            self.observation_space,
            self.action_space,
            1,
            self.config["dreamer_model"],
            name="DreamerV3Model",
            # framework="torch",
        )

        self.model_variables = model.variables()

        return model

    def extra_grad_process(
            self, optimizer: "torch.optim.Optimizer", loss: TensorType
    ) -> Dict[str, TensorType]:
        return apply_grad_clipping(self, optimizer, loss)
