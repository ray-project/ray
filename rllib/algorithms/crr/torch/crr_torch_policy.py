import gym
import numpy as np

from typing import (
    cast,
    Dict,
    List,
    Tuple,
    Type,
    Union,
)

from ray.rllib.algorithms import AlgorithmConfig
from ray.rllib.algorithms.crr.torch import CRRModel
from ray.rllib.algorithms.ddpg.noop_model import TorchNoopModel
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import (
    TorchCategorical,
    TorchDistributionWrapper,
    get_torch_categorical_class_with_temperature,
)
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.policy.torch_mixins import TargetNetworkMixin
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.torch_utils import (
    huber_loss,
    l2_loss,
)
from ray.rllib.utils.typing import (
    TrainerConfigDict,
    TensorType,
)

torch, nn = try_import_torch()


class CRRTorchPolicy(TorchPolicyV2, TargetNetworkMixin):
    def __init__(
        self,
        observation_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        config: TrainerConfigDict,
    ):

        self.target_model = None  # assign it in self.make_model
        self._is_action_discrete = isinstance(action_space, gym.spaces.Discrete)

        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )

        # For discreet action space, use a custom TorchCategorical distribution
        # that supports temperature.
        if self._is_action_discrete:
            assert self.dist_class == TorchCategorical
            self.dist_class = get_torch_categorical_class_with_temperature(
                config["categorical_distribution_temperature"]
            )

        """
        by here your model should include the following
        (We assume state s is already encoded and there is no need to use RNNs/other
        models to encode observations into states):
        1. a nn representing the actor pi(a|s)
          1.1* in case of continuous actions it should be normal / squashed normal
          dist if the action space is bounded?
          1.2 in case of of discrete set of actions the output of the model should be
          a discrete distribution over action classes
        2. a nn representing the critic Q(s, a)
          2.1* in case of continuous actions it should take in concat([s,a]) and output
           a single scalar
          2.2 in case of discrete actions it should take in s and output a logit for
          each action class as well as a scale for matching the reward scale.
        3. for critic it should have n_critic copies of the Q function nn
        4. for each critic it should have a target model copy
        """

    def action_distribution_fn(
        self,
        model: ModelV2,
        *,
        obs_batch: TensorType,
        state_batches: TensorType,
        **kwargs,
    ) -> Tuple[TensorType, type, List[TensorType]]:

        model_out, _ = model(obs_batch)
        dist_input = model.get_policy_output(model_out)
        dist_class = self.dist_class

        return dist_input, dist_class, []

    def make_model(self) -> ModelV2:
        # copying ddpg build model to here to be explicit
        model_config = self.config["model"]
        model_config.update(
            dict(
                actor_hidden_activation=self.config["actor_hidden_activation"],
                actor_hiddens=self.config["actor_hiddens"],
                critic_hidden_activation=self.config["critic_hidden_activation"],
                critic_hiddens=self.config["critic_hiddens"],
                twin_q=self.config["twin_q"],
            )
        )
        num_outputs = int(np.product(self.observation_space.shape))

        # TODO: why do we even have to go through this get_model_v2 function?
        self.model = ModelCatalog.get_model_v2(
            obs_space=self.observation_space,
            action_space=self.action_space,
            num_outputs=num_outputs,
            model_config=model_config,
            framework=self.config["framework"],
            # use this model for interface (get_q, get_q_twin, .etc)
            model_interface=CRRModel,
            default_model=TorchNoopModel,
            name="model",
        )

        # TODO: this is a bad python pattern to assign attributes that do not exist in
        #  the constructor
        self.target_model = ModelCatalog.get_model_v2(
            obs_space=self.observation_space,
            action_space=self.action_space,
            num_outputs=num_outputs,
            model_config=model_config,
            framework=self.config["framework"],
            # use this model for interface (get_q, get_q_twin, .etc)
            model_interface=CRRModel,
            default_model=TorchNoopModel,
            name="target_model",
        )

        return self.model

    def optimizer(
        self,
    ) -> Union[List["torch.optim.Optimizer"], "torch.optim.Optimizer"]:

        # Set epsilons to match tf.keras.optimizers.Adam's epsilon default.
        actor_optimizer = torch.optim.Adam(
            params=self.model.policy_variables(),
            lr=self.config["actor_lr"],
            betas=(0.9, 0.999),
            eps=1e-8,
        )

        critic_optimizer = torch.optim.Adam(
            params=self.model.q_variables(),
            lr=self.config["critic_lr"],
            betas=(0.9, 0.999),
            eps=1e-8,
        )

        # Return them in the same order as the respective loss terms are returned.
        return actor_optimizer, critic_optimizer

    def loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:

        # update the actor
        # compute the weights assigned to every transition
        # (s_t, a_t) and log(pi(a_t|s_t))
        self._compute_action_weights_and_logps(model, dist_class, train_batch)

        # compute actor loss
        actor_loss = self._compute_actor_loss(model, dist_class, train_batch)

        # update the critic
        # standard critic update with pessimistic Q-learning (e.g. DQN)
        critic_loss = self._compute_critic_loss(model, dist_class, train_batch)

        self.log("loss_actor", actor_loss)
        self.log("loss_critic", critic_loss)

        return actor_loss, critic_loss

    def log(self, key, value):
        # internal log function
        self.model.tower_stats[key] = value

    # def update_target(self):
    #     tau = self.config['tau']
    #
    #     model_params = self.model.parameters()
    #     target_params = self.target_models[self.mode].parameters()
    #     for src_p, trg_p in zip(model_params, target_params):
    #         trg_p.data = (1 - tau) * trg_p.data + tau * src_p.data

    @override(TorchPolicyV2)
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        stats_dict = {
            k: torch.stack(self.get_tower_stats(k)).mean().item()
            for k in self.model.tower_stats
        }
        return stats_dict

    def _get_q_value(
        self, model: ModelV2, model_out: TensorType, actions: TensorType
    ) -> TensorType:
        # helper function to compute the pessimistic q value
        q1 = model.get_q_values(model_out, actions)
        q2 = model.get_twin_q_values(model_out, actions)
        return torch.minimum(q1, q2)

    def _compute_adv_and_logps(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> None:
        # uses mean|max|expectation to compute estimate of advantages
        # continuous/discrete action spaces:
        # for max:
        #   A(s_t, a_t) = Q(s_t, a_t) - max_{a^j} Q(s_t, a^j)
        #   where a^j is m times sampled from the policy p(a | s_t)
        # for mean:
        #   A(s_t, a_t) = Q(s_t, a_t) - avg( Q(s_t, a^j) )
        #   where a^j is m times sampled from the policy p(a | s_t)
        # discrete action space and adv_type=expectation:
        #   A(s_t, a_t) = Q(s_t, a_t) - sum_j[Q(s_t, a^j) * pi(a^j)]
        advantage_type = self.config["advantage_type"]
        n_action_sample = self.config["n_action_sample"]
        batch_size = len(train_batch)
        out_t, _ = model(train_batch)

        # construct pi(s_t) and Q(s_t, a_t) for computing advantage actions
        pi_s_t = dist_class(model.get_policy_output(out_t), model)
        q_t = self._get_q_value(model, out_t, train_batch[SampleBatch.ACTIONS])

        # compute the logp of the actions in the dataset (for computing actor's loss)
        action_logp = pi_s_t.dist.log_prob(train_batch[SampleBatch.ACTIONS])

        # fix the shape if it's not canonical (i.e. shape[-1] != 1)
        if len(action_logp.shape) <= 1:
            action_logp.unsqueeze_(-1)
        train_batch[SampleBatch.ACTION_LOGP] = action_logp

        if advantage_type == "expectation":
            assert (
                self._is_action_discrete
            ), "Action space should be discrete when advantage_type = expectation."
            assert hasattr(
                self.model, "q_model"
            ), "CRR's ModelV2 should have q_model neural network in discrete \
                action spaces"
            assert isinstance(
                pi_s_t.dist, torch.distributions.Categorical
            ), "The output of the policy should be a torch Categorical \
                distribution."

            q_vals = self.model.q_model(out_t)
            if hasattr(self.model, "twin_q_model"):
                q_twins = self.model.twin_q_model(out_t)
                q_vals = torch.minimum(q_vals, q_twins)

            probs = pi_s_t.dist.probs
            v_t = (q_vals * probs).sum(-1, keepdims=True)
        else:
            policy_actions = pi_s_t.dist.sample((n_action_sample,))  # samples

            if self._is_action_discrete:
                flat_actions = policy_actions.reshape(-1)
            else:
                flat_actions = policy_actions.reshape(-1, *self.action_space.shape)

            reshaped_s_t = train_batch[SampleBatch.OBS].view(
                1, batch_size, *self.observation_space.shape
            )
            reshaped_s_t = reshaped_s_t.expand(
                n_action_sample, batch_size, *self.observation_space.shape
            )
            flat_s_t = reshaped_s_t.reshape(-1, *self.observation_space.shape)

            input_v_t = SampleBatch(
                **{SampleBatch.OBS: flat_s_t, SampleBatch.ACTIONS: flat_actions}
            )
            out_v_t, _ = model(input_v_t)

            flat_q_st_pi = self._get_q_value(model, out_v_t, flat_actions)
            reshaped_q_st_pi = flat_q_st_pi.reshape(-1, batch_size, 1)

            if advantage_type == "mean":
                v_t = reshaped_q_st_pi.mean(dim=0)
            elif advantage_type == "max":
                v_t, _ = reshaped_q_st_pi.max(dim=0)
            else:
                raise ValueError(f"Invalid advantage type: {advantage_type}.")

        adv_t = q_t - v_t
        train_batch["advantages"] = adv_t

        # logging
        self.log("q_batch_avg", q_t.mean())
        self.log("q_batch_max", q_t.max())
        self.log("q_batch_min", q_t.min())
        self.log("v_batch_avg", v_t.mean())
        self.log("v_batch_max", v_t.max())
        self.log("v_batch_min", v_t.min())
        self.log("adv_batch_avg", adv_t.mean())
        self.log("adv_batch_max", adv_t.max())
        self.log("adv_batch_min", adv_t.min())
        self.log("reward_batch_avg", train_batch[SampleBatch.REWARDS].mean())

    def _compute_action_weights_and_logps(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> None:
        # uses bin|exp to compute action weights
        # 1(A>=0) or exp(A/temp)

        weight_type = self.config["weight_type"]
        self._compute_adv_and_logps(model, dist_class, train_batch)

        if weight_type == "bin":
            weights = (train_batch["advantages"] > 0.0).float()
        elif weight_type == "exp":
            temperature = self.config["temperature"]
            max_weight = self.config["max_weight"]
            weights = (
                (train_batch["advantages"] / temperature).exp().clamp(0.0, max_weight)
            )
        else:
            raise ValueError(f"invalid weight type: {weight_type}.")

        train_batch["action_weights"] = weights

        # logging
        self.log("weights_avg", weights.mean())
        self.log("weights_max", weights.max())
        self.log("weights_min", weights.min())

    def _compute_actor_loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        loss = -(
            train_batch["action_weights"] * train_batch[SampleBatch.ACTION_LOGP]
        ).mean(0)
        return loss

    def _compute_critic_loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ):
        discount = self.config["gamma"]

        # Compute bellman targets to regress on
        # target, use target model to compute the target
        target_model = cast(CRRModel, self.target_models[model])
        target_out_next, _ = target_model(
            {SampleBatch.OBS: train_batch[SampleBatch.NEXT_OBS]}
        )

        # compute target values with no gradient
        with torch.no_grad():
            # get the action of the current policy evaluated at the next state
            pi_s_next = dist_class(
                target_model.get_policy_output(target_out_next), target_model
            )
            target_a_next = pi_s_next.sample()
            if not self._is_action_discrete:
                target_a_next = target_a_next.clamp(
                    torch.from_numpy(self.action_space.low).to(target_a_next),
                    torch.from_numpy(self.action_space.high).to(target_a_next),
                )

            # q1_target = target_model.get_q_values(target_out_next, target_a_next)
            # q2_target = target_model.get_twin_q_values(target_out_next, target_a_next)
            # target_q_next = torch.minimum(q1_target, q2_target).squeeze(-1)
            target_q_next = self._get_q_value(
                target_model, target_out_next, target_a_next
            ).squeeze(-1)

            target = (
                train_batch[SampleBatch.REWARDS]
                + discount
                * (1.0 - train_batch[SampleBatch.DONES].float())
                * target_q_next
            )

        # compute the predicted output
        model = cast(CRRModel, model)
        model_out_t, _ = model({SampleBatch.OBS: train_batch[SampleBatch.OBS]})
        q1 = model.get_q_values(model_out_t, train_batch[SampleBatch.ACTIONS]).squeeze(
            -1
        )
        q2 = model.get_twin_q_values(
            model_out_t, train_batch[SampleBatch.ACTIONS]
        ).squeeze(-1)

        # compute the MSE loss for all q-functions
        td_error_q1 = q1 - target
        td_error_q2 = q2 - target
        loss_fn = l2_loss if self.config["td_error_loss_fn"] == "mse" else huber_loss
        loss = torch.mean(loss_fn(torch.cat((td_error_q1, td_error_q2), dim=0)))

        # logging
        self.log("td_error_q1", (td_error_q1 ** 2).mean())
        self.log("td_error_q2", (td_error_q2 ** 2).mean())
        self.log("td_error", loss)
        self.log("targets_avg", target.mean())
        self.log("targets_max", target.max())
        self.log("targets_min", target.min())

        return loss


if __name__ == "__main__":

    obs_space = gym.spaces.Box(np.array((-1, -1)), np.array((1, 1)))
    act_space = gym.spaces.Box(np.array((-1, -1)), np.array((1, 1)))
    config = AlgorithmConfig().framework(framework="torch").to_dict()
    print(config["framework"])
    CRRTorchPolicy(obs_space, act_space, config=config)
