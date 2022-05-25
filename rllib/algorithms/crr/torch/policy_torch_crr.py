from typing import Union, Type, TYPE_CHECKING, List, Dict, cast


from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.utils import get_activation_fn

from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.algorithms.cql import CQLTorchPolicy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.algorithms.ddpg.ddpg_torch_policy import ddpg_actor_critic_loss
# from ray.rllib.algorithms.sac.sac_tf_policy import build_sac_model
from ray.rllib.algorithms.ddpg.ddpg_tf_policy import build_ddpg_models
from ray.rllib.algorithms.ddpg.ddpg_torch_model import DDPGTorchModel
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.algorithms.ddpg.noop_model import NoopModel, TorchNoopModel

from ray.rllib.models.torch.torch_modelv2 import TorchModelV2

from ray.rllib.agents import TrainerConfig
import gym
import numpy as np

# if TYPE_CHECKING:
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.policy import Policy
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper
from ray.rllib.utils.typing import (
    TrainerConfigDict,
    TensorType,
    LocalOptimizer,
    GradInfoDict,
    ModelConfigDict,
)

from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class CRRModelContinuous(TorchModelV2, nn.Module):

    def __init__(
        self,
        obs_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        num_outputs: int,
        model_config: ModelConfigDict,
        name: str,
    ):
        TorchModelV2.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )
        nn.Module.__init__(self)

        # TODO: I don't know why this is true yet? (in = num_outputs)
        self.obs_ins = num_outputs
        self.action_dim = np.product(self.action_space.shape)
        self.actor_model = self._build_actor_net('actor')
        twin_q = self.model_config['twin_q']
        self.q_model = self._build_q_net("q")
        if twin_q:
            self.twin_q_model = self._build_q_net("twin_q")
        else:
            self.twin_q_model = None

    def _build_actor_net(self, name_):
        actor_hidden_activation = self.model_config['actor_hidden_activation']
        actor_hiddens = self.model_config['actor_hiddens']

        # Build the policy network.
        actor_net = nn.Sequential()

        activation = get_activation_fn(actor_hidden_activation, framework="torch")
        ins = self.obs_ins
        for i, n in enumerate(actor_hiddens):
            actor_net.add_module(
                f"{name_}_hidden_{i}",
                SlimFC(
                    ins,
                    n,
                    initializer=torch.nn.init.xavier_uniform_,
                    activation_fn=activation,
                ),
            )
            ins = n

        actor_net.add_module(
            f"{name_}_out",
            SlimFC(
                ins,
                2 * self.action_dim,  # also includes log_std
                initializer=torch.nn.init.xavier_uniform_,
                activation_fn=None,
            ),
        )

        return actor_net

    def _build_q_net(self, name_):
        # TODO: only supports continuous actions at the moment
        # actions are concatenated with flattened obs
        critic_hidden_activation = self.model_config['critic_hidden_activation']
        critic_hiddens = self.model_config['critic_hiddens']

        activation = get_activation_fn(critic_hidden_activation, framework="torch")
        q_net = nn.Sequential()
        ins = self.obs_ins + self.action_dim
        for i, n in enumerate(critic_hiddens):
            q_net.add_module(
                f"{name_}_hidden_{i}",
                SlimFC(
                    ins,
                    n,
                    initializer=torch.nn.init.xavier_uniform_,
                    activation_fn=activation,
                ),
            )
            ins = n

        q_net.add_module(
            f"{name_}_out",
            SlimFC(
                ins,
                1,
                initializer=torch.nn.init.xavier_uniform_,
                activation_fn=None,
            ),
        )
        return q_net

    def get_q_values(self, model_out: TensorType, actions: TensorType) -> TensorType:
        """Return the Q estimates for the most recent forward pass.

        This implements Q(s, a).

        Args:
            model_out (Tensor): obs embeddings from the model layers, of shape
                [BATCH_SIZE, num_outputs].
            actions (Tensor): Actions to return the Q-values for.
                Shape: [BATCH_SIZE, action_dim].

        Returns:
            tensor of shape [BATCH_SIZE].
        """
        return self.q_model(torch.cat([model_out, actions], -1))


    def get_twin_q_values(
        self, model_out: TensorType, actions: TensorType
    ) -> TensorType:
        """Same as get_q_values but using the twin Q net.

        This implements the twin Q(s, a).

        Args:
            model_out (Tensor): obs embeddings from the model layers, of shape
                [BATCH_SIZE, num_outputs].
            actions (Optional[Tensor]): Actions to return the Q-values for.
                Shape: [BATCH_SIZE, action_dim].

        Returns:
            tensor of shape [BATCH_SIZE].
        """
        return self.twin_q_model(torch.cat([model_out, actions], -1))

    def get_policy_output(self, model_out: TensorType) -> TensorType:
        """Return the action output for the most recent forward pass.

        This outputs the support for pi(s). For continuous action spaces, this
        is the action directly. For discrete, is is the mean / std dev.

        Args:
            model_out (Tensor): obs embeddings from the model layers, of shape
                [BATCH_SIZE, num_outputs].

        Returns:
            tensor of shape [BATCH_SIZE, action_out_size]
        """
        return self.actor_model(model_out)

    def policy_variables(
        self, as_dict: bool = False
    ) -> Union[List[TensorType], Dict[str, TensorType]]:
        """Return the list of variables for the policy net."""
        if as_dict:
            return self.actor_model.state_dict()
        return list(self.actor_model.parameters())

    def q_variables(
        self, as_dict=False
    ) -> Union[List[TensorType], Dict[str, TensorType]]:
        """Return the list of variables for Q / twin Q nets."""
        if as_dict:
            return {
                **self.q_model.state_dict(),
                **(self.twin_q_model.state_dict() if self.twin_q_model else {}),
            }
        return list(self.q_model.parameters()) + (
            list(self.twin_q_model.parameters()) if self.twin_q_model else []
        )


class CRRTorchPolicy(TorchPolicyV2):

    def __init__(
        self,
        observation_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        config: TrainerConfigDict,
    ):

        self.target_model = None  # assign it in self.make_model

        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )

        # by here your model should include the following
        # (We assume state s is already encoded and there is no need to use RNNs/other models to encode observations into states):
        # 1. a nn representing the actor pi(a|s)
        #   1.1* in case of continuous actions it should be normal / squashed normal dist if the action space is bounded?
        #   1.2 in case of of discrete set of actions the output of the model should be a discrete distribution over action classes
        # 2. a nn representing the critic Q(s, a)
        #   2.1* in case of continuous actions it should take in concat([s,a]) and output a single scalar
        #   2.2 in case of discrete actions it should take in s and output a logit for each action class as well as a scale for matching the reward scale.
        # 3. for critic it should have n_critic copies of the Q function nn
        # 4. for each critic it should have a target model copy

    def make_model(self) -> ModelV2:
        # copying ddpg build model to here to be explicit
        model_config = self.config['model']
        model_config.update(dict(
            actor_hidden_activation=self.config["actor_hidden_activation"],
            actor_hiddens=self.config["actor_hiddens"],
            critic_hidden_activation=self.config["critic_hidden_activation"],
            critic_hiddens=self.config["critic_hiddens"],
            twin_q=self.config["twin_q"],
        ))
        num_outputs = int(np.product(self.observation_space.shape))

        # TODO: why do we even have to go through this get_model_v2 function?
        self.model = ModelCatalog.get_model_v2(
            obs_space=self.observation_space,
            action_space=self.action_space,
            num_outputs=num_outputs,
            model_config=model_config,
            framework=self.config["framework"],
            model_interface=CRRModelContinuous,  # use this model for interface (get_q, get_q_twin, .etc)
            default_model=TorchNoopModel,
            name="model",
        )

        # TODO: this is a bad python pattern to assign attributes that do not exist in the constructor
        self.target_model = ModelCatalog.get_model_v2(
            obs_space=self.observation_space,
            action_space=self.action_space,
            num_outputs=num_outputs,
            model_config=model_config,
            framework=self.config["framework"],
            model_interface=CRRModelContinuous,  # use this model for interface (get_q, get_q_twin, .etc)
            default_model=TorchNoopModel,
            name="target_model",
        )

        return self.model

    def loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:

        ############ update the actor
        # compute the weights assigned to every transition (s_t, a_t) and log(pi(a_t|s_t))
        self._compute_action_weights_and_logps(model, dist_class, train_batch)

        # compute actor loss
        actor_loss = self._compute_actor_loss(model, dist_class, train_batch)

        ############ update the critic
        # standard critic update with pessimistic Q-learning (e.g. DQN)
        critic_loss = self._compute_critic_loss(model, dist_class, train_batch)

        return {'actor': actor_loss, 'critic': critic_loss}

    def _get_q_value(self, model: ModelV2, model_out: TensorType, actions: TensorType) -> TensorType:
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
        # uses mean|max to compute estimate of advantages
        # continuous/discrete action spaces:
        #   A(s_t, a_t) = Q(s_t, a_t) - max_{a^j} Q(s_t, a^j) where a^j is m times sampled from the policy p(a | s_t)
        #   A(s_t, a_t) = Q(s_t, a_t) - avg( Q(s_t, a^j) ) where a^j is m times sampled from the policy p(a | s_t)
        # questions: Do we use pessimistic q approximate or the normal one?
        advantage_type = self.config['advantage_type']
        n_action_sample = self.config['n_action_sample']
        batch_size = len(train_batch)
        out_t, _ = model(train_batch)

        # construct pi(s_t) for sampling actions
        pi_s_t = dist_class(model.get_policy_output(out_t), model)
        policy_actions = pi_s_t.dist.sample((n_action_sample, ))  # samples
        flat_actions = policy_actions.view(-1, *self.action_space.shape)

        # compute the logp of the actions in the dataset
        train_batch[SampleBatch.ACTION_LOGP] = pi_s_t.dist.log_prob(train_batch[SampleBatch.ACTIONS])

        reshaped_s_t = train_batch[SampleBatch.OBS].view(1, batch_size, *self.observation_space.shape)
        reshaped_s_t = reshaped_s_t.expand(
            n_action_sample, batch_size, *self.observation_space.shape
        )
        flat_s_t = reshaped_s_t.reshape(-1, *self.observation_space.shape)

        input_next = SampleBatch(
            **{SampleBatch.OBS: flat_s_t, SampleBatch.ACTIONS: flat_actions}
        )
        out_next, _ = model(input_next)

        flat_q_next = self._get_q_value(model, out_next, flat_actions)
        reshaped_q_next = flat_q_next.reshape(-1, batch_size, 1)

        if advantage_type == 'mean':
            v_next = reshaped_q_next.mean(dim=0)
        elif advantage_type == 'max':
            v_next = reshaped_q_next.max(dim=0)
        else:
            raise ValueError(
                f'Invalid advantage type: {advantage_type}.'
            )

        q_t = model.get_q_values(out_t, train_batch[SampleBatch.ACTIONS])

        adv_t = q_t - v_next
        train_batch['advantages'] = adv_t


    def _compute_action_weights_and_logps(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> None:
        # uses bin|exp to compute action weights
        # 1(A>=0) or exp(A/temp)

        weight_type = self.config['weight_type']
        self._compute_adv_and_logps(model, dist_class, train_batch)

        if weight_type == "bin":
            weights = (train_batch['advantages'] > 0.0).float()
        elif weight_type == "exp":
            temperature = self.config['temperature']
            max_weight = self.config['max_weight']
            weights = (train_batch['advantages'] / temperature).exp().clamp(0.0, max_weight)
        else:
            raise ValueError(f"invalid weight type: {weight_type}.")

        train_batch['action_weights'] = weights


    def _compute_actor_loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        loss = - (train_batch['action_weights'] * train_batch[SampleBatch.ACTION_LOGP]).mean()
        return loss

    def _compute_critic_loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch
    ):
        discount = self.config['gamma']

        # Compute bellman targets to regress on
        # target, use target model to compute the target
        target_model = cast(CRRModelContinuous, self.target_models[model])
        target_out_next, _ = target_model({SampleBatch.OBS: train_batch[SampleBatch.NEXT_OBS]})

        with torch.no_grad():
            # get the action of the current policy evaluated at the next state
            pi_s_next = dist_class(target_model.get_policy_output(target_out_next), target_model)
            target_a_next = pi_s_next.sample()
            target_a_next = target_a_next.clamp(
                torch.from_numpy(self.action_space.low).to(target_a_next),
                torch.from_numpy(self.action_space.high).to(target_a_next)
            )

            q1_target = target_model.get_q_values(target_out_next, target_a_next)
            q2_target = target_model.get_twin_q_values(target_out_next, target_a_next)
            target_q_next = torch.minimum(q1_target, q2_target).squeeze(-1)

            target = train_batch[SampleBatch.REWARDS] + discount * (1.0 - train_batch[SampleBatch.DONES].float()) * target_q_next

        # compute the predicted output
        model = cast(CRRModelContinuous, model)
        model_out_next, _ = model({SampleBatch.OBS: train_batch[SampleBatch.OBS]})
        q1 = model.get_q_values(model_out_next, train_batch[SampleBatch.ACTIONS]).squeeze(-1)
        q2 = model.get_twin_q_values(model_out_next, train_batch[SampleBatch.ACTIONS]).squeeze(-1)

        # compute the MSE loss for all q-functions
        loss = 0.5 * ((target - q1) ** 2 + (target - q2) ** 2)
        loss = loss.mean(-1)

        return loss

if __name__ == '__main__':

    obs_space = gym.spaces.Box(np.array((-1, -1)), np.array((1, 1)))
    act_space =  gym.spaces.Box(np.array((-1,-1)), np.array((1, 1)))
    config = TrainerConfig().framework(framework='torch').to_dict()
    print(config['framework'])
    CRRTorchPolicy(obs_space, act_space, config=config)