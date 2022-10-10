"""Contributed port of MADDPG from OpenAI baselines.

The implementation has a couple assumptions:
- The number of agents is fixed and known upfront.
- Each agent is bound to a policy of the same name.
- Discrete actions are sent as logits (pre-softmax).

For a minimal example, see rllib/examples/two_step_game.py,
and the README for how to run with the multi-agent particle envs.
"""

import logging
from typing import List, Optional, Type

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.dqn.dqn import DQN
from ray.rllib.algorithms.maddpg.maddpg_tf_policy import MADDPGTFPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils.annotations import Deprecated, override
from ray.rllib.utils.typing import AlgorithmConfigDict
from ray.rllib.utils.deprecation import DEPRECATED_VALUE

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MADDPGConfig(AlgorithmConfig):
    """Defines a configuration class from which a MADDPG Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.maddpg.maddpg import MADDPGConfig
        >>> config = MADDPGConfig()
        >>> print(config.replay_buffer_config)
        >>> replay_config = config.replay_buffer_config.update(
        >>>     {
        >>>         "capacity": 100000,
        >>>         "prioritized_replay_alpha": 0.8,
        >>>         "prioritized_replay_beta": 0.45,
        >>>         "prioritized_replay_eps": 2e-6,
        >>>     }
        >>> )
        >>> config.training(replay_buffer_config=replay_config)\
        >>>       .resources(num_gpus=0)\
        >>>       .rollouts(num_rollout_workers=4)\
        >>>       .environment("CartPole-v1")
        >>> algo = config.build()
        >>> while True:
        >>>     algo.train()

    Example:
        >>> from ray.rllib.algorithms.maddpg.maddpg import MADDPGConfig
        >>> from ray import air
        >>> from ray import tune
        >>> config = MADDPGConfig()
        >>> config.training(n_step=tune.grid_search([3, 5]))
        >>> config.environment(env="CartPole-v1")
        >>> tune.Tuner(
        >>>     "MADDPG",
        >>>     run_config=air.RunConfig(stop={"episode_reward_mean":200}),
        >>>     param_space=config.to_dict()
        >>> ).fit()
    """

    def __init__(self, algo_class=None):
        """Initializes a DQNConfig instance."""
        super().__init__(algo_class=algo_class or MADDPG)

        # fmt: off
        # __sphinx_doc_begin__
        # MADDPG specific config settings:
        self.agent_id = None
        self.use_local_critic = False
        self.use_state_preprocessor = False
        self.actor_hiddens = [64, 64]
        self.actor_hidden_activation = "relu"
        self.critic_hiddens = [64, 64]
        self.critic_hidden_activation = "relu"
        self.n_step = 1
        self.good_policy = "maddpg"
        self.adv_policy = "maddpg"
        self.replay_buffer_config = {
            "type": "MultiAgentReplayBuffer",
            # Specify prioritized replay by supplying a buffer type that supports
            # prioritization, for example: MultiAgentPrioritizedReplayBuffer.
            "prioritized_replay": DEPRECATED_VALUE,
            "capacity": int(1e6),
            # Force lockstep replay mode for MADDPG.
            "replay_mode": "lockstep",
        }
        self.training_intensity = None
        # Number of timesteps to collect from rollout workers before we start
        # sampling from replay buffers for learning. Whether we count this in agent
        # steps  or environment steps depends on config["multiagent"]["count_steps_by"].
        self.num_steps_sampled_before_learning_starts = 1024 * 25
        self.critic_lr = 1e-2
        self.actor_lr = 1e-2
        self.target_network_update_freq = 0
        self.tau = 0.01
        self.actor_feature_reg = 0.001
        self.grad_norm_clipping = 0.5

        # Changes to Algorithm's default:
        self.rollout_fragment_length = 100
        self.train_batch_size = 1024
        self.num_workers = 1
        self.min_time_s_per_iteration = 0
        # fmt: on
        # __sphinx_doc_end__

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        agent_id: Optional[str] = None,
        use_local_critic: Optional[bool] = None,
        use_state_preprocessor: Optional[bool] = None,
        actor_hiddens: Optional[List[int]] = None,
        actor_hidden_activation: Optional[str] = None,
        critic_hiddens: Optional[List[int]] = None,
        critic_hidden_activation: Optional[str] = None,
        n_step: Optional[int] = None,
        good_policy: Optional[str] = None,
        adv_policy: Optional[str] = None,
        replay_buffer_config: Optional[dict] = None,
        training_intensity: Optional[float] = None,
        critic_lr: Optional[float] = None,
        actor_lr: Optional[float] = None,
        target_network_update_freq: Optional[int] = None,
        tau: Optional[float] = None,
        actor_feature_reg: Optional[float] = None,
        grad_norm_clipping: Optional[float] = None,
        **kwargs,
    ) -> "MADDPGConfig":
        """Sets the training related configuration.

        Args:
            agent_id: ID of the agent controlled by this policy.
            use_local_critic: Use a local critic for this policy.
            use_state_preprocessor: Apply a state preprocessor with spec given by the
                "model" config option (like other RL algorithms). This is mostly useful
                if you have a weird observation shape, like an image. Disabled by
                default.
            actor_hiddens: Postprocess the policy network model output with these hidden
                layers. If `use_state_preprocessor` is False, then these will be the
                *only* hidden layers in the network.
            actor_hidden_activation: Hidden layers activation of the postprocessing
                stage of the policy network.
            critic_hiddens: Postprocess the critic network model output with these
                hidden layers; again, if use_state_preprocessor is True, then the state
                will be preprocessed by the model specified with the "model" config
                option first.
            critic_hidden_activation: Hidden layers activation of the postprocessing
                state of the critic.
            n_step: N-step for Q-learning.
            good_policy: Algorithm for good policies.
            adv_policy: Algorithm for adversary policies.
            replay_buffer_config: Replay buffer config.
                Examples:
                {
                "_enable_replay_buffer_api": True,
                "type": "MultiAgentReplayBuffer",
                "capacity": 50000,
                "replay_sequence_length": 1,
                }
                - OR -
                {
                "_enable_replay_buffer_api": True,
                "type": "MultiAgentPrioritizedReplayBuffer",
                "capacity": 50000,
                "prioritized_replay_alpha": 0.6,
                "prioritized_replay_beta": 0.4,
                "prioritized_replay_eps": 1e-6,
                "replay_sequence_length": 1,
                }
                - Where -
                prioritized_replay_alpha: Alpha parameter controls the degree of
                prioritization in the buffer. In other words, when a buffer sample has
                a higher temporal-difference error, with how much more probability
                should it drawn to use to update the parametrized Q-network. 0.0
                corresponds to uniform probability. Setting much above 1.0 may quickly
                result as the sampling distribution could become heavily “pointy” with
                low entropy.
                prioritized_replay_beta: Beta parameter controls the degree of
                importance sampling which suppresses the influence of gradient updates
                from samples that have higher probability of being sampled via alpha
                parameter and the temporal-difference error.
                prioritized_replay_eps: Epsilon parameter sets the baseline probability
                for sampling so that when the temporal-difference error of a sample is
                zero, there is still a chance of drawing the sample.
            training_intensity: If set, this will fix the ratio of replayed from a
                buffer and learned on timesteps to sampled from an environment and
                stored in the replay buffer timesteps. Otherwise, the replay will
                proceed at the native ratio determined by
                `(train_batch_size / rollout_fragment_length)`.
            critic_lr: Learning rate for the critic (Q-function) optimizer.
            actor_lr: Learning rate for the actor (policy) optimizer.
            target_network_update_freq: Update the target network every
                `target_network_update_freq` sample steps.
            tau: Update the target by \tau * policy + (1-\tau) * target_policy.
            actor_feature_reg: Weights for feature regularization for the actor.
            grad_norm_clipping: If not None, clip gradients during optimization at this
                value.

        Returns:
            This updated AlgorithmConfig object.
        """

        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if agent_id is not None:
            self.agent_id = agent_id
        if use_local_critic is not None:
            self.use_local_critic = use_local_critic
        if use_state_preprocessor is not None:
            self.use_state_preprocessor = use_state_preprocessor
        if actor_hiddens is not None:
            self.actor_hiddens = actor_hiddens
        if actor_hidden_activation is not None:
            self.actor_hidden_activation = actor_hidden_activation
        if critic_hiddens is not None:
            self.critic_hiddens = critic_hiddens
        if critic_hidden_activation is not None:
            self.critic_hidden_activation = critic_hidden_activation
        if n_step is not None:
            self.n_step = n_step
        if good_policy is not None:
            self.good_policy = good_policy
        if adv_policy is not None:
            self.adv_policy = adv_policy
        if replay_buffer_config is not None:
            self.replay_buffer_config = replay_buffer_config
        if training_intensity is not None:
            self.training_intensity = training_intensity
        if critic_lr is not None:
            self.critic_lr = critic_lr
        if actor_lr is not None:
            self.actor_lr = actor_lr
        if target_network_update_freq is not None:
            self.target_network_update_freq = target_network_update_freq
        if tau is not None:
            self.tau = tau
        if actor_feature_reg is not None:
            self.actor_feature_reg = actor_feature_reg
        if grad_norm_clipping is not None:
            self.grad_norm_clipping = grad_norm_clipping

        return self


def before_learn_on_batch(multi_agent_batch, policies, train_batch_size):
    samples = {}

    # Modify keys.
    for pid, p in policies.items():
        i = p.config["agent_id"]
        keys = multi_agent_batch.policy_batches[pid].keys()
        keys = ["_".join([k, str(i)]) for k in keys]
        samples.update(dict(zip(keys, multi_agent_batch.policy_batches[pid].values())))

    # Make ops and feed_dict to get "new_obs" from target action sampler.
    new_obs_ph_n = [p.new_obs_ph for p in policies.values()]
    new_obs_n = list()
    for k, v in samples.items():
        if "new_obs" in k:
            new_obs_n.append(v)

    for i, p in enumerate(policies.values()):
        feed_dict = {new_obs_ph_n[i]: new_obs_n[i]}
        new_act = p.get_session().run(p.target_act_sampler, feed_dict)
        samples.update({"new_actions_%d" % i: new_act})

    # Share samples among agents.
    policy_batches = {pid: SampleBatch(samples) for pid in policies.keys()}
    return MultiAgentBatch(policy_batches, train_batch_size)


class MADDPG(DQN):
    @classmethod
    @override(DQN)
    def get_default_config(cls) -> AlgorithmConfigDict:
        return MADDPGConfig().to_dict()

    @override(DQN)
    def validate_config(self, config: AlgorithmConfigDict) -> None:
        """Adds the `before_learn_on_batch` hook to the config.

        This hook is called explicitly prior to TrainOneStep() in the execution
        setups for DQN and APEX.
        """
        # Call super's validation method.
        super().validate_config(config)

        def f(batch, workers, config):
            policies = dict(
                workers.local_worker().foreach_policy_to_train(lambda p, i: (i, p))
            )
            return before_learn_on_batch(batch, policies, config["train_batch_size"])

        config["before_learn_on_batch"] = f

    @override(DQN)
    def get_default_policy_class(self, config: AlgorithmConfigDict) -> Type[Policy]:
        return MADDPGTFPolicy


# Deprecated: Use ray.rllib.algorithms.maddpg.MADDPG instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(MADDPGConfig().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.maddpg.maddpg.DEFAULT_CONFIG",
        new="ray.rllib.algorithms.maddpg.maddpg.MADDPGConfig(...)",
        error=True,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
