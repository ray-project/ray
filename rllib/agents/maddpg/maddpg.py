"""Contributed port of MADDPG from OpenAI baselines.

The implementation has a couple assumptions:
- The number of agents is fixed and known upfront.
- Each agent is bound to a policy of the same name.
- Discrete actions are sent as logits (pre-softmax).

For a minimal example, see rllib/examples/two_step_game.py,
and the README for how to run with the multi-agent particle envs.
"""

import logging
from typing import Type

from ray.rllib.agents.maddpg.maddpg_tf_policy import MADDPGTFPolicy
from ray.rllib.agents.dqn.dqn import DQNTrainer
from ray.rllib.agents.trainer import COMMON_CONFIG, with_common_config
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.typing import TrainerConfigDict

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# fmt: off
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # === Framework to run the algorithm ===
    "framework": "tf",

    # === Settings for each individual policy ===
    # ID of the agent controlled by this policy
    "agent_id": None,
    # Use a local critic for this policy.
    "use_local_critic": False,

    # === Evaluation ===
    # Evaluation interval
    "evaluation_interval": None,
    # Number of episodes to run per evaluation period.
    "evaluation_duration": 10,

    # === Model ===
    # Apply a state preprocessor with spec given by the "model" config option
    # (like other RL algorithms). This is mostly useful if you have a weird
    # observation shape, like an image. Disabled by default.
    "use_state_preprocessor": False,
    # Postprocess the policy network model output with these hidden layers. If
    # use_state_preprocessor is False, then these will be the *only* hidden
    # layers in the network.
    "actor_hiddens": [64, 64],
    # Hidden layers activation of the postprocessing stage of the policy
    # network
    "actor_hidden_activation": "relu",
    # Postprocess the critic network model output with these hidden layers;
    # again, if use_state_preprocessor is True, then the state will be
    # preprocessed by the model specified with the "model" config option first.
    "critic_hiddens": [64, 64],
    # Hidden layers activation of the postprocessing state of the critic.
    "critic_hidden_activation": "relu",
    # N-step Q learning
    "n_step": 1,
    # Algorithm for good policies.
    "good_policy": "maddpg",
    # Algorithm for adversary policies.
    "adv_policy": "maddpg",

    # === Replay buffer ===
    # Size of the replay buffer. Note that if async_updates is set, then
    # each worker will have a replay buffer of this size.
    "buffer_size": DEPRECATED_VALUE,
    "replay_buffer_config": {
        "type": "MultiAgentReplayBuffer",
        "capacity": int(1e6),
    },
    # Observation compression. Note that compression makes simulation slow in
    # MPE.
    "compress_observations": False,
    # If set, this will fix the ratio of replayed from a buffer and learned on
    # timesteps to sampled from an environment and stored in the replay buffer
    # timesteps. Otherwise, the replay will proceed at the native ratio
    # determined by (train_batch_size / rollout_fragment_length).
    "training_intensity": None,
    # Force lockstep replay mode for MADDPG.
    "multiagent": merge_dicts(COMMON_CONFIG["multiagent"], {
        "replay_mode": "lockstep",
    }),

    # === Optimization ===
    # Learning rate for the critic (Q-function) optimizer.
    "critic_lr": 1e-2,
    # Learning rate for the actor (policy) optimizer.
    "actor_lr": 1e-2,
    # Update the target network every `target_network_update_freq` steps.
    "target_network_update_freq": 0,
    # Update the target by \tau * policy + (1-\tau) * target_policy
    "tau": 0.01,
    # Weights for feature regularization for the actor
    "actor_feature_reg": 0.001,
    # If not None, clip gradients during optimization at this value
    "grad_norm_clipping": 0.5,
    # How many steps of the model to sample before learning starts.
    "learning_starts": 1024 * 25,
    # Update the replay buffer with this many samples at once. Note that this
    # setting applies per-worker if num_workers > 1.
    "rollout_fragment_length": 100,
    # Size of a batched sampled from replay buffer for training. Note that
    # if async_updates is set, then each worker returns gradients for a
    # batch of this size.
    "train_batch_size": 1024,

    # === Parallelism ===
    # Number of workers for collecting samples with. This only makes sense
    # to increase if your environment is particularly slow to sample, or if
    # you're using the Async or Ape-X optimizers.
    "num_workers": 1,
    # Prevent iterations from going lower than this time span
    "min_time_s_per_reporting": 0,
})
# __sphinx_doc_end__
# fmt: on


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


class MADDPGTrainer(DQNTrainer):
    @classmethod
    @override(DQNTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(DQNTrainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
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

    @override(DQNTrainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        return MADDPGTFPolicy
