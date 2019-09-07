from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
"""Contributed port of MADDPG from OpenAI baselines.

The implementation has a couple assumptions:
- The number of agents is fixed and known upfront.
- Each agent is bound to a policy of the same name.
- Discrete actions are sent as logits (pre-softmax).

For a minimal example, see twostep_game.py, and the README for how to run
with the multi-agent particle envs.
"""

import logging

from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.dqn.dqn import GenericOffPolicyTrainer
from ray.rllib.contrib.maddpg.maddpg_policy import MADDPGTFPolicy
from ray.rllib.optimizers import SyncReplayOptimizer
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # === Settings for each individual policy ===
    # ID of the agent controlled by this policy
    "agent_id": None,
    # Use a local critic for this policy.
    "use_local_critic": False,

    # === Evaluation ===
    # Evaluation interval
    "evaluation_interval": None,
    # Number of episodes to run per evaluation period.
    "evaluation_num_episodes": 10,

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
    # Algorithm for good policies
    "good_policy": "maddpg",
    # Algorithm for adversary policies
    "adv_policy": "maddpg",

    # === Replay buffer ===
    # Size of the replay buffer. Note that if async_updates is set, then
    # each worker will have a replay buffer of this size.
    "buffer_size": int(1e6),
    # Observation compression. Note that compression makes simulation slow in
    # MPE.
    "compress_observations": False,

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
    "sample_batch_size": 100,
    # Size of a batched sampled from replay buffer for training. Note that
    # if async_updates is set, then each worker returns gradients for a
    # batch of this size.
    "train_batch_size": 1024,
    # Number of env steps to optimize for before returning
    "timesteps_per_iteration": 0,

    # === Parallelism ===
    # Number of workers for collecting samples with. This only makes sense
    # to increase if your environment is particularly slow to sample, or if
    # you're using the Async or Ape-X optimizers.
    "num_workers": 1,
    # Prevent iterations from going lower than this time span
    "min_iter_time_s": 0,
})
# __sphinx_doc_end__
# yapf: enable


def set_global_timestep(trainer):
    global_timestep = trainer.optimizer.num_steps_sampled
    trainer.train_start_timestep = global_timestep


def before_learn_on_batch(multi_agent_batch, policies, train_batch_size):
    samples = {}

    # Modify keys.
    for pid, p in policies.items():
        i = p.config["agent_id"]
        keys = multi_agent_batch.policy_batches[pid].data.keys()
        keys = ["_".join([k, str(i)]) for k in keys]
        samples.update(
            dict(
                zip(keys,
                    multi_agent_batch.policy_batches[pid].data.values())))

    # Make ops and feed_dict to get "new_obs" from target action sampler.
    new_obs_ph_n = [p.new_obs_ph for p in policies.values()]
    new_obs_n = list()
    for k, v in samples.items():
        if "new_obs" in k:
            new_obs_n.append(v)

    target_act_sampler_n = [p.target_act_sampler for p in policies.values()]
    feed_dict = dict(zip(new_obs_ph_n, new_obs_n))

    new_act_n = p.sess.run(target_act_sampler_n, feed_dict)
    samples.update(
        {"new_actions_%d" % i: new_act
         for i, new_act in enumerate(new_act_n)})

    # Share samples among agents.
    policy_batches = {pid: SampleBatch(samples) for pid in policies.keys()}
    return MultiAgentBatch(policy_batches, train_batch_size)


def make_optimizer(workers, config):
    return SyncReplayOptimizer(
        workers,
        learning_starts=config["learning_starts"],
        buffer_size=config["buffer_size"],
        train_batch_size=config["train_batch_size"],
        before_learn_on_batch=before_learn_on_batch,
        synchronize_sampling=True,
        prioritized_replay=False)


def add_trainer_metrics(trainer, result):
    global_timestep = trainer.optimizer.num_steps_sampled
    result.update(
        timesteps_this_iter=global_timestep - trainer.train_start_timestep,
        info=dict({
            "num_target_updates": trainer.state["num_target_updates"],
        }, **trainer.optimizer.stats()))


def collect_metrics(trainer):
    result = trainer.collect_metrics()
    return result


MADDPGTrainer = GenericOffPolicyTrainer.with_updates(
    name="MADDPG",
    default_config=DEFAULT_CONFIG,
    default_policy=MADDPGTFPolicy,
    before_init=None,
    before_train_step=set_global_timestep,
    make_policy_optimizer=make_optimizer,
    after_train_result=add_trainer_metrics,
    collect_metrics_fn=collect_metrics,
    before_evaluate_fn=None)
