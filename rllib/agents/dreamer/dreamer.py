import logging

import random
import numpy as np
import ray
from ray.rllib.utils.sgd import standardized
from ray.rllib.agents import with_common_config
from ray.rllib.agents.dreamer.dreamer_torch_policy import DreamerTorchPolicy
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.evaluation.metrics import get_learner_stats
from ray.rllib.execution.common import STEPS_SAMPLED_COUNTER, \
    STEPS_TRAINED_COUNTER, LEARNER_INFO, _get_shared_metrics
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.execution.metric_ops import CollectMetrics
from ray.util.iter import from_actors
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.torch_ops import convert_to_torch_tensor
from ray.rllib.evaluation.metrics import collect_episodes
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.agents.dreamer.dreamer_model import DreamerModel
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.util.iter_metrics import SharedMetrics
from ray.util.iter import LocalIterator
from ray.rllib.utils.typing import SampleBatchType

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # PlaNET Model LR
    "model_lr": 6e-4,
    # Actor LR
    "actor_lr": 8e-5,
    # Critic LR
    "critic_lr": 8e-5,
    # Grad Clipping
    "grad_clip": 100.0,
    # Discount
    "discount": 0.99,
    # Lambda
    "lambda": 0.95,
    # Training iterations per data collection from real env
    "dreamer_train_iters": 100,
    # If specified, clip the global norm of gradients by this amount
    "horizon": 1000,
    # Number of episodes to sample for Loss Calculation
    "batch_size": 50,
    # Length of each episode to sample for Loss Calculation
    "batch_length": 50,
    # Imagination Horizon for Training Actor and Critic
    "imagine_horizon": 15,
    # Free Nats
    "free_nats": 3.0,
    # KL Coeff for the Model Loss
    "kl_coeff": 1.0,
    # Distributed Dreamer not implemented yet
    "num_workers": 0,
    # Prefill Timesteps
    "prefill_timesteps": 5000,
    # This should be kept at 1 to preserve sample efficiency
    "num_envs_per_worker": 1,
    # Exploration Gaussian
    "explore_noise": 0.3,
    # Batch mode
    "batch_mode": "complete_episodes",
    # Custom Model
    "dreamer_model": {
        "custom_model": DreamerModel,
        # RSSM/PlaNET parameters
        "deter_size": 200,
        "stoch_size": 30,
        # CNN Decoder Encoder
        "depth_size": 32,
        # General Network Parameters
        "hidden_size": 400,
        # Action STD
        "action_init_std": 5.0,
    },

    "env_config": {
        # Repeats action send by policy for frame_skip times in env
        "frame_skip": 2,
    }
})
# __sphinx_doc_end__
# yapf: enable

class EpisodicBuffer(object):

  def __init__(self, max_length: int = 1000, length: int =50):
    """Data structure that stores episodes and samples chunks 
    of size length from episodes

    Arguments:
        max_length: Maximum episodes it can store
        length: Episode chunking lengh in sample()
    """

    # Stores all episodes into a list: List[SampleBatchType]
    self.episodes = []
    self.max_length = max_length
    self.timesteps = 0
    self.length = length

  def add(self, batch: SampleBatchType):
    """Splits a SampleBatch into episodes and adds episodes
    to the episode buffer

    Arguments:
        batch: SampleBatch to be added
    """

    self.timesteps += batch.count
    episodes = batch.split_by_episode()

    for i,e in enumerate(episodes):
        episodes[i] = self.preprocess_episode(e)
    self.episodes.extend(episodes)

    if len(self.episodes) > self.max_length:
      delta = len(self.episodes) - self.max_length
      # Drop oldest episodes
      self.episodes = self.episodes[delta:]
  
  def preprocess_episode(self, episode: SampleBatchType):
    """Batch format should be in the form of (s_t, a_(t-1), r_(t-1))
    When t=0, the resetted obs is paired with action and reward of 0.

    Arguments:
        episode: SampleBatch representing an episode
    """
    obs = episode['obs']
    new_obs = episode['new_obs']
    action = episode['actions']
    reward = episode['rewards']

    act_shape = action.shape
    act_reset = np.array([0.0]*act_shape[-1])[None]
    rew_reset = np.array(0.0)[None]
    obs_end = np.array(new_obs[act_shape[0]-1])[None]

    batch_obs = np.concatenate([obs, obs_end], axis=0)
    batch_action = np.concatenate([act_reset, action], axis=0)
    batch_rew = np.concatenate([rew_reset, reward], axis=0)

    new_batch = {'obs': batch_obs, "rewards": batch_rew, "actions": batch_action}
    return SampleBatch(new_batch)


  def sample(self, batch_size: int):
    """Samples [batch_size, length] from the list of episodes

    Arguments:
        batch_size: batch_size to be sampled
    """
    episodes_buffer = []
    while len(episodes_buffer) < batch_size:
      rand_index = random.randint(0, len(self.episodes)-1)
      episode = self.episodes[rand_index]
      if episode.count < self.length:
        continue
      available = episode.count - self.length
      index = int(random.randint(0, available))
      episodes_buffer.append(episode.slice(index, index + self.length))

    batch = {}
    for k in episodes_buffer[0].keys():
        batch[k] = np.stack([e[k] for e in episodes_buffer], axis=0)

    return SampleBatch(batch)


def total_sampled_timesteps(worker):
    return worker.policy_map[DEFAULT_POLICY_ID].global_timestep


def execution_plan(workers, config):
    # Special Replay Buffer for Dreamer agent
    episode_buffer = EpisodicBuffer(length=config["batch_length"])

    local_worker = workers.local_worker()

    # Prefill episode buffer with initial exploration (uniform sampling)
    while total_sampled_timesteps(local_worker) < config["prefill_timesteps"]:
        samples = local_worker.sample()
        episode_buffer.add(samples)

    batch_size = config["batch_size"]
    dreamer_train_iters = config["dreamer_train_iters"]
    action_repeat = config["action_repeat"]
    
    def dreamer_iteration(itr):

        def postprocess_gif(gif: np.ndarray):
            gif = np.clip(255 * gif, 0, 255).astype(np.uint8)
            B,T,C,H,W = gif.shape  
            frames = gif.transpose((1, 2, 3, 0,4)).reshape((1,T,C,H,B*W))
            return frames

        def policy_stats(fetches):
            return fetches['default_policy']['learner_stats']

        for samples in itr:
            # Dreamer Training Loop
            for n in range(dreamer_train_iters):
                batch = episode_buffer.sample(batch_size)
                if n == dreamer_train_iters -1:
                    batch["log_gif"] = True
                fetches = local_worker.learn_on_batch(batch)


            # Custom Logging
            policy_fetches = policy_stats(fetches)
            if "log_gif" in policy_fetches:
                gif = policy_fetches['log_gif']
                policy_fetches['log_gif'] = postprocess_gif(gif)

            metrics = _get_shared_metrics()
            metrics.info[LEARNER_INFO] = fetches
            metrics.counters[STEPS_SAMPLED_COUNTER] = episode_buffer.timesteps*action_repeat
            res = collect_metrics(local_worker=local_worker)
            res["info"] = metrics.info
            res["info"].update(metrics.counters)
            res["timesteps_total"] = metrics.counters[STEPS_SAMPLED_COUNTER]
            yield res

            episode_buffer.add(samples)

    rollouts = ParallelRollouts(workers)
    rollouts = rollouts.transform(dreamer_iteration)
    return rollouts


def get_policy_class(config):
    return DreamerTorchPolicy

def validate_config(config):
    config["action_repeat"] = config["env_config"]["frame_skip"]
    if config["framework"] != "torch":
        raise ValueError("Dreamer not supported in Tensorflow yet!")
    if config["batch_mode"] != "complete_episodes":
        raise ValueError("truncate_episodes not supported")
    if config["num_workers"] != 0:
        raise ValueError("Distributed Dreamer not supported yet!")
    if config["clip_actions"]:
        raise ValueError("Clipping is done inherently via policy tanh!")
    if config["action_repeat"] > 1:
        config["horizon"] = config["horizon"]/config["action_repeat"]

DREAMERTrainer = build_trainer(
    name="Dreamer",
    default_config=DEFAULT_CONFIG,
    default_policy=DreamerTorchPolicy,
    get_policy_class=get_policy_class,
    execution_plan=execution_plan,
    validate_config=validate_config)
